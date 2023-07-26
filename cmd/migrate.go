package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/influxdata/influxdb/client/v2"
)

type SourceConfig struct {
	URL             string
	Database        string
	RetentionPolicy string
	Username        string
	Password        string
}

type TargetConfig struct {
	URL             string
	Database        string
	RetentionPolicy string
	Username        string
	Password        string
}

type Cursor struct {
	client *client.Client

	startTime time.Time
	endTime   time.Time

	measurementSize    int
	currentMeasurement int

	MaxIntervalIterator     int // 最大时间间隔/每次迭代的时间间隔+1=迭代次数
	CurrentIntervalIterator int // 当前迭代次数
}

type Statistics struct {
	TotalPoints int64         // 总数据条数
	TotalTime   time.Duration // 总耗时
	Rate        float64       // 迁移速率
	CachePoints int64         // 缓存数据条数
}

type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	RateLimit      int           // 迁移速率，单位：Points/s
	Interval       time.Duration // 迁移时间间隔
	StartTime      time.Time     // 数据开始时间
	EndTime        time.Time     // 数据结束时间
	MaxCachePoints int           // 最大缓存数据条数

	SourceConfig *SourceConfig // 源数据库配置
	TargetConfig *TargetConfig // 目标数据库配置

	MeasurementCardinality int // 测量值基数
	SeriesCardinality      int // 序列基数

	Measurements []string // 需要迁移的 measurements

	BatchSize   int    // 每个请求的 Point 数量
	Precision   string // 时间戳精度	rfc3339|h|m|s|ms|u|ns
	Consistency string // 写入一致性：any|one|quorum|all
	workers     int    // 并发数

	statistics *Statistics // 统计信息
	Cursor     *Cursor

	CacheBar *pb.ProgressBar
	//ProgressBar *pb.ProgressBar
}

var Cache PointsCache // 缓存

func (cmd *Command) Next() bool {
	if cmd.Cursor.CurrentIntervalIterator < cmd.Cursor.MaxIntervalIterator && cmd.Cursor.currentMeasurement < cmd.Cursor.measurementSize-1 {
		cmd.Cursor.currentMeasurement++
		return true
	} else if cmd.Cursor.CurrentIntervalIterator < cmd.Cursor.MaxIntervalIterator && cmd.Cursor.currentMeasurement == cmd.Cursor.measurementSize-1 {
		cmd.Cursor.currentMeasurement = 0
		cmd.Cursor.CurrentIntervalIterator++
		duration := cmd.Cursor.endTime.Sub(cmd.Cursor.startTime)
		cmd.Cursor.startTime = cmd.Cursor.startTime.Add(duration)
		cmd.Cursor.endTime = cmd.Cursor.endTime.Add(duration)
		return true
	} else if cmd.Cursor.CurrentIntervalIterator >= cmd.Cursor.MaxIntervalIterator && cmd.Cursor.currentMeasurement < cmd.Cursor.measurementSize-1 {
		cmd.Cursor.currentMeasurement++
		return true
	} else if cmd.Cursor.CurrentIntervalIterator >= cmd.Cursor.MaxIntervalIterator && cmd.Cursor.currentMeasurement == cmd.Cursor.measurementSize-1 {
		return false
	}
	return false
}

// PointsCache  向 Cache 添加读写锁
type PointsCache struct {
	Points    chan client.BatchPoints // 缓存的数据
	MaxPoints int64                   // 缓存最大数据条数
	RWMutex   sync.RWMutex
}

func NewPointsCache(maxPoints int) *PointsCache {
	return &PointsCache{
		Points:    make(chan client.BatchPoints, 100),
		MaxPoints: int64(maxPoints),
	}
}

func main() {

	cmd := NewCommand()
	var err error
	if err != cmd.Run(os.Args[1:]...) {
		log.Fatal(err)
	}

}

func (cmd *Command) Run(args ...string) error {
	var start, end string
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)

	fs.IntVar(&cmd.RateLimit, "rate-limit", 50000, "Optional: the rate limit to migrate data (Points/s)")
	fs.DurationVar(&cmd.Interval, "interval", 0, "Optional: the interval to migrate data (e.g., 5m)")
	fs.StringVar(&start, "start-time", "", "Optional: the start time to export (RFC3339 format)")
	fs.StringVar(&end, "end-time", "", "Optional: the end time to export (RFC3339 format)")
	fs.IntVar(&cmd.MaxCachePoints, "max-cache-points", 100000, "Optional: the max cache points to write")
	fs.IntVar(&cmd.BatchSize, "batch-size", 5000, "Optional: the batch size to write")
	fs.StringVar(&cmd.SourceConfig.URL, "source-url", "", "Required: the source database URL")
	fs.StringVar(&cmd.SourceConfig.Database, "source-db", "", "Required: the source database name")
	fs.StringVar(&cmd.SourceConfig.RetentionPolicy, "source-rp", "", "Required: the source retention policy name")
	fs.StringVar(&cmd.SourceConfig.Username, "source-username", "", "Optional: the source database username")
	fs.StringVar(&cmd.SourceConfig.Password, "source-password", "", "Optional: the source database password")
	fs.StringVar(&cmd.TargetConfig.URL, "target-url", "", "Required: the target database URL")
	fs.StringVar(&cmd.TargetConfig.Database, "target-db", "", "Required: the target database name")
	fs.StringVar(&cmd.TargetConfig.RetentionPolicy, "target-rp", "", "Required: the target retention policy name")
	fs.StringVar(&cmd.TargetConfig.Username, "target-username", "", "Optional: the target database username")
	fs.StringVar(&cmd.TargetConfig.Password, "target-password", "", "Optional: the target database password")

	fs.SetOutput(cmd.Stdout)
	fs.Usage = func() {
		fmt.Fprintf(cmd.Stdout, `Usage: %s [flags]`, "migrate")
		fmt.Fprintf(cmd.Stdout, "\n\nFlags:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return err
	}

	// set defaults
	if start != "" {
		s, err := time.Parse(time.RFC3339, start)
		if err != nil {
			log.Fatal(err)
		}
		cmd.StartTime = s
	}

	if end != "" {
		e, err := time.Parse(time.RFC3339, end)
		if err != nil {
			log.Fatal(err)
		}
		cmd.EndTime = e
	}

	return cmd.migrate()
}

func (cmd *Command) migrate() error {
	// print meta info
	printMeta(cmd)
	cmd.init()
	cmd.Task()
	return nil
}

func GetBatchPoints(cmd *Command, batchPoints chan<- client.BatchPoints) {
	result, err := cmd.RunQuery(fmt.Sprintf("SELECT * FROM %s.%s.%s WHERE time >= '%s' AND time < '%s'",
		cmd.SourceConfig.Database, cmd.SourceConfig.RetentionPolicy, cmd.Measurements[cmd.Cursor.currentMeasurement], cmd.Cursor.startTime.Format(time.RFC3339Nano), cmd.Cursor.endTime.Format(time.RFC3339Nano)))
	if err != nil {
		log.Fatal(err)
	}
	if result.Results[0].Series == nil {
		// log.Printf("measurement %s: %s - %s  no data", cmd.Measurements[cmd.Cursor.currentMeasurement], cmd.Cursor.startTime, cmd.Cursor.endTime)
		return
	}

	name := result.Results[0].Series[0].Name
	columns := result.Results[0].Series[0].Columns
	tagKeys := cmd.GetTagKeys()
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:         cmd.TargetConfig.Database,
		RetentionPolicy:  cmd.TargetConfig.RetentionPolicy,
		WriteConsistency: cmd.Consistency,
	})
	if err != nil {
		log.Fatal(err)
	}
	count := 0
	for _, line := range result.Results[0].Series[0].Values {

		var tags = make(map[string]string)
		var fields = make(map[string]interface{})
		ts, err := time.Parse(time.RFC3339Nano, line[0].(string))

		if err != nil {
			log.Fatal(err)
		}
		for i := 1; i < len(line); i++ {
			if line[i] == nil {
				continue
			}

			// 表示字段为 tag 类型
			if ContainsString(tagKeys, columns[i]) {
				tags[columns[i]] = line[i].(string)
				// 否则为 field 类型
			} else {
				fields[columns[i]] = line[i]
			}

		}
		point, err := client.NewPoint(name, tags, fields, ts)
		if err != nil {
			log.Fatal(err)
		}
		bp.AddPoint(point)
		if cmd.BatchSize == count {
			count = 0
			atomic.AddInt64(&cmd.statistics.CachePoints, int64(len(bp.Points())))
			batchPoints <- bp
			bp, _ = client.NewBatchPoints(client.BatchPointsConfig{
				Database:         cmd.TargetConfig.Database,
				RetentionPolicy:  cmd.TargetConfig.RetentionPolicy,
				WriteConsistency: cmd.Consistency,
			})
		}
	}
	batchPoints <- bp
	atomic.AddInt64(&cmd.statistics.CachePoints, int64(len(bp.Points())))
	cmd.CacheBar.Add(len(bp.Points()))
}

func WriteBatchPoints(cmd *Command, bps <-chan client.BatchPoints) {
	for {
		select {
		case bp, ok := <-bps:
			if !ok {
				return
			}

			if err := cmd.GetTargetClient().Write(bp); err != nil {
				log.Fatal(err)
			}
			atomic.AddInt64(&cmd.statistics.CachePoints, -int64(len(bp.Points())))
			atomic.AddInt64(&cmd.statistics.TotalPoints, int64(len(bp.Points())))
			cmd.CacheBar.Add(-len(bp.Points()))
			//cmd.ProgressBar.Add(len(bp.Points()))

		case <-time.After(10 * time.Second):
			log.Println("WriteBatchPoints Finish (10 seconds)")
			return
		}
	}
}

func (cmd *Command) Task() {
	// 开始展示进度条
	startTime := time.Now()
	//cmd.ProgressBar.Start()
	cmd.CacheBar = pb.StartNew(cmd.MaxCachePoints)
	cache := NewPointsCache(cmd.MaxCachePoints)

	done := make(chan struct{}, 1)

	go func() {
		GetBatchPoints(cmd, cache.Points)

		for cmd.Next() {
			GetBatchPoints(cmd, cache.Points)
		}
		// Close 'cache.Points' when 'cmd.Next()' returns false
		close(cache.Points)
	}()

	go func() {
		WriteBatchPoints(cmd, cache.Points)
		done <- struct{}{}
	}()

	<-done

	//cmd.ProgressBar.Finish()
	cmd.CacheBar.Finish()
	cmd.statistics.TotalTime = time.Since(startTime)
	log.Printf("Write %d Points, total time: %s", cmd.statistics.TotalPoints, cmd.statistics.TotalTime)
	//os.Exit(0)
}

func printMeta(cmd *Command) {

	fmt.Printf("SOURCE\n")
	fmt.Printf("URL: %s\n", cmd.SourceConfig.URL)
	fmt.Printf("DATABASE: %s\n", cmd.SourceConfig.Database)
	fmt.Printf("RETENTION_POLICY: %s\n", cmd.SourceConfig.RetentionPolicy)
	fmt.Printf("\nTARGET\n")
	fmt.Printf("URL: %s\n", cmd.TargetConfig.URL)
	fmt.Printf("DATABASE: %s\n", cmd.TargetConfig.Database)
	fmt.Printf("RETENTION_POLICY: %s\n", cmd.TargetConfig.RetentionPolicy)
	fmt.Printf("\nMaxRate: %d Points/s\n", cmd.RateLimit)
	fmt.Printf("Interval: %s\n", cmd.Interval.String())
	fmt.Printf("Start time: %s\n", cmd.StartTime.Format(time.RFC3339))
	fmt.Printf("End time: %s\n", cmd.EndTime.Format(time.RFC3339))
	fmt.Printf("MEASUREMENT CARDINALITY: %d\n", cmd.GetMeasurementCardinality())
	fmt.Printf("SERIES CARDINALITY: %d\n", cmd.GetSeriesCardinality())
}

// ParseTags
// input SHOW TAG KEYS ON "telegraf" FROM "cpu"
// output []string{"cpu", "host"}
func ParseTags(result []client.Result) []string {
	//if result[0].Series have no data, return nil
	if result[0].Series == nil {
		return nil
	}
	var tags []string

	for _, tag := range result[0].Series[0].Values {
		tags = append(tags, tag[0].(string))
	}
	return tags
}

// ParseValues
// input SHOW FIELD KEYS ON "telegraf" FROM "cpu"
// output map[string]string{"usage_guest": "float", "usage_guest_nice": "float", "usage_idle": "float", "usage_iowait": "float", "usage_irq": "float", "usage_nice": "float", "usage_softirq": "float", "usage_steal": "float", "usage_system": "float", "usage_user": "float"}
func ParseValues(result []client.Result) map[string]string {
	//if result[0].Series have no data, return nil
	if result[0].Series == nil {
		return nil
	}
	var values = make(map[string]string)

	for _, vs := range result[0].Series[0].Values {
		fieldKey := vs[0].(string)
		fieldType := vs[1].(string)
		values[fieldKey] = fieldType
	}
	return values
}

func NewCommand() *Command {

	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,

		SourceConfig: &SourceConfig{},
		TargetConfig: &TargetConfig{},
		//ProgressBar:  pb.New64(0),
	}
}

// GetSourceClient 获取 InfluxDB Source 连接
func (cmd *Command) GetSourceClient() client.Client {
	return cmd.SourceConfig.GetClient()
}

// GetTargetClient 获取 InlfuxDB target 连接
func (cmd *Command) GetTargetClient() client.Client {
	return cmd.TargetConfig.GetClient()
}

func (config *TargetConfig) GetClient() client.Client {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     config.URL,
		Username: config.Username,
		Password: config.Password,
	})
	if err != nil {
		log.Fatal(err)
	}
	return c
}

func (config *SourceConfig) GetClient() client.Client {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     config.URL,
		Username: config.Username,
		Password: config.Password,
	})
	if err != nil {
		log.Fatal(err)
	}
	return c
}

// GetMeasurementCardinality 获取 MEASUREMENT CARDINALITY
func (cmd *Command) GetMeasurementCardinality() int {

	result, err := cmd.RunQuery(fmt.Sprintf("SHOW MEASUREMENT CARDINALITY ON %s", cmd.SourceConfig.Database))
	if err != nil {
		log.Fatal(err)
	}
	value := reflect.ValueOf(result.Results[0].Series[0].Values[0][0]).String()
	cardinality, err := strconv.Atoi(value)
	if err != nil {
		log.Fatal(err)
	}
	return cardinality
}

// GetSeriesCardinality 获取 SERIES CARDINALITY
func (cmd *Command) GetSeriesCardinality() int {
	result, err := cmd.RunQuery(fmt.Sprintf("SHOW SERIES CARDINALITY ON %s", cmd.SourceConfig.Database))
	if err != nil {
		log.Fatal(err)
	}
	value := reflect.ValueOf(result.Results[0].Series[0].Values[0][0]).String()
	cardinality, err := strconv.Atoi(value)
	if err != nil {
		log.Fatal(err)
	}
	return cardinality
}

// GetMeasurement 获取所有的 Measurement
func (cmd *Command) GetMeasurement() []string {
	result, err := cmd.RunQuery(fmt.Sprintf("SHOW MEASUREMENTS ON %s", cmd.SourceConfig.Database))
	if err != nil {
		log.Fatal(err)
	}
	var measurements []string
	if result.Results[0].Series == nil {
		log.Fatal("No measurement found")
	}
	for _, measurement := range result.Results[0].Series[0].Values {
		measurements = append(measurements, measurement[0].(string))
	}
	return measurements
}

// GetTagKeys 获取 TagKeys
func (cmd *Command) GetTagKeys() []string {
	result, err := cmd.RunQuery(fmt.Sprintf("SHOW TAG KEYS ON %s FROM %s.%s WHERE time > '%s' AND time < '%s'", cmd.SourceConfig.Database, cmd.SourceConfig.RetentionPolicy, cmd.Measurements[cmd.Cursor.currentMeasurement], cmd.Cursor.startTime.Format(time.RFC3339), cmd.Cursor.endTime.Format(time.RFC3339)))

	var tags []string
	if err != nil {
		log.Fatal(err)
	}
	if result.Results[0].Series == nil {
		return nil
	}
	for _, tag := range result.Results[0].Series[0].Values {
		tags = append(tags, tag[0].(string))
	}

	return tags
}

// RunQuery executes a query and returns the first value as a string.
func (cmd *Command) RunQuery(query string) (*client.Response, error) {
	q := client.Query{
		Command:         query,
		Database:        cmd.SourceConfig.Database,
		RetentionPolicy: cmd.SourceConfig.RetentionPolicy,
		Precision:       "",
		Chunked:         false,
		ChunkSize:       0,
		Parameters:      nil,
	}
	result, err := cmd.GetSourceClient().Query(q)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (cmd *Command) init() {
	cmd.Measurements = cmd.GetMeasurement()
	cmd.Cursor = &Cursor{
		client:                  nil,
		startTime:               cmd.StartTime,
		endTime:                 cmd.StartTime.Add(cmd.Interval),
		measurementSize:         len(cmd.Measurements),
		currentMeasurement:      0,
		MaxIntervalIterator:     int(cmd.EndTime.Sub(cmd.StartTime).Nanoseconds()/cmd.Interval.Nanoseconds()) + 1,
		CurrentIntervalIterator: 0,
	}
	cmd.statistics = &Statistics{
		TotalPoints: 0,
		TotalTime:   time.Duration(0),
		Rate:        0,
		CachePoints: 0,
	}

}

// ContainsString checks if the target string exists in the given array.
func ContainsString(arr []string, target string) bool {
	for _, s := range arr {
		if s == target {
			return true
		}
	}
	return false
}
