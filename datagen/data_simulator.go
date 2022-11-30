package datagen

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type DataSimulator interface {
	SetLines(lines int)
	CSV(dirPath string, fileName string, typeFlag flag) error
}

type CPUDataSimulator struct {
	Duration   time.Duration
	Interval   time.Duration
	StartFrom  time.Time
	CPUNumber  int
	HostNumber int
	Lines      int
}

type fileLine struct {
	fileName string
	payload  string
}

type flag int

const (
	// 生成一个csv，适用于通过python tool导入
	TypeFlagCommonOne flag = iota
	// 按host_cpu组合生成csv，适用于通过tdengine_loader导入
	TypeFlagTDEngine
	// 生成多个csv，适用于通过telegraf导入
	TypeFlagCommonMulti
	TypeFlagWrite
)

func (cs *CPUDataSimulator) SetLines(lines int) {
	cs.Lines = lines
}

func (cs *CPUDataSimulator) CSV(dirPath string, fileName string, typeFlag flag) error {
	rand.Seed(time.Now().UnixNano())

	if cs.CPUNumber == 0 || cs.HostNumber == 0 {
		return errors.New("CPUNumber or HostNumber is incorrect")
	}

	done := make(chan interface{})
	flc := make(chan fileLine, 10000)
	fhm := make(map[string]*os.File)
	go func() {
		defer func() {
			done <- nil
		}()

		for fl := range flc {
			filePath := path.Join(dirPath, fileName)
			if fileName == "" {
				filePath = path.Join(dirPath, fl.fileName)
			}

			if _, ok := fhm[filePath]; !ok {
				fh, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
				if err != nil {
					return
				}
				// tdEngine 命令行导入不需要title
				if typeFlag != TypeFlagTDEngine {
					fh.WriteString(CSVTitle())
				}

				fhm[filePath] = fh
			}

			fhm[filePath].WriteString(fl.payload)
		}
	}()

	cs.FakeTimePassed(flc, typeFlag)
	close(flc)
	<-done

	for _, fh := range fhm {
		_ = fh.Close()
	}

	return nil
}

func (cs *CPUDataSimulator) TickOnce(flc chan fileLine, offset int, typeFlag flag) {
	ts := cs.StartFrom.Add(time.Duration(offset) * cs.Interval)
	if typeFlag == TypeFlagWrite {
		i, j := rand.Intn(cs.HostNumber), rand.Intn(cs.CPUNumber+1)
		cp := &CPUPayload{}
		cp.Init(i, j, ts)
		flc <- fileLine{
			fileName: "",
			payload:  cp.CSVLineWithDateFormat(),
		}
		return
	}

	for i := 0; i < cs.HostNumber; i++ {
		// 多迭代一次是代表cpuTotal这一行
		for j := 0; j <= cs.CPUNumber; j++ {
			cp := &CPUPayload{}
			cp.Init(i, j, ts)

			fileName := fmt.Sprintf("host%d_cpu%d.csv", i, j)
			var payload string
			if typeFlag == TypeFlagTDEngine {
				payload = cp.TDEngineCSVLine()
			} else if typeFlag == TypeFlagCommonMulti {
				payload = cp.CSVLineWithUnixTime()
			} else {
				payload = cp.CSVLineWithDateFormat()
			}
			flc <- fileLine{
				fileName: fileName,
				payload:  payload,
			}
		}
	}
}

func (cs *CPUDataSimulator) FakeTimePassed(flc chan fileLine, typeFlag flag) {
	var wg sync.WaitGroup
	times := cs.Lines
	if times <= 0 {
		ds := cs.Duration.Seconds()
		is := cs.Interval.Seconds()
		times = int(ds) / int(is)
	}

	for offset := 1; offset <= times; offset += 1 {
		wg.Add(1)
		go func(offset int) {
			cs.TickOnce(flc, offset, typeFlag)
			wg.Done()
		}(offset)
	}
	wg.Wait()
}

type CPUPayload struct {
	Ts time.Time

	// Tags
	CPU  string `json:"cpu"`
	Host string `json:"host"`

	// Fields
	UsageIrq       float64 `json:"usage_irq"`
	UsageSoftIrq   float64 `json:"usage_softirq"`
	UsageSteal     float64 `json:"usage_steal"`
	UsageUser      float64 `json:"usage_user"`
	UsageNice      float64 `json:"usage_nice"`
	UsageIOWait    float64 `json:"usage_iowait"`
	UsageGuest     float64 `json:"usage_guest"`
	UsageGuestNice float64 `json:"usage_guest_nice"`
	UsageSystem    float64 `json:"usage_system"`
	UsageIdle      float64 `json:"usage_idle"`
}

func (p *CPUPayload) Init(hostIndex, cpuIndex int, ts time.Time) {
	p.CPU = fmt.Sprintf("cpu%d", cpuIndex)
	p.Host = fmt.Sprintf("host%d", hostIndex)
	p.Ts = ts

	pv := reflect.ValueOf(p).Elem()
	for i := 0; i < pv.NumField(); i++ {
		fn := pv.Type().Field(i).Name
		if fn == "CPU" || fn == "Host" || fn == "Ts" {
			continue
		}

		fv := rand.Float64()
		pv.Field(i).SetFloat(fv)
	}
}

// "meters,location=California.LosAngeles,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249",}
func (p *CPUPayload) TdEngineLine() string {
	s1 := fmt.Sprintf("%s,host=%s,cpu=%s", "cpu", p.Host, p.CPU)

	s2 := make([]string, 0, 10)
	pv := reflect.ValueOf(p).Elem()
	for i := 0; i < pv.NumField(); i++ {
		field := pv.Type().Field(i)
		fn := field.Name
		if fn == "CPU" || fn == "Host" || fn == "Ts" {
			continue
		}
		fv := pv.Field(i).Float()
		tn := field.Tag.Get("json")
		s2 = append(s2, fmt.Sprintf("%s=%f", tn, fv))
	}

	return fmt.Sprintf("%s %s %d", s1, strings.Join(s2, ","), p.Ts.Local().UnixNano())
}

func (p *CPUPayload) InfluxPoint() *write.Point {
	tags, fields := make(map[string]string),
		make(map[string]interface{})

	pv := reflect.ValueOf(p).Elem()
	for i := 0; i < pv.NumField(); i++ {
		field := pv.Type().Field(i)
		fn := field.Name
		if fn == "Ts" {
			continue
		}

		tn := field.Tag.Get("json")
		if fn == "CPU" || fn == "Host" {
			tags[tn] = pv.Field(i).String()
			continue
		}
		fv := pv.Field(i).Float()
		fields[tn] = fv
	}

	return influxdb2.NewPoint("cpu", tags, fields, p.Ts)

}

func (p *CPUPayload) TDEngineCSVLine() string {
	// _ts,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_user,usage_system,usage_guest_nice,cpu,host
	// '2022-08-30 14:03:30.000000000',1.560062402,1.404056162,97.035881435,0.000000000,0.000000000,0.000000000,0.000000000,0.000000000,0.000000000,0.000000000,'cpu2','LAPTOP-S0J8J4JQ'
	ts := p.Ts.UTC().Format("2006-01-02 15:04:05.000000000")
	return fmt.Sprintf("'%s',%.9f,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f,'%s','%s'\n",
		ts, p.UsageIdle, p.UsageNice, p.UsageIOWait, p.UsageIrq,
		p.UsageSoftIrq, p.UsageSteal, p.UsageGuest, p.UsageUser,
		p.UsageSystem, p.UsageGuestNice, p.CPU, p.Host,
	)
}

func (p *CPUPayload) CSVLineWithUnixTime() string {
	// name,time,cpu,host,usage_guest,usage_guest_nice,usage_idle,usage_iowait,usage_irq,usage_nice,usage_softirq,usage_steal,usage_system,usage_user
	// cpu,1662445990,cpu-total,LAPTOP-S0J8J4JQ,0,0,94.40532081377152,0,0,0,0,0,4.147104851330203,1.4475743348982786
	ts := p.Ts.UTC().UnixNano()
	return fmt.Sprintf("%s,%d,%s,%s,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f\n",
		"cpu", ts, p.CPU, p.Host, p.UsageIdle, p.UsageNice, p.UsageIOWait, p.UsageIrq,
		p.UsageSoftIrq, p.UsageSteal, p.UsageGuest, p.UsageUser,
		p.UsageSystem, p.UsageGuestNice,
	)
}

func (p *CPUPayload) CSVLineWithDateFormat() string {
	// name,time,cpu,host,usage_guest,usage_guest_nice,usage_idle,usage_iowait,usage_irq,usage_nice,usage_softirq,usage_steal,usage_system,usage_user
	// cpu,1662445990,cpu-total,LAPTOP-S0J8J4JQ,0,0,94.40532081377152,0,0,0,0,0,4.147104851330203,1.4475743348982786
	ts := p.Ts.UTC().Format("2006-01-02 15:04:05.000000")
	return fmt.Sprintf("%s,%s,%s,%s,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f,%.9f\n",
		"cpu", ts, p.CPU, p.Host, p.UsageIdle, p.UsageNice, p.UsageIOWait, p.UsageIrq,
		p.UsageSoftIrq, p.UsageSteal, p.UsageGuest, p.UsageUser,
		p.UsageSystem, p.UsageGuestNice,
	)
}

func CSVTitle() string {
	// name,time,cpu,host,usage_guest,usage_guest_nice,usage_idle,usage_iowait,usage_irq,usage_nice,usage_softirq,usage_steal,usage_system,usage_user
	return "name,time,cpu,host,usage_guest,usage_guest_nice,usage_idle,usage_iowait,usage_irq,usage_nice,usage_softirq,usage_steal,usage_system,usage_user\n"
}
