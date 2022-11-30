package write

import (
	"bufio"
	"context"
	"errors"
	"io"
	"ivst/datagen"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

type Writer interface {
	InsertBatch(ctx context.Context, payload ...*datagen.CPUPayload) error
}

type WriterManager struct {
	iw  Writer
	tw  Writer
	ds  datagen.DataSimulator
	wfs []wf
}

type SimulateConf struct {
	Lines     int
	BatchSize int
	Workers   int
	DirPath   string
	FileName  string
}

func (w *WriterManager) Simulate(ctx context.Context, sc *SimulateConf) error {
	// 生成测试数据
	w.ds.SetLines(sc.Lines)
	if sc.DirPath == "" {
		sc.DirPath = `D:\code\ivst\asset\write`
	}
	if sc.FileName == "" {
		sc.FileName = "cpu.csv"
	}
	if err := w.ds.CSV(sc.DirPath, sc.FileName, datagen.TypeFlagWrite); err != nil {
		return err
	}

	time.Sleep(1 * time.Second)
	// 加载数据
	cpc := make(chan *datagen.CPUPayload, 10000)
	go w.load(ctx, sc, cpc)
	// 写入
	id, err := w.Start(ctx, sc, cpc, w.iw.InsertBatch)
	if err != nil {
		return err
	}

	cpc = make(chan *datagen.CPUPayload, 10000)
	go w.load(ctx, sc, cpc)
	td, err := w.Start(ctx, sc, cpc, w.tw.InsertBatch)
	if err != nil {
		return err
	}

	log.Printf("influx cost %dms, tdengine cost %dms", id.Milliseconds(), td.Milliseconds())

	return nil

}

func (w *WriterManager) Start(ctx context.Context, sc *SimulateConf, cpc <-chan *datagen.CPUPayload, f wf) (time.Duration, error) {
	t1 := time.Now()
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < sc.Workers; i++ {
		g.Go(func() error {
			for {
				cps := make([]*datagen.CPUPayload, 0, sc.BatchSize)
				for payload := range cpc {
					cps = append(cps, payload)
					if len(cps) == sc.BatchSize {
						break
					}
				}
				// chan is closed
				if len(cps) == 0 {
					return nil
				}
				if err := f(ctx, cps...); err != nil {
					log.Print(err)
					// return err
				}
			}
		})
	}

	if err := g.Wait(); err != nil {
		return time.Since(t1), err
	}
	return time.Since(t1), nil
}

func (w *WriterManager) load(ctx context.Context, sc *SimulateConf, cpc chan<- *datagen.CPUPayload) {
	defer close(cpc)

	filePath := filepath.Join(sc.DirPath, sc.FileName)
	fh, err := os.Open(filePath)
	if err != nil {
		log.Print(err)
	}

	br := bufio.NewReader(fh)
	title, err := br.ReadString('\n')
	if err != nil {
		log.Print(err)
		return
	}
	titles := strings.Split(strings.TrimSuffix(title, "\n"), ",")

	for {
		m := make(map[string]interface{})
		line, err := br.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Print(err)
			return
		}
		lines := strings.Split(strings.TrimSuffix(line, "\n"), ",")
		for i := 0; i < len(lines); i++ {
			m[titles[i]] = lines[i]
		}

		cp := &datagen.CPUPayload{}
		rv := reflect.ValueOf(cp).Elem()
		for i := 0; i < rv.NumField(); i++ {
			field := rv.Type().Field(i)
			fn := field.Name
			tn := field.Tag.Get("json")
			if fn == "Ts" {
				ts, _ := m["time"].(string)
				tt, _ := time.Parse(
					"2006-01-02 15:04:05.000000",
					ts,
				)
				rv.Field(i).Set(reflect.ValueOf(tt))
				continue
			}

			tv, _ := m[tn].(string)
			if fn == "CPU" || fn == "Host" {
				rv.Field(i).SetString(tv)
				continue
			}
			tf, err := strconv.ParseFloat(tv, 64)
			if err != nil {
				log.Print(err)
				return
			}
			rv.Field(i).SetFloat(tf)
		}

		cpc <- cp
	}

}

// 废弃函数
func (w *WriterManager) write(ctx context.Context, wg *sync.WaitGroup, sc *SimulateConf, cpc <-chan *datagen.CPUPayload, fs ...wf) {
	defer wg.Done()
	for {
		cps := make([]*datagen.CPUPayload, 0, sc.BatchSize)
		for payload := range cpc {
			cps = append(cps, payload)
			if len(cps) == sc.BatchSize {
				break
			}
		}
		// chan is closed
		if len(cps) == 0 {
			return
		}

		for _, f := range fs {
			if err := f(ctx, cps...); err != nil {
				log.Print(err)
			}
		}
	}
}

type wf func(context.Context, ...*datagen.CPUPayload) error

// 废弃函数
func (w *WriterManager) WrapTimeCalculate(sumMiliSeconds *int64, f wf) wf {
	return func(ctx context.Context, cps ...*datagen.CPUPayload) error {
		t1 := time.Now()
		if err := f(ctx, cps...); err != nil {
			return err
		}
		d := time.Since(t1).Milliseconds()
		atomic.AddInt64(sumMiliSeconds, d)
		return nil
	}
}
