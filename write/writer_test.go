package write

import (
	"context"
	"ivst/datagen"
	"sync"
	"testing"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/taosdata/driver-go/v3/af"
)

type testWriterManger struct {
	ctx context.Context
	wm  *WriterManager
}

func newTestWriterManger(t *testing.T) *testWriterManger {
	wm := &WriterManager{}

	client := influxdb2.NewClientWithOptions(
		"http://localhost:8086",
		"",
		influxdb2.DefaultOptions().SetBatchSize(10000),
	)
	wa := client.WriteAPI("", "csv")
	wm.iw = NewInfluxWriter(wa)

	conn, err := af.Open("localhost", "root", "taosdata", "csv", 6030)
	if err != nil {
		t.Fatal(err)
	}
	wm.tw = NewTdEngineWriter(conn)

	wm.ds = &datagen.CPUDataSimulator{
		Interval:   10 * time.Second,
		StartFrom:  time.Now(),
		CPUNumber:  12,
		HostNumber: 26,
	}

	return &testWriterManger{
		ctx: context.Background(),
		wm:  wm,
	}
}

func TestSimulate(t *testing.T) {
	tests := []struct {
		name string
		sc   *SimulateConf
	}{
		// {
		// 	name: "ok:workers:1, lines:1,000, batch size 1",
		// 	sc: &SimulateConf{
		// 		Lines:     1000,
		// 		Workers:   1,
		// 		BatchSize: 1,
		// 	},
		// },
		// {
		// 	name: "ok:workers:1, lines:10,000, batch size 1",
		// 	sc: &SimulateConf{
		// 		Lines:     10000,
		// 		Workers:   1,
		// 		BatchSize: 1,
		// 	},
		// },
		// {
		// 	name: "ok:workers:5, lines:10,000, batch size 100",
		// 	sc: &SimulateConf{
		// 		Lines:     10000,
		// 		Workers:   5,
		// 		BatchSize: 100,
		// 	},
		// },
		{
			name: "ok:workers:5, lines:100,000, batch size 100",
			sc: &SimulateConf{
				Lines:     100000,
				Workers:   5,
				BatchSize: 100,
			},
		},
		// {
		// 	name: "ok:workers:5, lines:1,000,000, batch size 1,000",
		// 	sc: &SimulateConf{
		// 		Lines:     1000000,
		// 		Workers:   5,
		// 		BatchSize: 1000,
		// 	},
		// },
		// {
		// 	name: "ok:workers:10, lines:1,000,000, batch size 1,000",
		// 	sc: &SimulateConf{
		// 		Lines:     1000000,
		// 		Workers:   10,
		// 		BatchSize: 1000,
		// 	},
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			twm := newTestWriterManger(t)
			err := twm.wm.Simulate(twm.ctx, tt.sc)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWrapTimeCalculate(t *testing.T) {
	tests := []struct {
		name string
		fs   []wf
		cost int64
	}{
		{
			name: "ok",
			fs: []wf{
				func(context.Context, ...*datagen.CPUPayload) error {
					time.Sleep(1 * time.Second)
					return nil
				},
				func(context.Context, ...*datagen.CPUPayload) error {
					time.Sleep(1 * time.Second)
					return nil
				},
				func(context.Context, ...*datagen.CPUPayload) error {
					time.Sleep(1 * time.Second)
					return nil
				},
			},
			cost: 0,
		},
	}

	for _, tt := range tests {
		twm := newTestWriterManger(t)
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			for _, f := range tt.fs {
				wg.Add(1)
				go func(ff wf, wg *sync.WaitGroup) {
					twm.wm.WrapTimeCalculate(&tt.cost, ff)(twm.ctx)
					wg.Done()
				}(f, &wg)
			}
			wg.Wait()

			t.Logf("cost %dms", tt.cost)
		})
	}

}
