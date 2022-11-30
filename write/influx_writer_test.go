package write

import (
	"context"
	"ivst/datagen"
	"testing"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

type testInfluxWriter struct {
	ctx context.Context
	iw  *InfluxWriter
}

func newTestInfluxWriter(t *testing.T) *testInfluxWriter {
	client := influxdb2.NewClientWithOptions(
		"http://localhost:8086",
		"",
		influxdb2.DefaultOptions().SetBatchSize(10000),
	)
	wa := client.WriteAPI("", "csv")

	return &testInfluxWriter{
		ctx: context.Background(),
		iw: &InfluxWriter{
			writeAPI: wa,
		},
	}
}

func TestInfluxInsertBatch(t *testing.T) {
	tests := []struct {
		name    string
		payload []*datagen.CPUPayload
	}{
		{
			name: "ok",
			payload: func() []*datagen.CPUPayload {
				p1 := &datagen.CPUPayload{}
				p1.Init(1, 1, time.Now())
				p2 := &datagen.CPUPayload{}
				p2.Init(2, 2, time.Now())
				return []*datagen.CPUPayload{p1, p2}
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tiw := newTestInfluxWriter(t)
			if err := tiw.iw.InsertBatch(tiw.ctx, tt.payload...); err != nil {
				t.Fatal(err)
			}
		})
	}
}
