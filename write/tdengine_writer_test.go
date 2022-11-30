package write

import (
	"context"
	"ivst/datagen"
	"testing"
	"time"

	"github.com/taosdata/driver-go/v3/af"
)

type testTdEngineWriter struct {
	ctx context.Context
	tw  *TdEngineWriter
}

func newTestTdEngierWriter(t *testing.T) *testTdEngineWriter {
	conn, err := af.Open("localhost", "root", "taosdata", "csv", 6030)
	if err != nil {
		t.Fatal(err)
	}

	return &testTdEngineWriter{
		ctx: context.Background(),
		tw: &TdEngineWriter{
			conn: conn,
		},
	}
}

func TestTdengineInsertBatch(t *testing.T) {
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
			ttw := newTestTdEngierWriter(t)
			if err := ttw.tw.InsertBatch(ttw.ctx, tt.payload...); err != nil {
				t.Fatal(err)
			}
		})
	}
}
