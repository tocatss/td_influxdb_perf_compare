package write

import (
	"context"
	"ivst/datagen"

	"github.com/taosdata/driver-go/v3/af"
)

type TdEngineWriter struct {
	conn *af.Connector
}

func NewTdEngineWriter(conn *af.Connector) *TdEngineWriter {
	return &TdEngineWriter{
		conn: conn,
	}
}

func (tw *TdEngineWriter) InsertBatch(ctx context.Context, payload ...*datagen.CPUPayload) error {
	lines := make([]string, len(payload))
	if len(lines) == 0 {
		return nil
	}

	for i := 0; i < len(payload); i++ {
		lines[i] = payload[i].TdEngineLine()
	}
	if err := tw.conn.InfluxDBInsertLines(lines, "ns"); err != nil {
		return err
	}

	return nil
}
