package write

import (
	"context"
	"ivst/datagen"

	"github.com/influxdata/influxdb-client-go/v2/api"
)

type InfluxWriter struct {
	writeAPI api.WriteAPI
}

func NewInfluxWriter(writerAPI api.WriteAPI) *InfluxWriter {
	return &InfluxWriter{
		writeAPI: writerAPI,
	}
}

func (iw *InfluxWriter) InsertBatch(ctx context.Context, payload ...*datagen.CPUPayload) error {
	lines := make([]string, len(payload))
	if len(lines) == 0 {
		return nil
	}

	for i := 0; i < len(payload); i++ {
		point := payload[i].InfluxPoint()
		iw.writeAPI.WritePoint(point)
	}
	iw.writeAPI.Flush()
	return nil
}
