package query

import (
	"context"
	"testing"
	"time"

	_ "github.com/influxdata/influxdb1-client"
	client "github.com/influxdata/influxdb1-client/v2"
)

type iQueryTester struct {
	iq  *InfluxQuery
	ctx context.Context
}

func newiQueryTester(t *testing.T) *iQueryTester {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://127.0.0.1:8086",
	})
	if err != nil {
		t.Fatal(err)
	}
	iq := &InfluxQuery{
		pf:     pf,
		client: c,
	}
	return &iQueryTester{
		iq:  iq,
		ctx: context.Background(),
	}
}

func TestISelectFirstLastRowTime(t *testing.T) {
	iqt := newiQueryTester(t)
	st, et, err := iqt.iq.SelectFirstLastRowTime(iqt.ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(st, et)
}

func TestISelect(t *testing.T) {
	iqt := newiQueryTester(t)
	if err := iqt.iq.SelectByTags(iqt.ctx, 1); err != nil {
		t.Fatal(err)
	}

	if err := iqt.iq.SelectGroupByTags(iqt.ctx); err != nil {
		t.Fatal(err)
	}

	endStr := "2022-08-08T06:58:30.374"
	end, err := time.Parse("2006-01-02T15:04:05.000", endStr)
	if err != nil {
		t.Fatal(err)
	}
	startStr := "2022-08-08T06:46:30.374"
	start, err := time.Parse("2006-01-02T15:04:05.000", startStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := iqt.iq.MaxByTimeInterval(iqt.ctx, start, end, "1m"); err != nil {
		t.Fatal(err)
	}
}
