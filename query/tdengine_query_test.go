package query

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/taosdata/driver-go/v3/taosSql"
)

type tQueryTester struct {
	teq *TdEngineQuery
	ctx context.Context
}

func newTQueryTester(t *testing.T) *tQueryTester {
	var taosUri = "root:taosdata@tcp(localhost:6030)/csv"
	taos, err := sql.Open("taosSql", taosUri)
	if err != nil {
		panic(err)
	}

	teq := &TdEngineQuery{
		pf: pf,
		db: sqlx.NewDb(taos, "taosSql"),
	}

	return &tQueryTester{
		teq: teq,
		ctx: context.Background(),
	}
}

func TestTSelectFirstLastRowTime(t *testing.T) {
	tqt := newTQueryTester(t)
	st, et, err := tqt.teq.SelectFirstLastRowTime(tqt.ctx)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(st, et)
}

func TestTSelect(t *testing.T) {
	tqt := newTQueryTester(t)
	if err := tqt.teq.SelectByTags(tqt.ctx, 12); err != nil {
		t.Fatal(err)
	}

	if err := tqt.teq.SelectGroupByTags(tqt.ctx); err != nil {
		t.Fatal(err)
	}

	endStr := "2022-08-07 17:11:42.000000000"
	end, err := time.Parse("2006-01-02 15:04:05.000000000", endStr)
	if err != nil {
		t.Fatal(err)
	}
	startStr := "2022-08-07 16:55:42.000000000"
	start, err := time.Parse("2006-01-02 15:04:05.000000000", startStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := tqt.teq.MaxByTimeInterval(tqt.ctx, start, end, "1m"); err != nil {
		t.Fatal(err)
	}
}
