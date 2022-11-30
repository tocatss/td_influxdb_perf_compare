package query

import (
	"context"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"
)

type TdEngineQuery struct {
	pf RandTagpolicy
	db *sqlx.DB
}

func (td *TdEngineQuery) SelectFirstLastRowTime(ctx context.Context) (time.Time, time.Time, error) {
	q := "SELECT _ts FROM cpu ORDER BY _ts ASC LIMIT ?"
	st := make([]time.Time, 0, 1)
	if err := td.db.SelectContext(ctx, &st, q, 1); err != nil {
		return time.Time{}, time.Time{}, err
	}

	q = "SELECT _ts FROM cpu ORDER BY _ts DESC LIMIT ?"
	et := make([]time.Time, 0, 1)
	if err := td.db.SelectContext(ctx, &et, q, 1); err != nil {
		return time.Time{}, time.Time{}, err
	}
	return st[0], et[0], nil
}

func (td *TdEngineQuery) SelectByTags(ctx context.Context, args ...interface{}) error {
	if len(args) != 1 {
		return errors.New("args is incorrect")
	}
	pageSize, ok := args[0].(int)
	if !ok {
		return errors.New("args is incorrect")
	}

	host, cpu := td.pf()
	q := "SELECT * FROM cpu WHERE host='?' AND cpu='?' ORDER BY _ts DESC LIMIT ?"
	if _, err := td.db.QueryContext(ctx, q, host, cpu, pageSize); err != nil {
		return err
	}
	return nil
}

func (td *TdEngineQuery) SelectGroupByTags(ctx context.Context, args ...interface{}) error {
	q := "SELECT LAST_ROW(*) FROM cpu PARTITION BY host,cpu"
	if _, err := td.db.QueryContext(ctx, q); err != nil {
		return err
	}
	return nil
}

func (td *TdEngineQuery) MaxByTimeInterval(ctx context.Context, args ...interface{}) error {
	if len(args) != 3 {
		return errors.New("args is incorrect")
	}

	start, ok1 := args[0].(time.Time)
	end, ok2 := args[1].(time.Time)
	interval, ok3 := args[2].(string)
	if !ok1 || !ok2 || !ok3 {
		return errors.New("args is incorrect")
	}

	host, cpu := td.pf()
	q := `
	SELECT max(usage_user) FROM cpu 
	WHERE host='?' AND cpu='?' AND _ts >= '?' AND _ts <= '?' INTERVAL(?) 
	ORDER BY _ts DESC LIMIT 1`
	if _, err := td.db.QueryContext(ctx, q, host, cpu,
		start.Format("2006-01-02 15:04:05.000000000"),
		end.Format("2006-01-02 15:04:05.000000000"),
		interval,
	); err != nil {
		return err
	}
	return nil
}
