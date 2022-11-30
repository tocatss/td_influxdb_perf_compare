package query

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/jmoiron/sqlx"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Queryer interface {
	// args = pageSize
	SelectByTags(ctx context.Context, args ...interface{}) error
	// args = nil
	SelectGroupByTags(ctx context.Context, args ...interface{}) error
	// args: start, end time.Time, interval string
	MaxByTimeInterval(ctx context.Context, args ...interface{}) error

	SelectFirstLastRowTime(ctx context.Context) (time.Time, time.Time, error)
}

type QueryManager struct {
	iq Queryer
	tq Queryer
}

func NewQueryManager() *QueryManager {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://127.0.0.1:8086",
	})
	if err != nil {
		panic(err)
	}
	if _, _, err := c.Ping(1 * time.Second); err != nil {
		panic(err)
	}
	iq := &InfluxQuery{
		pf:     pf,
		client: c,
	}

	taosUri := "root:taosdata@tcp(localhost:6030)/csv"
	taos, err := sql.Open("taosSql", taosUri)
	if err != nil {
		panic(err)
	}

	teq := &TdEngineQuery{
		pf: pf,
		db: sqlx.NewDb(taos, "taosSql"),
	}
	if err := teq.db.Ping(); err != nil {
		panic(err)
	}

	return &QueryManager{
		iq: iq,
		tq: teq,
	}
}

func (qm *QueryManager) CostSelectByTags(ctx context.Context) error {
	pageSize := 12
	influxCostSelectByTagsWithPageSize, err := qm.CalculateMilliseconds(
		qm.iq.SelectByTags, 100)(ctx, pageSize)
	if err != nil {
		return err
	}
	time.Sleep(1 * time.Second)

	tdengineCostSelectByTagsWithPageSize, err := qm.CalculateMilliseconds(
		qm.tq.SelectByTags, 100)(ctx, pageSize)
	if err != nil {
		return err
	}

	time.Sleep(1 * time.Second)

	influxCostSelectNewOneByTags, err := qm.CalculateMilliseconds(
		qm.iq.SelectByTags, 100)(ctx, 1)
	if err != nil {
		return err
	}

	time.Sleep(1 * time.Second)

	tdengineCostSelectNewOneByTags, err := qm.CalculateMilliseconds(
		qm.tq.SelectByTags, 100)(ctx, 1)
	if err != nil {
		return err
	}

	log.Printf(
		"influx cost %dms pageSize=%d influx cost %dms pageSize=1\n tdengine cost %dms pageSize=%d tdengine cost %dms pageSize=1 ",
		influxCostSelectByTagsWithPageSize, pageSize,
		influxCostSelectNewOneByTags,
		tdengineCostSelectByTagsWithPageSize, pageSize,
		tdengineCostSelectNewOneByTags,
	)
	return nil
}

func (qm *QueryManager) CostSelectNewOneGroupByTags(ctx context.Context) error {
	influxCostSelectNewOneGroupByTags, err := qm.CalculateMilliseconds(
		qm.iq.SelectGroupByTags, 100)(ctx)
	if err != nil {
		return err
	}
	tdengineCostSelectNewOneGroupByTags, err := qm.CalculateMilliseconds(
		qm.tq.SelectGroupByTags, 100)(ctx)
	if err != nil {
		return err
	}

	log.Printf(
		"influx cost %dms tdengine cost %dms ",
		influxCostSelectNewOneGroupByTags,
		tdengineCostSelectNewOneGroupByTags,
	)

	return nil
}

func (qm *QueryManager) CostMaxByTimeInterval(ctx context.Context) error {
	is, ie, err := qm.iq.SelectFirstLastRowTime(ctx)
	if err != nil {
		return err
	}
	is, ie = random1HourIn(is, ie)

	ts, te, err := qm.tq.SelectFirstLastRowTime(ctx)
	if err != nil {
		return err
	}
	ts, te = random1HourIn(ts, te)

	influxCostMaxByTimeInterval1m, err := qm.CalculateMilliseconds(
		qm.iq.MaxByTimeInterval, 100)(ctx, is, ie, "1m")
	if err != nil {
		return err
	}
	influxCostMaxByTimeInterval10m, err := qm.CalculateMilliseconds(
		qm.iq.MaxByTimeInterval, 100)(ctx, is, ie, "10m")
	if err != nil {
		return err
	}

	time.Sleep(1 * time.Second)

	tdengineCostMaxByTimeInterval1m, err := qm.CalculateMilliseconds(
		qm.tq.MaxByTimeInterval, 100)(ctx, ts, te, "1m")
	if err != nil {
		return err
	}
	tdengineCostMaxByTimeInterval10m, err := qm.CalculateMilliseconds(
		qm.tq.MaxByTimeInterval, 100)(ctx, ts, te, "10m")
	if err != nil {
		return err
	}

	log.Printf(
		"influx interval(1m) cost %dms, interval(10m) cost %dms \ntdengine interval(1m) cost %dms, interval(10m) cost %dms",
		influxCostMaxByTimeInterval1m,
		influxCostMaxByTimeInterval10m,
		tdengineCostMaxByTimeInterval1m,
		tdengineCostMaxByTimeInterval10m,
	)

	return nil
}

type CalculateFunc func(ctx context.Context, args ...interface{}) (int, error)

func (qm *QueryManager) CalculateMilliseconds(f func(context.Context, ...interface{}) error, count int) CalculateFunc {
	return CalculateFunc(func(ctx context.Context, args ...interface{}) (int, error) {
		start := time.Now()
		for i := 0; i < count; i++ {
			if err := f(ctx, args...); err != nil {
				return 0, err
			}
		}
		end := time.Now()

		return int(end.Sub(start).Milliseconds()) / count, nil
	})
}

type RandTagpolicy func() (string, string)

func pf() (string, string) {
	maxHost, maxCpu := 25, 12

	return fmt.Sprintf("host%d", rand.Intn(maxHost+1)),
		fmt.Sprintf("cpu%d", rand.Intn(maxCpu+1))
}

func random1HourIn(start, end time.Time) (time.Time, time.Time) {
	overHours := int(end.Sub(start).Hours())
	if overHours <= 1 {
		return start, start.Add(1 * time.Hour)
	}

	random := rand.Intn(overHours)
	return start.Add(time.Duration(random) * time.Hour),
		start.Add(time.Duration(random) * time.Hour).Add(1 * time.Hour)

}

// 废弃
func (qm *QueryManager) Process(ctx context.Context) error {
	return nil
}
