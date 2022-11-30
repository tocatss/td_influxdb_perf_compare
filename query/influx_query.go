package query

import (
	"context"
	"errors"
	"fmt"
	"time"

	_ "github.com/influxdata/influxdb1-client"
	client "github.com/influxdata/influxdb1-client/v2"
)

type InfluxQuery struct {
	pf     RandTagpolicy
	client client.Client
}

func (iq *InfluxQuery) SelectFirstLastRowTime(ctx context.Context) (time.Time, time.Time, error) {
	q := client.NewQuery(
		fmt.Sprintf(
			"SELECT * FROM cpu ORDER BY time ASC LIMIT %d",
			1,
		),
		"csv",
		"",
	)
	response, err := iq.client.Query(q)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	if response.Error() != nil {
		return time.Time{}, time.Time{}, response.Error()
	}
	timeStr := response.Results[0].Series[0].Values[0][0].(string)
	startTime, err := time.Parse("2006-01-02T15:04:05Z", timeStr)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}

	q = client.NewQuery(
		fmt.Sprintf(
			"SELECT * FROM cpu ORDER BY time DESC LIMIT %d",
			1,
		),
		"csv",
		"",
	)
	response, err = iq.client.Query(q)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	if response.Error() != nil {
		return time.Time{}, time.Time{}, response.Error()
	}
	endTimeStr := response.Results[0].Series[0].Values[0][0].(string)
	endTime, err := time.Parse("2006-01-02T15:04:05Z", endTimeStr)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return startTime, endTime, nil
}

func (iq *InfluxQuery) SelectByTags(ctx context.Context, args ...interface{}) error {
	if len(args) != 1 {
		return errors.New("args is incorrect")
	}
	pageSize, ok := args[0].(int)
	if !ok {
		return errors.New("args is incorrect")
	}

	host, cpu := iq.pf()

	q := client.NewQuery(
		fmt.Sprintf(
			"SELECT * FROM cpu WHERE host='%s' AND cpu='%s' ORDER BY time DESC LIMIT %d",
			host, cpu, pageSize,
		),
		"csv",
		"",
	)
	response, err := iq.client.Query(q)
	if err != nil {
		return err
	}
	if response.Error() != nil {
		return response.Error()
	}
	return nil
}

func (iq *InfluxQuery) SelectGroupByTags(ctx context.Context, args ...interface{}) error {
	q := client.NewQuery(
		fmt.Sprintf(
			"SELECT * FROM cpu GROUP BY host,cpu ORDER BY time DESC LIMIT %d",
			1,
		),
		"csv",
		"",
	)
	response, err := iq.client.Query(q)
	if err != nil {
		return err
	}
	if response.Error() != nil {
		return response.Error()
	}
	return nil
}

func (iq *InfluxQuery) MaxByTimeInterval(ctx context.Context, args ...interface{}) error {
	if len(args) != 3 {
		return errors.New("args is incorrect")
	}

	start, ok1 := args[0].(time.Time)
	end, ok2 := args[1].(time.Time)
	interval, ok3 := args[2].(string)
	if !ok1 || !ok2 || !ok3 {
		return errors.New("args is incorrect")
	}

	host, cpu := iq.pf()
	q := client.NewQuery(
		fmt.Sprintf(`SELECT max(usage_user) FROM cpu WHERE host='%s' AND cpu='%s' 
			AND time >= '%s' AND time <= '%s'
			GROUP BY time(%s) ORDER BY time DESC LIMIT %d`,
			host, cpu,
			start.Format("2006-01-02 15:04:05.000000000"),
			end.Format("2006-01-02 15:04:05.000000000"),
			interval,
			1,
		),
		"csv",
		"",
	)
	response, err := iq.client.Query(q)
	if err != nil {
		return err
	}
	if response.Error() != nil {
		return response.Error()
	}
	return nil
}
