package query

import (
	"context"
	"math/rand"
	"testing"
	"time"
)

type testQueryManager struct {
	ctx context.Context
	qm  *QueryManager
}

func newTestQueryManger(t *testing.T) *testQueryManager {
	ctx := context.Background()
	qm := NewQueryManager()

	return &testQueryManager{
		ctx: ctx,
		qm:  qm,
	}
}

func TestCostSelectByTags(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "ok",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tqm := newTestQueryManger(t)
			if err := tqm.qm.CostSelectByTags(tqm.ctx); err != nil {
				t.Fatal(err)
			}
		})
	}

}

func TestCostSelectNewOneGroupByTags(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "ok",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tqm := newTestQueryManger(t)
			if err := tqm.qm.CostSelectNewOneGroupByTags(tqm.ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCostMaxByTimeInterval(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "ok",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tqm := newTestQueryManger(t)
			if err := tqm.qm.CostMaxByTimeInterval(tqm.ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRandom1HourIn(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	s, e := random1HourIn(time.Now(), time.Now().Add(30*time.Minute))
	t.Log(s, e)

	s1, e1 := random1HourIn(time.Now(), time.Now().Add(30*time.Hour))
	t.Log(s1, e1)
}
