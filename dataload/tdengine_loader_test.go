package dataload

import (
	"context"
	"testing"
)

func Test_LoadCPUFromCSVFiles(t *testing.T) {
	ctx := context.Background()
	ts := NewTDEngineLoader()
	err := ts.LoadCPUFromCSVFiles(ctx, `D:\code\ivst\asset\tdengine`)
	if err != nil {
		t.Fatal(err)
	}
}
