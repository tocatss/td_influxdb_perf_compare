package datagen

import (
	"testing"
	"time"
)

// func TestSimulatorTdEngineCSV(t *testing.T) {
// 	s := &CPUDataSimulator{
// 		CPUNumber:  12,
// 		HostNumber: 26,
// 		Duration:   time.Duration(10 * time.Minute),
// 		Interval:   time.Duration(10 * time.Second),
// 		StartFrom:  time.Now().Add(-1 * 30 * 24 * time.Hour),
// 	}
// 	n, err := s.TdEngineCSV("D:\\code\\ivst\\asset\\tdengine")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	t.Log(n)
// }

func TestSimulatorCSV(t *testing.T) {
	tests := []struct {
		name     string
		cs       *CPUDataSimulator
		typeFlag flag
		fileName string
		dirPath  string
	}{
		{
			name: " typeFlagCommonOne",
			cs: &CPUDataSimulator{
				CPUNumber:  12,
				HostNumber: 26,
				Duration:   time.Duration(10 * time.Minute),
				Interval:   time.Duration(10 * time.Second),
				StartFrom:  time.Now().Add(-1 * 30 * 24 * time.Hour),
			},
			typeFlag: TypeFlagCommonOne,
			fileName: "cpu.csv",
			dirPath:  `D:\code\ivst\asset\common`,
		},
		// {
		// 	name: " typeFlagTdengine",
		// 	cs: &CPUDataSimulator{
		// 		CPUNumber:  12,
		// 		HostNumber: 26,
		// 		Duration:   time.Duration(10 * time.Minute),
		// 		Interval:   time.Duration(10 * time.Second),
		// 		StartFrom:  time.Now().Add(-1 * 30 * 24 * time.Hour),
		// 	},
		// 	typeFlag: TypeFlagTDEngine,
		// 	fileName: "",
		// 	dirPath:  `D:\code\ivst\asset\tdengine`,
		// },
		// {
		// 	name: " typeFlagCommonMulti",
		// 	cs: &CPUDataSimulator{
		// 		CPUNumber:  12,
		// 		HostNumber: 26,
		// 		Duration:   time.Duration(10 * time.Minute),
		// 		Interval:   time.Duration(10 * time.Second),
		// 		StartFrom:  time.Now().Add(-1 * 30 * 24 * time.Hour),
		// 	},
		// 	typeFlag: TypeFlagCommonMulti,
		// 	fileName: "",
		// 	dirPath:  `D:\code\ivst\asset\common`,
		// },
		// {
		// 	name: " typeFlagCommonMulti",
		// 	cs: &CPUDataSimulator{
		// 		CPUNumber:  12,
		// 		HostNumber: 26,
		// 		Duration:   time.Duration(10 * time.Minute),
		// 		Interval:   time.Duration(10 * time.Second),
		// 		StartFrom:  time.Now().Add(-1 * 30 * 24 * time.Hour),
		// 	},
		// 	typeFlag: TypeFlagCommonMulti,
		// 	fileName: "",
		// 	dirPath:  `D:\code\ivst\asset\common`,
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cs.CSV(tt.dirPath, tt.fileName, tt.typeFlag)
		})
	}
}
