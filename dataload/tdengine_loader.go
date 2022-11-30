package dataload

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
	_ "github.com/taosdata/driver-go/v3/taosSql"
)

type TDEngineLoader interface {
	LoadCPUFromCSVFiles(ctx context.Context, dirPath string) error
}

type tdengineLoad struct {
	db     *sqlx.DB
	tagReg *regexp.Regexp
}

func NewTDEngineLoader() *tdengineLoad {
	var taosUri = "root:taosdata@tcp(localhost:6030)/telegraf"
	taos, err := sql.Open("taosSql", taosUri)
	if err != nil {
		panic(err)
	}

	reg := regexp.MustCompile(`host(\d+)_cpu(\d+)`)
	return &tdengineLoad{db: sqlx.NewDb(taos, "taosSql"), tagReg: reg}
}

func (l *tdengineLoad) TestSQL(ctx context.Context) {
	q := "SELECT * FROM system_cpu_usage LIMIT 1"
	r := l.db.QueryRowxContext(ctx, q)
	d := make(map[string]interface{})
	err := sqlx.MapScan(r, d)
	if err != nil {
		panic(err)
	}

}

func (l *tdengineLoad) LoadCPUFromCSVFiles(ctx context.Context, dirPath string) error {
	f, err := os.Stat(dirPath)
	if err != nil {
		return err
	}
	if !f.IsDir() {
		return errors.New("is not dir")
	}

	l.ResetTable(ctx)
	var wg sync.WaitGroup
	root := os.DirFS(dirPath)
	err = fs.WalkDir(root, ".", func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}

		ext := filepath.Ext(d.Name())
		if ext != ".csv" {
			return errors.New("x")
		}
		wg.Add(1)
		go func(pp string) {
			if err := l.loadByPath(ctx, pp); err != nil {
				log.Print("load csv faild: " + err.Error())
			}
			wg.Done()
		}(filepath.Join(dirPath, path))
		return nil
	})
	if err != nil {
		return err
	}
	wg.Wait()
	return nil
}

func (l *tdengineLoad) loadByPath(ctx context.Context, fp string) error {
	match := l.tagReg.FindAllStringSubmatch(fp, 1)
	hostTag, CPUTag := match[0][1], match[0][2]

	q := fmt.Sprintf(
		"INSERT INTO %s USING cpu TAGS('%s','%s') FILE '%s' ",
		fmt.Sprintf("cpu_host_%s_cpu_%s", hostTag, CPUTag),
		CPUTag, hostTag,
		strings.ReplaceAll(fp, "\\", "/"),
	)
	log.Print(q)
	if _, err := l.db.ExecContext(ctx, q); err != nil {
		return err
	}
	return nil
}

func (l *tdengineLoad) ResetTable(ctx context.Context) error {
	q := "DROP TABLE cpu"
	if _, err := l.db.ExecContext(ctx, q); err != nil {
		return err
	}
	q = " CREATE STABLE `cpu` (`_ts` TIMESTAMP, `usage_idle` DOUBLE, `usage_nice` DOUBLE, `usage_iowait` DOUBLE, `usage_softirq` DOUBLE, `usage_steal` DOUBLE, `usage_guest_nice` DOUBLE, `usage_user` DOUBLE, `usage_system` DOUBLE, `usage_irq` DOUBLE, `usage_guest` DOUBLE) TAGS (`cpu` NCHAR(16), `host` NCHAR(16))"
	if _, err := l.db.ExecContext(ctx, q); err != nil {
		return err
	}
	return nil
}
