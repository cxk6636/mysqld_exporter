package collector

import (
	"context"
	"database/sql"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
)

const showProcesslistQuery = "SHOW PROCESSLIST"

var (
	showprocesslistDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "cluster", "show_processlist_info"),
		"xxxxxx",
		[]string{"type", "category", "sign"}, nil)
)

type ScrapShowProcesslist struct {
}

func (ScrapShowProcesslist) Name() string {
	return "showprocesslist"
}

func (ScrapShowProcesslist) Help() string {
	return "xxxxxxxxxx"
}

func (ScrapShowProcesslist) Version() float64 {
	return 5.1
}

const total_name = "total"
const active_name = "active"

func (ScrapShowProcesslist) Scrape(ctx context.Context, dbcon *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	queryContext, err := dbcon.QueryContext(ctx, showProcesslistQuery)
	if err != nil {
		return err
	}
	defer queryContext.Close()

	var Id, Time int64
	var User, Host, db, Command string
	var Info sql.NullString

	//用户统计
	userCounts := make(map[string]map[string]uint32)
	//来源统计
	hostCounts := make(map[string]map[string]uint32)
	//数据库统计
	dbCounts := make(map[string]map[string]uint32)

	for queryContext.Next() {
		var dbt, state sql.NullString
		err := queryContext.Scan(&Id, &User, &Host, &dbt, &Command, &Time, &state, &Info)
		if err != nil {
			return err
		}
		Command = sanitizeState(Command)
		if Host == "" {
			Host = "unknown"
		}
		Host = strings.Split(Host, ":")[0]
		db = dbt.String

		//init map
		if _, ok := userCounts[User]; !ok {
			userCounts[User] = make(map[string]uint32)
		}
		if _, ok := userCounts[User][total_name]; !ok {
			userCounts[User][total_name] = 0
			userCounts[User][active_name] = 0
		}

		if _, ok := hostCounts[Host]; !ok {
			hostCounts[Host] = make(map[string]uint32)
		}
		if _, ok := hostCounts[Host][total_name]; !ok {
			hostCounts[Host][total_name] = 0
			hostCounts[Host][active_name] = 0
		}

		if _, ok := dbCounts[db]; !ok {
			dbCounts[db] = make(map[string]uint32)
		}
		if _, ok := dbCounts[db][total_name]; !ok {
			dbCounts[db][total_name] = 0
			dbCounts[db][active_name] = 0
		}

		userCounts[User][total_name] += 1
		hostCounts[Host][total_name] += 1
		dbCounts[db][total_name] += 1

		if Command == "query" {
			userCounts[User][active_name] += 1
			hostCounts[Host][active_name] += 1
			dbCounts[db][active_name] += 1
		}
	}

	for key, value := range userCounts {
		total := float64(value[total_name])
		active := float64(value[active_name])
		ch <- prometheus.MustNewConstMetric(showprocesslistDesc, prometheus.GaugeValue, total, "user", key, total_name)
		ch <- prometheus.MustNewConstMetric(showprocesslistDesc, prometheus.GaugeValue, active, "user", key, active_name)
	}

	for key, value := range hostCounts {
		total := float64(value[total_name])
		active := float64(value[active_name])
		ch <- prometheus.MustNewConstMetric(showprocesslistDesc, prometheus.GaugeValue, total, "host", key, total_name)
		ch <- prometheus.MustNewConstMetric(showprocesslistDesc, prometheus.GaugeValue, active, "host", key, active_name)
	}

	for key, value := range dbCounts {
		total := float64(value[total_name])
		active := float64(value[active_name])
		ch <- prometheus.MustNewConstMetric(showprocesslistDesc, prometheus.GaugeValue, total, "db", key, total_name)
		ch <- prometheus.MustNewConstMetric(showprocesslistDesc, prometheus.GaugeValue, active, "db", key, active_name)
	}

	return nil
}

func sanitizeState(state string) string {
	if state == "" {
		state = "unknown"
	}
	state = strings.ToLower(state)
	replacements := map[string]string{
		";": "",
		",": "",
		":": "",
		".": "",
		"(": "",
		")": "",
		" ": "_",
		"-": "_",
	}
	for r := range replacements {
		state = strings.Replace(state, r, replacements[r], -1)
	}
	return state
}

var _ Scraper = ScrapShowProcesslist{}
