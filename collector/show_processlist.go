package collector

import (
	"context"
	"database/sql"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
)

const showProcesslistQuery = `
		SELECT 
    	  user, 
    	  SUBSTRING_INDEX(host, ':', 1) AS host, 
    	  COALESCE(db,'') AS db, 
    	  COALESCE(command,'') AS command 
		FROM information_schema.processlist
		`
const totalName = "total"
const activeName = "active"

var showProcesslistDesc = prometheus.NewDesc(
	prometheus.BuildFQName(namespace, "cluster", "show_processlist_info"),
	" Count connections",
	[]string{"type", "category", "sign"}, nil)

type ScrapShowProcesslist struct {
}

func (ScrapShowProcesslist) Name() string {
	return "show.process.list"
}

func (ScrapShowProcesslist) Help() string {
	return "count connections "
}

func (ScrapShowProcesslist) Version() float64 {
	return 5.1
}

func (ScrapShowProcesslist) Scrape(ctx context.Context, dbcon *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	queryContext, err := dbcon.QueryContext(ctx, showProcesslistQuery)
	if err != nil {
		return err
	}
	defer queryContext.Close()

	var user, host, dbt, command string

	//用户统计
	userCounts := make(map[string]map[string]float64)
	//来源统计
	hostCounts := make(map[string]map[string]float64)
	//数据库统计
	dbCounts := make(map[string]map[string]float64)

	for queryContext.Next() {
		err := queryContext.Scan(&user, &host, &dbt, &command)
		if err != nil {
			return err
		}
		command = sanitizeState(command)
		if host == "" {
			host = "unknown"
		}
		if dbt == "" {
			dbt = "unknown"
		}

		//init map
		if _, ok := userCounts[user]; !ok {
			userCounts[user] = make(map[string]float64)
		}
		if _, ok := userCounts[user][totalName]; !ok {
			userCounts[user][totalName] = 0
			userCounts[user][activeName] = 0
		}

		if _, ok := hostCounts[host]; !ok {
			hostCounts[host] = make(map[string]float64)
		}
		if _, ok := hostCounts[host][totalName]; !ok {
			hostCounts[host][totalName] = 0
			hostCounts[host][activeName] = 0
		}

		if _, ok := dbCounts[dbt]; !ok {
			dbCounts[dbt] = make(map[string]float64)
		}
		if _, ok := dbCounts[dbt][totalName]; !ok {
			dbCounts[dbt][totalName] = 0
			dbCounts[dbt][activeName] = 0
		}

		userCounts[user][totalName] += 1
		hostCounts[host][totalName] += 1
		dbCounts[dbt][totalName] += 1

		if command == "query" {
			userCounts[user][activeName] += 1
			hostCounts[host][activeName] += 1
			dbCounts[dbt][activeName] += 1
		}
	}

	for key, value := range userCounts {
		ch <- prometheus.MustNewConstMetric(showProcesslistDesc, prometheus.GaugeValue, value[totalName], "user", key, totalName)
		ch <- prometheus.MustNewConstMetric(showProcesslistDesc, prometheus.GaugeValue, value[activeName], "user", key, activeName)
	}

	for key, value := range hostCounts {
		ch <- prometheus.MustNewConstMetric(showProcesslistDesc, prometheus.GaugeValue, value[totalName], "host", key, totalName)
		ch <- prometheus.MustNewConstMetric(showProcesslistDesc, prometheus.GaugeValue, value[activeName], "host", key, activeName)
	}

	for key, value := range dbCounts {
		ch <- prometheus.MustNewConstMetric(showProcesslistDesc, prometheus.GaugeValue, value[totalName], "db", key, totalName)
		ch <- prometheus.MustNewConstMetric(showProcesslistDesc, prometheus.GaugeValue, value[activeName], "db", key, activeName)
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

//之前版本的直接使用命令
//const showProcesslistQuery = "SHOW FULL PROCESSLIST"
//const totalName = "total"
//const activeName = "active"
//
//var showProcesslistDesc = prometheus.NewDesc(
//	prometheus.BuildFQName(namespace, "cluster", "show_processlist_info"),
//	" Count connections",
//	[]string{"type", "category", "sign"}, nil)
//
//type ScrapShowProcesslist struct {
//}
//
//func (ScrapShowProcesslist) Name() string {
//	return "show.process.list"
//}
//
//func (ScrapShowProcesslist) Help() string {
//	return "count connections "
//}
//
//func (ScrapShowProcesslist) Version() float64 {
//	return 5.1
//}
//
//func (ScrapShowProcesslist) Scrape(ctx context.Context, dbcon *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
//	queryContext, err := dbcon.QueryContext(ctx, showProcesslistQuery)
//	if err != nil {
//		return err
//	}
//	defer queryContext.Close()
//
//	var Id, Time, Time_ms, Rows_sent, Rows_examined int64
//	var User, Host, db, Command string
//	var Info sql.NullString
//
//	//用户统计
//	userCounts := make(map[string]map[string]float64)
//	//来源统计
//	hostCounts := make(map[string]map[string]float64)
//	//数据库统计
//	dbCounts := make(map[string]map[string]float64)
//
//	for queryContext.Next() {
//		var dbt, state sql.NullString
//		err := queryContext.Scan(&Id, &User, &Host, &dbt, &Command, &Time, &state, &Info, &Time_ms, &Rows_sent, &Rows_examined)
//		if err != nil {
//			return err
//		}
//		Command = sanitizeState(Command)
//		if Host == "" {
//			Host = "unknown"
//		} else {
//			Host = strings.Split(Host, ":")[0]
//		}
//		db = dbt.String
//
//		//init map
//		if _, ok := userCounts[User]; !ok {
//			userCounts[User] = make(map[string]float64)
//		}
//		if _, ok := userCounts[User][totalName]; !ok {
//			userCounts[User][totalName] = 0
//			userCounts[User][activeName] = 0
//		}
//
//		if _, ok := hostCounts[Host]; !ok {
//			hostCounts[Host] = make(map[string]float64)
//		}
//		if _, ok := hostCounts[Host][totalName]; !ok {
//			hostCounts[Host][totalName] = 0
//			hostCounts[Host][activeName] = 0
//		}
//
//		if _, ok := dbCounts[db]; !ok {
//			dbCounts[db] = make(map[string]float64)
//		}
//		if _, ok := dbCounts[db][totalName]; !ok {
//			dbCounts[db][totalName] = 0
//			dbCounts[db][activeName] = 0
//		}
//
//		userCounts[User][totalName] += 1
//		hostCounts[Host][totalName] += 1
//		dbCounts[db][totalName] += 1
//
//		if Command == "query" {
//			userCounts[User][activeName] += 1
//			hostCounts[Host][activeName] += 1
//			dbCounts[db][activeName] += 1
//		}
//	}
//
//	for key, value := range userCounts {
//		ch <- prometheus.MustNewConstMetric(showProcesslistDesc, prometheus.GaugeValue, value[totalName], "user", key, totalName)
//		ch <- prometheus.MustNewConstMetric(showProcesslistDesc, prometheus.GaugeValue, value[activeName], "user", key, activeName)
//	}
//
//	for key, value := range hostCounts {
//		ch <- prometheus.MustNewConstMetric(showProcesslistDesc, prometheus.GaugeValue, value[totalName], "host", key, totalName)
//		ch <- prometheus.MustNewConstMetric(showProcesslistDesc, prometheus.GaugeValue, value[activeName], "host", key, activeName)
//	}
//
//	for key, value := range dbCounts {
//		ch <- prometheus.MustNewConstMetric(showProcesslistDesc, prometheus.GaugeValue, value[totalName], "db", key, totalName)
//		ch <- prometheus.MustNewConstMetric(showProcesslistDesc, prometheus.GaugeValue, value[activeName], "db", key, activeName)
//	}
//
//	return nil
//}
//
//func sanitizeState(state string) string {
//	if state == "" {
//		state = "unknown"
//	}
//	state = strings.ToLower(state)
//	replacements := map[string]string{
//		";": "",
//		",": "",
//		":": "",
//		".": "",
//		"(": "",
//		")": "",
//		" ": "_",
//		"-": "_",
//	}
//	for r := range replacements {
//		state = strings.Replace(state, r, replacements[r], -1)
//	}
//	return state
//}
//
//var _ Scraper = ScrapShowProcesslist{}
