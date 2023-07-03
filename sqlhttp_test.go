package scopy

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestSqlHttp(t *testing.T) {
	dbdrv := os.Getenv("sqlhttp_db_driver")
	dburl := os.Getenv("sqlhttp_db_url")
	if dburl == "" {
		dburl = "http://localhost:9090/aceql"
	}
	dbname := os.Getenv("sqlhttp_db_name")
	dbusername := os.Getenv("sqlhttp_db_username")
	dbpassword := os.Getenv("sqlhttp_db_password")

	fmt.Println("sqlhttp_db_driver =", dbdrv)
	fmt.Println("sqlhttp_db_name =", dbname)
	fmt.Println("sqlhttp_db_username =", dbusername)
	fmt.Println("sqlhttp_db_password =", dbpassword)

	target, err := DBHTTP(dburl,
		dbname, dbusername, dbpassword, "", 0, true)
	if err != nil {
		t.Error(err)
		return
	}

	sess, err := target.GetSession()
	if err != nil {
		t.Error(err)
		return
	}

	_, err = target.c.ExecuteUpdate(sess, DefaultResetSQL, nil, false)
	if err != nil {
		t.Error(err)
		return
	}

	initSql := DefaultInitSQL
	if dbdrv == "mysql" {
		initSql = strings.Replace(initSql, "bytea", "blob", -1)
	}
	_, err = target.c.ExecuteUpdate(sess, initSql, nil, false)
	if err != nil {
		t.Error(err)
		return
	}

	runTest(t, target)
	runTest(t, target)
}

func TestSqlHttpOpen(t *testing.T) {
	dburl := os.Getenv("sqlhttp_db_url")
	if dburl == "" {
		dburl = "http://localhost:9090/aceql"
	}
	dbdrv := os.Getenv("sqlhttp_db_driver")
	dbname := os.Getenv("sqlhttp_db_name")
	dbusername := os.Getenv("sqlhttp_db_username")
	dbpassword := os.Getenv("sqlhttp_db_password")

	fmt.Println("sqlhttp_db_driver =", dbdrv)
	fmt.Println("sqlhttp_db_name =", dbname)
	fmt.Println("sqlhttp_db_username =", dbusername)
	fmt.Println("sqlhttp_db_password =", dbpassword)

	target, _, err := Open("db+"+dburl+"?sc_dbname="+dbname+"&sc_dbtable=abc",
		dbusername, dbpassword)
	if err != nil {
		t.Error(err)
		return
	}

	sess, err := target.(*sqlhttpTarget).GetSession()
	if err != nil {
		t.Error(err)
		return
	}

	_, err = target.(*sqlhttpTarget).c.ExecuteUpdate(sess, strings.Replace(DefaultResetSQL, "tpt_files", "abc", -1), nil, false)
	if err != nil {
		t.Error(err)
		return
	}

	initSql := strings.Replace(DefaultInitSQL, "tpt_files", "abc", -1)
	if dbdrv == "mysql" {
		initSql = strings.Replace(initSql, "bytea", "blob", -1)
	}
	_, err = target.(*sqlhttpTarget).c.ExecuteUpdate(sess, initSql, nil, false)
	if err != nil {
		t.Error(err)
		return
	}

	runTest(t, target)
	runTest(t, target)
}
