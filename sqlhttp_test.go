package scopy

import (
	"fmt"
	"os"
	"testing"
)

func TestSqlHttp(t *testing.T) {
	dbname := os.Getenv("sqlhttp_db_name")
	dbusername := os.Getenv("sqlhttp_db_username")
	dbpassword := os.Getenv("sqlhttp_db_password")

	fmt.Println(dbname, dbusername, dbpassword)

	target, err := DBHTTP("http://localhost:8083/aceql",
		dbname, dbusername, dbpassword, "", 0)
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

	_, err = target.c.ExecuteUpdate(sess, DefaultInitSQL, nil, false)
	if err != nil {
		t.Error(err)
		return
	}

	runTest(t, target)
	runTest(t, target)
}

func TestSqlHttpOpen(t *testing.T) {
	dbname := os.Getenv("sqlhttp_db_name")
	dbusername := os.Getenv("sqlhttp_db_username")
	dbpassword := os.Getenv("sqlhttp_db_password")

	fmt.Println(dbname, dbusername, dbpassword)

	target, _, err := Open("db+http://localhost:8083/aceql?sc_dbname="+dbname+"&sc_dbtable=abc",
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

	_, err = target.(*sqlhttpTarget).c.ExecuteUpdate(sess, DefaultResetSQL, nil, false)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = target.(*sqlhttpTarget).c.ExecuteUpdate(sess, DefaultInitSQL, nil, false)
	if err != nil {
		t.Error(err)
		return
	}

	runTest(t, target)
	runTest(t, target)
}