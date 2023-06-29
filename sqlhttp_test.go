package scopy

import (
	"testing"
)

func TestSqlHttp(t *testing.T) {
	target, err := DBHTTP("http://localhost:8083/aceql", "sampledb", os.Getenv("db_username"), os.Getenv("db_password"), "", 0)
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
