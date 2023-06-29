package scopy

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestDB(t *testing.T) {
	username := os.Getenv("mysql_username")
	if username == "" {
		username = "moo_test"
	}
	password := os.Getenv("mysql_password")
	if password == "" {
		password = "moo_test_password"
	}
	dbname := os.Getenv("mysql_dbname")
	if dbname == "" {
		dbname = "mytest"
	}

	target, err := DB("mysql",
		username+":"+password+"@tcp(192.168.1.2:3306)/"+dbname+"?timeout=90s&collation=utf8mb4_unicode_ci&parseTime=true",
		"", 0)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = target.conn.Exec(DefaultResetSQL)
	if err != nil {
		t.Error(err)
		return
	}

	s := strings.Replace(DefaultInitSQL, "bytea", "blob", -1)
	_, err = target.conn.Exec(s)
	if err != nil {
		t.Error(err)
		return
	}

	runTest(t, target)
	runTest(t, target)
}

func TestDBOpen(t *testing.T) {
	username := os.Getenv("mysql_username")
	if username == "" {
		username = "moo_test"
	}
	password := os.Getenv("mysql_password")
	if password == "" {
		password = "moo_test_password"
	}
	dbname := os.Getenv("mysql_dbname")
	if dbname == "" {
		dbname = "mytest"
	}

	urlstr := "db+mysql://192.168.1.2:3306/" + dbname + "?timeout=90s&collation=utf8mb4_unicode_ci&parseTime=true"

	target, _, err := Open(urlstr, username, password)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = target.(*dbTarget).conn.Exec(DefaultResetSQL)
	if err != nil {
		t.Error(err)
		return
	}

	s := strings.Replace(DefaultInitSQL, "bytea", "blob", -1)
	_, err = target.(*dbTarget).conn.Exec(s)
	if err != nil {
		t.Error(err)
		return
	}

	runTest(t, target)
	runTest(t, target)
}

func runTest(t *testing.T, target Session) {
	excepted := "adsdfsddf"

	exists, err := target.Exists("AAA.txt")
	if err != nil {
		t.Error(err)
		return
	}
	if exists {
		err = target.Delete("AAA.txt")
		if err != nil {
			t.Error(err)
			return
		}
	}

	writer, err := target.Write("AAA.txt")
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 10; i++ {
		_, err = writer.Write([]byte(excepted))
		if err != nil {
			t.Error(err)
			return
		}
	}
	excepted = strings.Repeat(excepted, 10)

	for i := 0; i < 10; i++ {
		err = writer.Close()
		if err != nil {
			t.Error(err)
			return
		}
	}

	reader, err := target.Read("AAA.txt")
	if err != nil {
		t.Error(err)
		return
	}

	bs, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 10; i++ {
		err = reader.Close()
		if err != nil {
			t.Error(err)
			return
		}
	}

	if excepted != string(bs) {
		t.Error("want:", excepted)
		t.Error(" got:", string(bs))
	}

	fis, err := target.List("")
	if err != nil {
		t.Error(err)
		return
	}

	found := false
	for _, fi := range fis {
		if fi.Name() == "AAA.txt" {
			found = true
		}
	}
	if !found {
		t.Error("Not found")
	}

	err = target.Rename("AAA.txt", "BBB.txt")
	if err != nil {
		t.Error(err)
		return
	}

	exists, err = target.Exists("BBB.txt")
	if err != nil {
		t.Error(err)
		return
	}
	if exists {
		err = target.Delete("AAA.txt")
		if err != nil {
			t.Error(err)
			return
		}
	} else {
		t.Error("BBB.txt isnot exists")
	}
}
