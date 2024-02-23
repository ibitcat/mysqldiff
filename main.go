package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

var (
	MysqlDB *sql.DB

	// flag
	dbHelp        bool
	dbUser        string
	dbPass        string
	dbHost        string
	dbPort        int
	dbName        string
	dbFile        string
	dbChar        string
	onlyCk        bool
	modifySrcFile bool
)

func init() {
	flag.BoolVar(&dbHelp, "help", false, "Mysql diff help.")
	flag.StringVar(&dbUser, "u", "", "User for login if not current user.")
	flag.StringVar(&dbPass, "p", "", "Password to use when connecting to server.")
	flag.StringVar(&dbHost, "h", "localhost", "Connect to host.")
	flag.IntVar(&dbPort, "P", 3306, "Port number to use for connection.")
	flag.StringVar(&dbName, "d", "", "Database to diff.")
	flag.StringVar(&dbFile, "f", "", "Read this sql file to update database.")
	flag.StringVar(&dbChar, "default-character-set", "utf8mb4", "Set the default character set.")
	flag.BoolVar(&onlyCk, "only-check", false, "Only check diff.")
	flag.BoolVar(&modifySrcFile, "modify", false, "Modified source file .")

	flag.Usage = usage
}

func usage() {
	fmt.Fprintf(os.Stderr, `mysqldiff version: 0.1.0
Usage: mysqldiff [OPTIONS]

    eg.: mysqldiff -u root -p 123456 -h 127.0.0.1 -P 3306 -d database -f filename.sql

Options:
`)
	flag.PrintDefaults()
}

func main() {
	flag.Parse()
	if dbHelp || flag.NFlag() <= 0 {
		flag.Usage()
		return
	}

	var err error
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=%s", dbUser, dbPass, dbHost, dbPort, dbChar)
	MysqlDB, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Println("打开数据库错误, err:" + err.Error())
		return
	}
	MysqlDB.SetMaxOpenConns(1) // 不使用连接池
	defer MysqlDB.Close()

	err = MysqlDB.Ping()
	if err != nil {
		log.Fatalln("连接数据库错误, err:" + err.Error())
	} else {
		log.Println("数据库连接成功")
	}

	// update
	mysqlDiffUpdate(dbFile, dbName)
}
