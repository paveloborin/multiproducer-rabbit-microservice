package dbProxy

import (
	"database/sql"
	"fmt"
	"github.com/paveloborin/multiproducer-rabbit-microservice/pkg/config"
	"time"
	"log"
)

type DbProxy struct {
	db     *sql.DB
	config *config.ResourceConfig
}

func InitDbConnection(configDb *config.ResourceConfig) (*DbProxy, error) {
	db, err := createConnect(configDb)

	return &DbProxy{db: db, config: configDb}, err

}

func createConnect(configDb *config.ResourceConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%v)/%s?charset=utf8", configDb.User, configDb.Pass, configDb.Host, configDb.Port, configDb.DbName)
	db, err := sql.Open("mysql", dsn)
	if nil != err {
		return nil, err
	}

	db.SetConnMaxLifetime(time.Minute * 1)
	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(5)

	return db, err
}

func (dbProxy *DbProxy) Query(sqlQuery string) (*sql.Rows, error) {
	err := dbProxy.db.Ping()
	if nil != err {
		log.Printf("Db query error: %s\n", err)
		for i := 0; i < 10; i++ {
			log.Println("Try reconnect to db")
			dbProxy.db, err = createConnect(dbProxy.config)
			if err == nil {
				break
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}

	}

	return dbProxy.db.Query(sqlQuery)
}

func (dbProxy *DbProxy) Close() error {
	return dbProxy.db.Close()
}
