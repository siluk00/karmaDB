package main

import (
	"fmt"
	"log"

	"github.com/siluk00/karmaDB/internal/api/server"
	"github.com/siluk00/karmaDB/internal/storage"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}

func initializeTransactionLogger(filename string) (storage.TransactionLogger, error) {

	logger, err := storage.NewFileTransctionLogger(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := logger.ReadEvents()

	e := storage.Event{}
	ok := true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case storage.EventDelete:
				err = storage.Delete(e.Key)
			case storage.EventPut:
				err = storage.Put(e.Key, e.Value)
			}
		}
	}
	logger.Run()

	return logger, err
}
