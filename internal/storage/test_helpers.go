package storage

import (
	"fmt"
)

// intializes a transaction logger reads the logger and executes it
func initializeTransactionLogger(filename string) (TransactionLogger, error) {

	logger, err := NewFileTransctionLogger(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := logger.ReadEvents()

	e := Event{}
	ok := true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete:
				err = Delete(e.Key)
			case EventPut:
				err = Put(e.Key, e.Value)
			}
		}
	}
	logger.Run()

	return logger, err
}
