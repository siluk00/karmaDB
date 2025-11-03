package storage

import (
	"errors"
	"sync"
)

type lockableMap struct {
	sync.RWMutex
	m map[string]string
}

var store = lockableMap{
	m: make(map[string]string),
}

func Put(key, value string) error {
	store.Lock()
	defer store.Unlock()

	store.m[key] = value

	return nil
}

var ErrNoSuchKey = errors.New("no such key")

func Get(key string) (string, error) {
	store.RLock()
	defer store.RUnlock()

	value, ok := store.m[key]
	if !ok {
		return "", ErrNoSuchKey
	}

	return value, nil
}

func Delete(key string) error {
	store.Lock()
	defer store.Unlock()
	delete(store.m, key)

	return nil
}
