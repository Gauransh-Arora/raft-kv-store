package storage

import "sync"


type KVStore struct{
	mu sync.RWMutex
	data map[string]string
}

func NewKVStore() *KVStore{
	return &KVStore{
		data: make(map[string]string),
	}
}

func (kv *KVStore) Set(key, value string){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = value
}

func (kv *KVStore) Get(key string) (string, bool){
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	value, ok := kv.data[key]
	return value, ok
}

func (kv *KVStore) Delete(key string){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.data, key)
}