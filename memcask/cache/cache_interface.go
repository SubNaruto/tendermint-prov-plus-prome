package cache

type CacheInterface interface {
	Put(key, value []byte)
	Get(key []byte) ([]byte, bool)
}
