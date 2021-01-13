package levelcache

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/jinzhu/copier"
	jsoniter "github.com/json-iterator/go"
	"github.com/patrickmn/go-cache"
	"strconv"
	"strings"
	"time"
)

const (
	cacheKeyJoint        = "#$#"
	cacheInvalidInterval = 24 * time.Hour
	maxUpdateLen         = 1000
)

var (
	DataLoaderNotFound = errors.New("data loader not found")
)

type (
	levelCache struct {
		c       *cache.Cache
		rdb     *redis.Client
		loaders map[string]DataLoader
		cfg     CacheConfig
		version map[string]int64
		updates chan string
		stop    chan struct{}
	}

	CacheConfig struct {
		RedisAddr       string
		RedisDb         int
		RedisPassw      string
		RedisPoolSize   int
		LocalExpiration time.Duration
		CleanupInterval time.Duration
	}
)

func New(cfg CacheConfig) (*levelCache, error) {
	lc := &levelCache{
		c:       cache.New(cfg.LocalExpiration, cfg.CleanupInterval),
		loaders: make(map[string]DataLoader),
		cfg:     cfg,
		version: make(map[string]int64),
		updates: make(chan string, maxUpdateLen),
		stop:    make(chan struct{}, 1),
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassw,
		DB:       cfg.RedisDb,
	})
	if err := rdb.Ping(context.TODO()).Err(); err != nil {
		return nil, err
	}
	return lc, nil
}

func (p *levelCache) RegisterLoader(namespace string, loader DataLoader) {
	p.loaders[namespace] = loader
}

func (p *levelCache) RegisterLoaders(loaders map[string]DataLoader) {
	if len(loaders) > 0 {
		for namespace, loader := range loaders {
			p.loaders[namespace] = loader
		}
	}
}

func (p *levelCache) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case k := <-p.updates:
				_ = p.parseAndDo(ctx, k)
			case <-p.stop:
				close(p.stop)
				close(p.updates)
				return
			}
		}
	}()
}

func (p *levelCache) Get(ctx context.Context, key string, obj Cacheable) error {
	p.checkCacheUpdate(ctx, obj.Namespace(), key)
	return p.get(ctx, key, obj)
}

func (p *levelCache) get(ctx context.Context, key string, obj Cacheable) error {
	k := jointKey(obj.Namespace(), key)
	// read local cache
	if data, ok := p.c.Get(k); ok {
		return copier.Copy(obj, data)
	}

	// read redis cache
	content, err := p.rdb.Get(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	if content != "" {
		if err := jsoniter.UnmarshalFromString(content, obj); err != nil {
			return err
		}
		p.c.Set(k, obj, cacheInvalidInterval)
	}

	loader, exist := p.loaders[obj.Namespace()]
	if !exist {
		return DataLoaderNotFound
	}
	data, err := loader(ctx, key)
	if err != nil {
		return err
	}
	if err := copier.Copy(obj, data); err != nil {
		return err
	}
	p.rdb.Set(ctx, k, jsonString(obj), cacheInvalidInterval)
	p.c.Set(k, obj, cacheInvalidInterval)
	if _, ok := p.version[k]; !ok {
		p.version[k] = 0
	}
	return nil
}
func (p *levelCache) checkCacheUpdate(ctx context.Context, namespace, key string) {
	k := jointKey(namespace, key)
	vk := jointKey(namespace, key, "version")
	latestContent, err := p.rdb.Get(ctx, vk).Result()
	if err != nil {
		return
	}
	latest, err := strconv.ParseInt(latestContent, 10, 64)
	if err != nil {
		return
	}
	current, ok := p.version[k]
	if !ok {
		return
	}
	if latest != current {
		p.updates <- k
	}
}

func (p *levelCache) parseAndDo(ctx context.Context, k string) error {
	namespace, key := splitKey(k)
	loader, exist := p.loaders[namespace]
	if !exist {
		return DataLoaderNotFound
	}
	data, err := loader(ctx, key)
	if err != nil {
		return err
	}
	p.c.Set(k, data, cacheInvalidInterval)
	return nil
}

func (p *levelCache) Refresh(ctx context.Context, namespace, key string) {
	loader, exist := p.loaders[namespace]
	if !exist {
		return
	}
	data, err := loader(ctx, key)
	if err != nil {
		return
	}
	k := jointKey(namespace, key)
	p.rdb.Set(ctx, k, jsonString(data), cacheInvalidInterval)
	p.c.Set(k, data, cacheInvalidInterval)

	vk := jointKey("version", k)
	if recNo, err := p.rdb.Incr(ctx, vk).Result(); err == nil {
		p.version[k] = recNo
	}
}

func jointKey(a ...string) string {
	return strings.Join(a, cacheKeyJoint)
}
func splitKey(a string) (string, string) {
	arr := strings.Split(a, cacheKeyJoint)
	if len(arr) < 2 {
		return "", ""
	}
	return arr[0], arr[1]
}
func jsonString(obj interface{}) string {
	content, err := jsoniter.MarshalToString(obj)
	if err != nil {
		return ""
	}
	return content
}
