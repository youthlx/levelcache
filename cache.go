package levelcache

import (
	"context"
	"fmt"
	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"github.com/jinzhu/copier"
	jsoniter "github.com/json-iterator/go"
	"github.com/patrickmn/go-cache"
	"strconv"
	"strings"
	"time"
)

const (
	cacheKeyJoint               = "#$#"
	defaultCacheInvalidInterval = 1440 * time.Minute //one day
	defaultCleanupInterval      = 1441 * time.Minute
	defaultMaxUpdateBuffer      = 100
	defaultUpdateLockInterval   = time.Minute
)

type (
	levelCache struct {
		c       *cache.Cache
		rdb     *redis.Client
		loaders map[string]DataLoader
		cfg     CacheConfig
		version map[string]int64
		updates chan versionInfo
		stop    chan struct{}
		locker  *redislock.Client
	}

	CacheConfig struct {
		RedisAddr       string
		RedisDb         int
		RedisPassword   string
		RedisPoolSize   int
		CacheExpiration time.Duration
		CleanupInterval time.Duration
		LockInterval    time.Duration
		MaxUpdateBuffer int
	}

	versionInfo struct {
		dataKey   string
		versionNo int64
	}
)

func (p *CacheConfig) checkAndLoadDefault() error {
	if p.RedisAddr == "" {
		return fmt.Errorf("invalid redis connect addr")
	}
	if p.RedisPoolSize == 0 {
		p.RedisPoolSize = 40
	}
	if p.CacheExpiration == 0 {
		p.CacheExpiration = defaultCacheInvalidInterval
	}
	if p.CleanupInterval == 0 {
		p.CleanupInterval = defaultCleanupInterval
	}
	if p.LockInterval == 0 {
		p.LockInterval = defaultUpdateLockInterval
	}
	if p.MaxUpdateBuffer == 0 {
		p.MaxUpdateBuffer = defaultMaxUpdateBuffer
	}
	return nil
}

func New(cfg CacheConfig) (*levelCache, error) {
	if err := cfg.checkAndLoadDefault(); err != nil {
		return nil, err
	}
	lc := &levelCache{
		c:       cache.New(cfg.CacheExpiration, cfg.CleanupInterval),
		loaders: make(map[string]DataLoader),
		cfg:     cfg,
		version: make(map[string]int64),
		updates: make(chan versionInfo, cfg.MaxUpdateBuffer),
		stop:    make(chan struct{}, 1),
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDb,
	})
	if err := rdb.Ping(context.TODO()).Err(); err != nil {
		return nil, err
	}
	lc.rdb = rdb
	lc.locker = redislock.New(rdb)
	return lc, nil
}

func (p *levelCache) RegisterLoader(namespace string, loader DataLoader) error {
	if _, ok := p.loaders[namespace]; ok {
		return fmt.Errorf("data loader [%s] existed", namespace)
	}
	p.loaders[namespace] = loader
	return nil
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
			case update := <-p.updates:
				_ = p.parseAndDo(ctx, update)
			case <-p.stop:
				close(p.stop)
				close(p.updates)
				return
			}
		}
	}()
}

func (p *levelCache) Stop() {
	p.stop <- struct{}{}
}

func (p *levelCache) Get(ctx context.Context, key string, obj Cacheable) error {
	p.checkCacheUpdate(ctx, obj.Namespace(), key)
	return p.get(ctx, key, obj)
}

func (p *levelCache) get(ctx context.Context, key string, obj Cacheable) error {
	k := jointKey(obj.Namespace(), key)
	// read local cache
	if content, ok := p.c.Get(k); ok {
		if err := jsoniter.UnmarshalFromString(content.(string), obj); err != nil {
			return err
		}
		return nil
	}

	// read redis cache
	content, err := p.rdb.Get(ctx, k).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	if content != "" {
		if err := jsoniter.UnmarshalFromString(content, obj); err != nil {
			return err
		}
		p.c.SetDefault(k, toJson(obj))
	}

	loader, exist := p.loaders[obj.Namespace()]
	if !exist {
		return fmt.Errorf("data loader [%s] not found", obj.Namespace())
	}
	data, err := loader(ctx, key)
	if err != nil {
		return err
	}
	if err := copier.Copy(obj, data); err != nil {
		return err
	}
	p.rdb.Set(ctx, k, toJson(obj), p.cfg.CacheExpiration)
	p.c.SetDefault(k, toJson(obj))
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
		p.updates <- versionInfo{
			dataKey:   k,
			versionNo: latest,
		}
	}
}

func (p *levelCache) parseAndDo(ctx context.Context, info versionInfo) error {
	content, err := p.rdb.Get(ctx, info.dataKey).Result()
	if err != nil {
		return err
	}
	p.version[info.dataKey] = info.versionNo
	p.c.SetDefault(info.dataKey, content)
	return nil
}

func (p *levelCache) Refresh(ctx context.Context, namespace, key string) {
	loader, exist := p.loaders[namespace]
	if !exist {
		return
	}
	go func() {
		k := jointKey(namespace, key)
		lockKey := jointKey("lock", k)
		vk := jointKey("version", k)
		for {
			lock, err := p.locker.Obtain(ctx, lockKey, p.cfg.LockInterval, nil)
			if err != nil {
				time.Sleep(time.Millisecond)
				continue
			}
			data, err := loader(ctx, key)
			if err != nil {
				return
			}
			p.c.SetDefault(k, toJson(data))
			p.rdb.Set(ctx, k, toJson(data), p.cfg.CacheExpiration)

			if recNo, err := p.rdb.Incr(ctx, vk).Result(); err == nil {
				p.version[k] = recNo
			}
			_ = lock.Release(ctx)
			break
		}
	}()
}

func jointKey(a ...string) string {
	return strings.Join(a, cacheKeyJoint)
}

func toJson(obj interface{}) string {
	content, err := jsoniter.MarshalToString(obj)
	if err != nil {
		return ""
	}
	return content
}
