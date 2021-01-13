package levelcache

import "context"

type Cacheable interface {
	Namespace() string
	Key() string
}

type DataLoader func(ctx context.Context, key string) (Cacheable, error)
