package levelcache

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLevelCache_Get(t *testing.T) {
	cache, err := New(CacheConfig{
		RedisAddr:     "localhost:6379",
		RedisPoolSize: 10,
	})
	if err != nil {
		t.Errorf("init cache fail:%+v", err)
		return
	}
	_ = cache.RegisterLoader("dish", GetDish)
	cache.Start(context.Background())

	var hotDish Dish
	if err := cache.Get(context.TODO(), "1", &hotDish); err != nil {
		t.Errorf("get cache fail:%+v", err)
		return
	}
	t.Logf("hot dish:%+v", hotDish)
	assert.Equal(t, 1, hotDish.ID)

	var hotDishByCache Dish
	if err := cache.Get(context.TODO(), "1", &hotDishByCache); err != nil {
		t.Errorf("get cache for second fail:%+v", err)
		return
	}
	t.Logf("hot dish:%+v", hotDish)
	assert.Equal(t, 1, hotDish.ID)
}

func TestLevelCache_Refresh(t *testing.T) {
	cache, err := New(CacheConfig{
		RedisAddr:     "localhost:6379",
		RedisPoolSize: 10,
	})
	if err != nil {
		t.Errorf("init cache fail:%+v", err)
		return
	}
	_ = cache.RegisterLoader("dish", GetDish)
	cache.Start(context.Background())

	var dish Dish
	cache.Refresh(context.TODO(), dish.Namespace(), "1")

	if err := cache.Get(context.TODO(), "1", &dish); err != nil {
		t.Errorf("get cache for second fail:%+v", err)
		return
	}
	t.Logf("hot dish:%+v", dish)
}
