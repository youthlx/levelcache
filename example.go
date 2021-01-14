package levelcache

import (
	"context"
	"fmt"
	"strconv"
)

type Dish struct {
	ID      int     `json:"id"`
	Name    string  `json:"name"`
	Taste   int     `json:"taste"`
	Price   float64 `json:"price"`
	Comment string  `json:"comment"`
}

func (p *Dish) Namespace() string {
	return "dish"
}

func (p *Dish) Key() string {
	return strconv.Itoa(p.ID)
}

func GetDish(ctx context.Context, key string) (Cacheable, error) {
	switch key {
	case "1":
		return &Dish{
			ID:      1,
			Name:    "GongBaoJiDing",
			Taste:   1,
			Price:   40,
			Comment: "awesome",
		}, nil
	case "2":
		return &Dish{
			ID:      2,
			Name:    "GongBaoJiDing",
			Taste:   0,
			Price:   100,
			Comment: "excellent",
		}, nil
	default:
		return nil, fmt.Errorf("dish [%s] not found", key)
	}
}
