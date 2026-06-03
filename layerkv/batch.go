package layerkv

import (
	"context"
	"errors"
	"maps"

	kv "github.com/chenyanchen/kv"
)

type batch[K comparable, V any] struct {
	cache kv.BatchKV[K, V]
	store kv.BatchKV[K, V]
}

func NewBatch[K comparable, V any](cache, store kv.BatchKV[K, V]) (*batch[K, V], error) {
	if cache == nil {
		return nil, errors.New("cache is nil")
	}
	if store == nil {
		return nil, errors.New("store is nil")
	}
	return &batch[K, V]{
		cache: cache,
		store: store,
	}, nil
}

func (l batch[K, V]) Get(ctx context.Context, keys []K) (map[K]V, error) {
	cached, err := l.cache.Get(ctx, keys)
	if err != nil {
		return nil, err
	}

	if len(cached) == len(keys) {
		return cached, nil
	}

	miss := make([]K, 0, len(keys)-len(cached))
	for _, key := range keys {
		if _, ok := cached[key]; !ok {
			miss = append(miss, key)
		}
	}

	stored, err := l.store.Get(ctx, miss)
	if err != nil {
		return nil, err
	}

	maps.Copy(cached, stored)

	return cached, l.cache.Set(ctx, stored)
}

func (l batch[K, V]) Set(ctx context.Context, kvs map[K]V) error {
	if err := l.store.Set(ctx, kvs); err != nil {
		return err
	}

	// Direct key extraction avoids allocations from maps.Keys + slices.Collect
	keys := make([]K, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	return l.cache.Del(ctx, keys)
}

func (l batch[K, V]) Del(ctx context.Context, keys []K) error {
	if err := l.store.Del(ctx, keys); err != nil {
		return err
	}

	return l.cache.Del(ctx, keys)
}
