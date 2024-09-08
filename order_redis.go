//go:build redis
// +build redis

package main

import (
	"context"
	"log"
	"unsafe"

	"github.com/redis/go-redis/v9"
	"github.com/simpleKV/kvt"
)

var (
	kvOrder *kvt.KVT
	bdb     *redis.Client
	ctx     = context.Background()
)

func initOrder(create bool) {
	kvOrder = makeKVT()
	bdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	if create {
		//here create the Order/index buckets, you only need run it for the first time
		bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			p := kvt.NewRedisPoler(bdb, pipe, ctx)
			kvOrder.CreateDataBucket(p)
			kvOrder.SetSequence(p, 1000)
			kvOrder.CreateIndexBuckets(p)
			return nil
		})
	}
}

// Create or Update
func Save(order Order) {
	//do some updates
	//you MUST NEVER NEVER NEVER update/modify the ID of the order

	_, err := bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		p := kvt.NewRedisPoler(bdb, pipe, ctx)
		if order.ID == 0 {
			id, _ := kvOrder.NextSequence(p)
			order.ID = id //fill the ID for creation case, but you should not overwritten it
		}
		log.Println("save Order: ", order)
		kvOrder.Put(p, &order)
		return nil
	})
	if err != nil {
		log.Println(err)
		//blala...
	}
}

func Delete(order Order) {
	_, err := bdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		p := kvt.NewRedisPoler(bdb, pipe, ctx)
		kvOrder.Delete(p, &order)
		return nil
	})
	if err != nil {
		//maybe you do blala..
		log.Println(err)
	}
}

func Query(typ string, begin, end uint16) (orders []*Order) {

	//where order.Type="fruit" and 2 <= order.Status and order.Status < 4
	rqi := kvt.RangeInfo{
		IndexName: "idx_Type_Status",
		Where: map[string]map[string][]byte{
			"Type": map[string][]byte{
				"=": []byte(typ),
			},
			"Status": map[string][]byte{
				">=": kvt.Bytes(kvt.Ptr(&begin), unsafe.Sizeof(begin)),
				"<":  kvt.Bytes(kvt.Ptr(&end), unsafe.Sizeof(end)),
			},
		},
	}
	p := kvt.NewRedisPoler(bdb, nil, ctx)
	list, _ := kvOrder.RangeQuery(p, rqi)
	for i := range list {
		orders = append(orders, list[i].(*Order))
	}

	return orders
}
