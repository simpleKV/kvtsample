//go:build boltdb
// +build boltdb

package main

import (
	"log"
	"unsafe"

	"github.com/simpleKV/kvt"

	bolt "go.etcd.io/bbolt"
)

var (
	kvOrder *kvt.KVT
	bdb     *bolt.DB
)

func initOrder(create bool) {
	kvOrder = makeKVT()
	var err error
	bdb, err = bolt.Open("kvt_test.bdb", 0600, nil)
	if err != nil {
		return
	}

	if create {
		//here create the Order/index buckets, you only need run it for the first time
		bdb.Update(func(tx *bolt.Tx) error {
			p, _ := kvt.NewPoler(tx)
			kvOrder.CreateDataBucket(p)   //create Order bucket
			kvOrder.SetSequence(p, 1000)  //init sequence
			kvOrder.CreateIndexBuckets(p) //create index bucket: idx_Type
			return nil
		})
	}

}

// Create or Update
func Save(order Order) {
	//do some updates
	//you MUST NEVER NEVER NEVER update/modify the ID of the order

	bdb.Update(func(tx *bolt.Tx) error {
		p, _ := kvt.NewPoler(tx)
		if order.ID == 0 {
			id, _ := kvOrder.NextSequence(p)
			order.ID = id //fill the ID for creation case, but you should not overwritten it
		}
		log.Println("save Order: ", order)
		kvOrder.Put(p, &order)
		return nil
	})
}

func Delete(order Order) {
	bdb.Update(func(tx *bolt.Tx) error {
		p, _ := kvt.NewPoler(tx)
		kvOrder.Delete(p, &order)
		return nil
	})
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
	bdb.View(func(tx *bolt.Tx) error {
		p, _ := kvt.NewPoler(tx)
		list, _ := kvOrder.RangeQuery(p, rqi)
		for i := range list {
			orders = append(orders, list[i].(*Order))
		}
		return nil
	})

	return orders
}
