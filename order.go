package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"unsafe"

	"github.com/simpleKV/kvt"
)

type Order struct {
	ID     uint64
	Type   string
	Status uint16
	Name   string
	Num    int
}

// produce a primary key(pk) from a Order object,
// save as Key in the main bucket
// save as Value in index bucket
func (this *Order) Key() ([]byte, error) {
	return kvt.Bytes(kvt.Ptr(&this.ID), unsafe.Sizeof(this.ID)), nil
}

// encode should match with decode, here we use gob, you can use json as you like
// generater []byte value to save you obj into kv db
func (this *Order) Value() ([]byte, error) {
	var network bytes.Buffer // Stand-in for the network.
	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)
	enc.Encode(this)
	return network.Bytes(), nil
}

// produce a primary key(pk) from a Order object,
// save  in the main bucket like that (pk(return by order_pk_ID),  value(return by order_valueEncode))
func (this *Order) Index(name string) ([]byte, error) {
	switch name {
	case "idx_Type_Status":
		return this.order_idx_Type_Status()
	}
	return nil, fmt.Errorf("Index not found")
}

// a union index function: Type and Status
// this index function produce a key of idx bucket, and the value is primary key(order_pk_ID produce)
func (this *Order) order_idx_Type_Status() ([]byte, error) {
	ret := kvt.MakeIndexKey(nil, []byte(this.Type), kvt.Bytes(kvt.Ptr(&this.Status), unsafe.Sizeof(this.Status)))
	return ret, nil
}

// unmarshal []byte to your obj
func orderUnmarshal(b []byte, obj kvt.KVer) (kvt.KVer, error) {
	r := bytes.NewReader(b)
	dec := gob.NewDecoder(r)

	c, ok := obj.(*Order)
	if !ok {
		c = new(Order)
	}
	if err := dec.Decode(c); err != nil {
		return nil, err
	}
	return c, nil
}

func makeKVT() *kvt.KVT {
	kp := kvt.KVTParam{
		Bucket:    "bkt_Order",
		Unmarshal: orderUnmarshal,
		Indexs: []kvt.IndexInfo{
			//self define index, 3 key infos: index name,  index fields and index function
			//index name can't omit, you can spec a full path as file path
			//index has 2 types, common index("idx_"), multi index with prefix "midx_"
			//index name is arbitrary if you supply index fields(except the prefix)
			//fields is optional, will parse from index name when omits, fields should match with the struct field name
			//index function name is arbitrary, match with index name is a better choice, it called by Index(idx_name)
			{
				Name: "bkt_Order/idx_Type_Status", //under bkt_Order, so it will not conflict with other idx
				//Name: "idx_Type_Status", //same with above, will extract fields: ["Type", "Status"]
				//Fields: []string{"Type", "Status"}, //Fields is optional, when omit, will parse from the index name

				//you can also spec path with fields as below, it will not depend on bkt_Order
				//Name: "[root/path/to/]idx_xyz"
				//Fields: []string{"Type", "Status"},
			},
		},
	}
	var err error
	kvOrder, err := kvt.New(Order{}, &kp)
	if err != nil {
		fmt.Printf("new kvt fail: %s", err)
		return nil
	}
	return kvOrder
}
