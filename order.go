package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
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
// save  in the main bucket like that (pk(return by order_pk_ID),  value(return by order_valueEncode))
func order_pk_ID(obj interface{}) ([]byte, error) {
	c := obj.(*Order)
	return kvt.Bytes(kvt.Ptr(&c.ID), unsafe.Sizeof(c.ID)), nil
}

// a union index function: Type and Status
// this index function produce a key of idx bucket, and the value is primary key(order_pk_ID produce)
func order_idx_Type_Status(obj interface{}) ([]byte, error) {
	c, _ := obj.(*Order)
	ret := kvt.MakeIndexKey(nil, []byte(c.Type), kvt.Bytes(kvt.Ptr(&c.Status), unsafe.Sizeof(c.Status)))
	return ret, nil
}

// encode should match with decode, here we use gob, you can use json as you like
// generater []byte to save you obj into kv db
func order_valueEncode(obj interface{}) ([]byte, error) {
	var network bytes.Buffer // Stand-in for the network.
	// Create an encoder and send a value.
	enc := gob.NewEncoder(&network)
	c, _ := obj.(*Order)
	enc.Encode(c)
	return network.Bytes(), nil
}

// unmarshal []byte to your obj
func order_valueDecode(b []byte, obj any) (any, error) {
	r := bytes.NewReader(b)
	dec := gob.NewDecoder(r)
	var c *Order
	if obj != nil {
		c = obj.(*Order)
	} else {
		c = &Order{}
	}
	dec.Decode(c)
	log.Println("decode Order:", c)
	return c, nil
}

func makeKVT() *kvt.KVT {
	kp := kvt.KVTParam{
		Bucket:    "bkt_Order",
		Marshal:   order_valueEncode,
		Unmarshal: order_valueDecode,
		Indexs: []kvt.Index{
			//pk is primary key, it is necessary, you can't omit it, it support union fields too
			//you MUST NEVER NEVER NEVER update/modify the pk of a object
			{
				&kvt.IndexInfo{Name: "pk_ID"}, //here omit Fields:["ID"], will parse from idx name: "pk_ID"
				order_pk_ID,                   //why a prefix order ?  for pk_ID may conflict with another Type's pk in the package
			},
			//self define index, 3 key infos: index name,  index fields and index function
			//fields is optional, will parse from index name when omitted, fields should match with the struct field name
			//index has 3 types, primary key(prefix "pk_"), common index("idx_"), multi index with prefix "midx_"
			//index name prefis is one of (pk_, idx, midx_), is arbitrary if you supply index fields
			//index function name is arbitrary, match with index name is a better choice
			{
				&kvt.IndexInfo{
					Name: "idx_Type_Status",
					//Fields: []string{"Type", "Status"}, //Fields is optional, when omit, will parse from the index name
				},
				order_idx_Type_Status,
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
