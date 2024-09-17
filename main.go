package main

import "fmt"

func main() {

	orders := []Order{
		Order{
			Type:   "book",
			Status: 1,
			Name:   "Alice",
			Num:    5,
		},
		Order{
			Type:   "fruit",
			Status: 2,
			Name:   "Bob",
			Num:    4,
		},
		Order{
			Type:   "fruit",
			Status: 3,
			Name:   "Carl",
			Num:    13,
		},
		Order{
			Type:   "book",
			Status: 4,
			Name:   "Dicken",
			Num:    3,
		},
	}
	initOrder(true)

	for i := range orders {
		Save(orders[i])
	}

	r := Query("fruit", 2, 4)

	for i := range r {
		fmt.Println(r[i])
	}

	r[0].Status = 8
	r[0].Num = 3 + 2

	//update r[0]
	Save(*r[0])

	r = Query("fruit", 0, 9999)
	for i := range r {
		fmt.Println(r[i])
	}

	fmt.Println("begin query by time")
	r = QueryByTime(2010, 2012)
	for i := range r {
		fmt.Println(r[i])
	}
}
