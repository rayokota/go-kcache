package kcache

import (
	"fmt"
	"github.com/emirpasic/gods/utils"
	"github.com/rayokota/go-kcache/serde"
	"testing"
)

func TestKCache(t *testing.T) {
	s := serde.StringSerde{}
	c, err := New("localhost:9092", nil, s, s, utils.StringComparator)
	if err != nil {
		t.Error(err)
	}
	println("start init")
	c.Init()
	println("done init")

	c.Put("hi", "there")
	println("done putting")

	c.Put("hi2", "there")
	println("done putting")

	c.Put("bye", "where")
	println("done putting")

	v, _ := c.Get("hi")
	fmt.Printf("%v\n", v)

	v, _ = c.Get("bye")
	fmt.Printf("%v\n", v)

	v, _ = c.Delete("bye")
	v, _ = c.Get("bye")
	fmt.Printf("%v\n", v)
}