package serde

import (
	"testing"
)

func TestStringSerde(t *testing.T) {
	serde := StringSerde{}
	s := "hi"
	b, err := serde.ToBytes(s)
	if err != nil {
		t.Error(err)
	}
	s2, err := serde.FromBytes(b)
	if err != nil {
		t.Error(err)
	}
	if s != s2 {
		t.Errorf(`Unmarshal = %v; want %v`, s2, s)
	}
}