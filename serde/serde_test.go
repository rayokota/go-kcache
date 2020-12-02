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

type TestStruct struct {
	Field1 string `json:"field1"`
	Field2 int32 `json:"field2"`
}

type TestStructSerde struct {}

func (serde TestStructSerde) ToBytes(v interface{}) ([]byte, error) {
	var b []byte
	err := Marshal(v, &b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (serde TestStructSerde) FromBytes(bytes []byte) (interface{}, error) {
	var s TestStruct
	err := Unmarshal(bytes, &s)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func TestJSONSerde(t *testing.T) {
	serde := TestStructSerde{}
	s := TestStruct {
		Field1: "value",
		Field2: 2,
	}
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
