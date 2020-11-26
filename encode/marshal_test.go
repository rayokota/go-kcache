package encode

import (
	"testing"
	"time"
)

func TestMarshalString(t *testing.T) {
	var b []byte
	err := Marshal("hi", &b)
	if err != nil {
		t.Error(err)
	}
	var s string
	err = Unmarshal(b, &s)
	if err != nil {
		t.Error(err)
	}
	if s != "hi" {
		t.Errorf(`Unmarshal = %q; want "hi"`, s)
	}
}

func TestMarshalTime(t *testing.T) {
	date := time.Unix(1000, 0)
	var b []byte
	err := Marshal(date, &b)
	if err != nil {
		t.Error(err)
	}
	var date2 time.Time
	err = Unmarshal(b, &date2)
	if err != nil {
		t.Error(err)
	}
	if date != date2 {
		t.Errorf(`Unmarshal = %v; want %v`, date2, date)
	}
}
