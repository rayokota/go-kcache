package serde

type Serde interface {
	Serializer
	Deserializer
}

type Serializer interface {
	ToBytes(v interface{}) ([]byte, error)
}

type Deserializer interface {
	FromBytes(bytes []byte) (interface{}, error)
}

type StringSerde struct{}

func (serde StringSerde) ToBytes(v interface{}) ([]byte, error) {
	var b []byte
	err := Marshal(v, &b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (serde StringSerde) FromBytes(bytes []byte) (interface{}, error) {
	var s string
	err := Unmarshal(bytes, &s)
	if err != nil {
		return nil, err
	}
	return s, nil
}
