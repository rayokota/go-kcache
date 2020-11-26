package encode

import (
	"encoding"
	"fmt"
	"strconv"
	"time"
)

// Marshal parses `v` to bytes `b` with appropriate type.
func Marshal(v interface{}, b *[]byte) error {
	switch v := v.(type) {
	case nil:
		*b = append(*b, StringToBytes("")...)
		return nil
	case string:
		*b = append(*b, StringToBytes(v)...)
		return nil
	case []byte:
		*b = append(*b, v...)
		return nil
	case int:
		*b = append(*b, strconv.AppendInt(*b, int64(v), 10)...)
		return nil
	case int8:
		*b = append(*b, strconv.AppendInt(*b, int64(v), 10)...)
		return nil
	case int16:
		*b = append(*b, strconv.AppendInt(*b, int64(v), 10)...)
		return nil
	case int32:
		*b = append(*b, strconv.AppendInt(*b, int64(v), 10)...)
		return nil
	case int64:
		*b = append(*b, strconv.AppendInt(*b, v, 10)...)
		return nil
	case uint:
		*b = append(*b, strconv.AppendUint(*b, uint64(v), 10)...)
		return nil
	case uint8:
		*b = append(*b, strconv.AppendUint(*b, uint64(v), 10)...)
		return nil
	case uint16:
		*b = append(*b, strconv.AppendUint(*b, uint64(v), 10)...)
		return nil
	case uint32:
		*b = append(*b, strconv.AppendUint(*b, uint64(v), 10)...)
		return nil
	case uint64:
		*b = append(*b, strconv.AppendUint(*b, v, 10)...)
		return nil
	case float32:
		*b = append(*b, strconv.AppendFloat(*b, float64(v), 'f', -1, 64)...)
		return nil
	case float64:
		*b = append(*b, strconv.AppendFloat(*b, v, 'f', -1, 64)...)
		return nil
	case bool:
		if v {
			*b = append(*b, strconv.AppendInt(*b, 1, 10)...)
			return nil
		}
		*b = append(*b, strconv.AppendInt(*b, 0, 10)...)
		return nil
	case time.Time:
		*b = v.AppendFormat(*b, time.RFC3339Nano)
		return nil
	case encoding.BinaryMarshaler:
		bytes, err := v.MarshalBinary()
		if err != nil {
			return err
		}
		*b = append(*b, bytes...)
		return nil
	default:
		return fmt.Errorf(
			"can't marshal %T (implement encoding.BinaryMarshaler)", v)
	}
}
