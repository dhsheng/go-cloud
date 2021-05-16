package alitablestore

import (
	"fmt"
	"reflect"

	"gocloud.dev/docstore/driver"
)


type decoder struct {
	val interface{}
}

func (d decoder) String() string {
	return fmt.Sprint(d.val)
}

func (d decoder) AsNull() bool {
	return d.val == nil
}

func (d decoder) AsBool() (bool, bool) {
	b, ok := d.val.(bool)
	return b, ok
}

func (d decoder) AsString() (string, bool) {
	s, ok := d.val.(string)
	return s, ok
}

func (d decoder) AsInt() (int64, bool) {
	i, ok := d.val.(int64)
	return i, ok
}

func (d decoder) AsUint() (uint64, bool) {
	i, ok := d.val.(int64)
	return uint64(i), ok
}

func (d decoder) AsFloat() (float64, bool) {
	f, ok := d.val.(float64)
	return f, ok
}

func (d decoder) AsBytes() ([]byte, bool) {
	bs, ok := d.val.([]byte)
	return bs, ok
}

func (d decoder) AsInterface() (interface{}, error) {
	return d.val, nil
}

func (d decoder) ListLen() (int, bool) {
	if s, ok := d.val.([]interface{}); ok {
		return len(s), true
	}
	return 0, false
}

func (d decoder) DecodeList(f func(i int, d2 driver.Decoder) bool) {
	for i, e := range d.val.([]interface{}) {
		if !f(i, decoder{e}) {
			return
		}
	}
}

func (d decoder) MapLen() (int, bool) {
	if m, ok := d.val.(map[string]interface{}); ok {
		return len(m), true
	}
	return 0, false
}

func (d decoder) DecodeMap(f func(key string, d2 driver.Decoder, _ bool) bool) {
	for k, v := range d.val.(map[string]interface{}) {
		if !f(k, decoder{v}, true) {
			return
		}
	}
}

func (d decoder) AsSpecial(v reflect.Value) (bool, interface{}, error) {
	return false, nil, nil
}
