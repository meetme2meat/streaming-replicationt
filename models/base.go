package models

import "encoding/json"

type Base struct {
}

func (u *Base) _assign(attrs map[string]interface{}) {
	_bytes, err := json.Marshal(attrs)
	if err != nil {
		panic(err)
	}
	json.Unmarshal(_bytes, &u)
}
