package models

import (
	"encoding/json"
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/teliax/streaming-replication-ex/constant"
	"github.com/teliax/streaming-replication-ex/wal"
)

type User struct {
	gorm.Model
	Name  string `json:"name"`
	Age   uint   `json:"age"`
	PgxID uint   `json:"pgx_id"`
}

func (u User) TableName() string {
	return "users"
}

func (u User) Insert(data wal.Message) bool {
	if data.Table != u.TableName() {
		panic("Not a model you are looking for")
	}

	attrs := filterAttribute(u, data)
	u._assign(attrs)
	DB.Create(&u)
	return true

}

func (u User) Update(data wal.Message) bool {
	key := fmt.Sprintf("%s = ?", constant.QueryKey)
	value := u.valueFor(constant.QueryKey, data)

	if value == nil {
		panic("Got a nil value")
	}
	DB.First(&u, key, value)
	DB.Model(u).Omit(u.omitFields()...).Updates(filterAttribute(u, data))
	return true
}

func (u User) Delete(data wal.Message) bool {
	key := fmt.Sprintf("%s = ?", constant.QueryKey)
	value := u.valueFor(constant.QueryKey, data)
	if value == nil {
		panic("Got a nil value")
	}
	DB.First(&u, key, value)
	DB.Unscoped().Delete(&u)
	return true
}

func (u User) omitFields() []string {
	return []string{"pgx_id", "updated_at", "created_at", "deleted_at", "age"}
}

func (u User) fields() map[string]string {
	fields := map[string]string{"id": "pgx_id", "name": "name", "age": "age"}
	return fields
}

func (u *User) _assign(attrs map[string]interface{}) {
	_bytes, err := json.Marshal(attrs)
	if err != nil {
		panic(err)
	}
	json.Unmarshal(_bytes, &u)
}

// {"nextlsn":"0/16CEC20","change":[{"kind":"update","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","character varying"],"columnoptionals":[false,true],"columnvalues":[1,"Werain"],"oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[1]}},{"kind":"update","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","character varying"],"columnoptionals":[false,true],"columnvalues":[2,"Werain"],"oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[2]}}]}

// {"nextlsn":"0/16CEFB8","change":[{"kind":"delete","schema":"public","table":"users","oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[3]}}]}
// {"nextlsn":"0/235BCD8","change":[{"kind":"delete","schema":"public","table":"users","oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[2]}}]}
func (u User) valueFor(key string, data wal.Message) interface{} {
	values := data.GetValues()
	fields := u.fields()
	for idx, name := range data.GetNames() {
		if cName, _ := fields[name]; cName == key {
			return values[idx]
		}
		continue
	}

	return nil
}
