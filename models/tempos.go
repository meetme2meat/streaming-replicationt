package models

import (
	"encoding/json"
	"fmt"

	"github.com/jinzhu/gorm"
	"github.com/teliax/streaming-replication-ex/constant"
	"github.com/teliax/streaming-replication-ex/wal"
)

type Tempo struct {
	gorm.Model
	Name  string `json:"name"`
	Brand string `json:"brand"`
	Year  uint   `json:"year"`
	PgxID uint   `json:"pgx_id"`
}

func (p Tempo) TableName() string {
	return "tempos"
}

// {"nextlsn":"0/16CC638","change":[{"kind":"insert","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","character varying"],"columnoptionals":[false,true],"columnvalues":[1,"werain"]}]}

// func (record Tempo) BeforeCreate(scope *gorm.Scope) error {
// 	scope.SetColumn("", record.ID)
// 	return nil
// }

// func (p Tempo) QueryKey() string {
// 	return constant.QueryKey
// }

func (p Tempo) Insert(data wal.Message) bool {
	if data.Table != p.TableName() {
		panic("Not a model  you are looking for")
	}

	attrs := filterAttribute(p, data)
	p._assign(attrs)

	DB.Create(&p)
	return true
}

// {"nextlsn":"0/16CEC20","change":[{"kind":"update","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","character varying"],"columnoptionals":[false,true],"columnvalues":[1,"Werain"],"oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[1]}},{"kind":"update","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","character varying"],"columnoptionals":[false,true],"columnvalues":[2,"Werain"],"oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[2]}}]}

func (p Tempo) Update(data wal.Message) bool {
	key := fmt.Sprintf("%s = ?", constant.QueryKey)
	value := p.valueFor(constant.QueryKey, data)
	if value == nil {
		panic("Got a nil value")
	}
	DB.First(&p, key, value)

	DB.Model(p).Omit(p.omitFields()...).Updates(filterAttribute(p, data))
	return true
}

func (p Tempo) omitFields() []string {
	return []string{"pgx_id", "updated_at", "created_at", "deleted_at", "brand", "year"}
}

func (p Tempo) valueFor(key string, data wal.Message) interface{} {
	values := data.GetValues()
	fields := p.fields()
	for idx, name := range data.GetNames() {
		if cName, _ := fields[name]; cName == key {
			return values[idx]
		}
		continue
	}
	return nil
}

func (p Tempo) Delete(data wal.Message) bool {
	key := fmt.Sprintf("%s = ?", constant.QueryKey)
	value := p.valueFor(constant.QueryKey, data)
	if value == nil {
		panic("Got a nil value")
	}
	DB.First(&p, key, value)
	DB.Unscoped().Delete(&p)
	return true
}

// we expect the fields and column_type field to be in same order
func (p Tempo) fields() map[string]string {
	fields := map[string]string{"id": "pgx_id", "name": "name", "brand": "brand", "year": "year"}
	return fields
}

func (p Tempo) types() []string {
	types := []string{"integer", "character varying"}
	return types
}

func (p *Tempo) _assign(attrs map[string]interface{}) {
	_bytes, err := json.Marshal(attrs)
	if err != nil {
		panic(err)
	}
	json.Unmarshal(_bytes, &p)
}
