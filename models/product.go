package models

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/teliax/streaming-replication-ex/constant"
	"github.com/teliax/streaming-replication-ex/wal"
)

type Product struct {
	gorm.Model
	Code        string `gorm:"type:varchar(10)"`
	Price       uint
	PurchasesAt time.Time `gorm:"type: timestamp without time zone" json:"purchased_at"`
	ReadAt      time.Time `gorm:"type: timestamp without time zone" json:"read_at"`
	PgxID       uint      `json:"pgx_id"`
}

func (p Product) TableName() string {
	return "products"
}

// {"nextlsn":"0/16CC638","change":[{"kind":"insert","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","character varying"],"columnoptionals":[false,true],"columnvalues":[1,"werain"]}]}

// func (record Tempo) BeforeCreate(scope *gorm.Scope) error {
// 	scope.SetColumn("", record.ID)
// 	return nil
// }

// func (p Tempo) QueryKey() string {
// 	return constant.QueryKey
// }

func (p Product) Insert(data wal.Message) bool {
	if data.Table != p.TableName() {
		panic("Not a model you are looking for")
	}

	attrs := filterAttribute(p, data)
	p._assign(attrs)

	DB.Create(&p)
	return true
}

// {"nextlsn":"0/16CEC20","change":[{"kind":"update","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","character varying"],"columnoptionals":[false,true],"columnvalues":[1,"Werain"],"oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[1]}},{"kind":"update","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","character varying"],"columnoptionals":[false,true],"columnvalues":[2,"Werain"],"oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[2]}}]}

func (p Product) Update(data wal.Message) bool {
	key := fmt.Sprintf("%s = ?", constant.QueryKey)
	value := p.valueFor(constant.QueryKey, data)
	if value == nil {
		panic("Got a nil value")
	}
	DB.First(&p, key, value)
	//db.Query("").First(&p)
	//fieldValues := fieldsValue(p)
	//
	// db.Save(p)
	DB.Model(p).Omit(p.omitFields()...).Updates(filterAttribute(p, data))
	return true
}

func (p Product) omitFields() []string {
	return []string{"pgx_id", "updated_at", "created_at", "deleted_at", "brand", "year"}
}

func (p Product) valueFor(key string, data wal.Message) interface{} {
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

func (p Product) Delete(data wal.Message) bool {
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
func (p Product) fields() map[string]string {
	fields := map[string]string{"id": "pgx_id", "code": "code", "price": "price", "purchased_at": "purchased_at", "read_at": "read_at"}
	return fields
}

func (p Product) types() []string {
	types := []string{"integer", "character varying"}
	return types
}

func (p *Product) _assign(attrs map[string]interface{}) {
	_bytes, err := json.Marshal(attrs)
	if err != nil {
		panic(err)
	}
	json.Unmarshal(_bytes, &p)
}
