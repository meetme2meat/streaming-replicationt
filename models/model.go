package models

import (
	"github.com/jinzhu/gorm"
	"github.com/teliax/streaming-replication-ex/wal"
)

type SQLTable interface {
	TableName() string
	Insert(data wal.Message) bool
	Update(data wal.Message) bool
	Delete(data wal.Message) bool
	fields() map[string]string
}

var SQLModels []SQLTable
var DB *gorm.DB
var dberr error

func init() {
	SQLModels = []SQLTable{
		User{},
		Tempo{},
		Product{},
	}
}

func Setup(db *gorm.DB) {
	DB = db
	DB.AutoMigrate(&Tempo{})
	DB.AutoMigrate(&User{})
	DB.AutoMigrate(&Product{})
}

func Find(table string) SQLTable {
	for _, model := range SQLModels {
		if model.TableName() == table {
			// TODO need to check if GC is good with this
			// create a clone so that we know stepping onto other object.
			return clone(model)
		}
	}
	panic("could not find the model")
}

func clone(model SQLTable) SQLTable {
	switch model.(type) {
	case User:
		return User{}
	case Tempo:
		return Tempo{}
	case Product:
		return Product{}
	default:
		panic("Received unknown type")
	}
}

func filterAttribute(r SQLTable, data wal.Message) map[string]interface{} {
	attrs := make(map[string]interface{}, 0)
	_fields := r.fields()
	values := data.GetValues()
	for idx, name := range data.GetNames() {
		if _, ok := _fields[name]; !ok {
			continue
		}
		key := _fields[name]
		attrs[key] = values[idx]
	}
	return attrs
}
