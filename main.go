package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/nats-io/go-nats"
	snats "github.com/nats-io/go-nats-streaming"
	loggy "github.com/sirupsen/logrus"
	"github.com/teliax/streaming-replication-ex/constant"
	"github.com/teliax/streaming-replication-ex/models"
	"github.com/teliax/streaming-replication-ex/wal"
)

var streamingConn snats.Conn
var DB *gorm.DB
var dberr error

func init() {
	setupLoggy() // setup Loggy
	setupBrake() // setup airbrake
	setupDB()    // setupDB
}

func setupLoggy() {
	loggy.SetOutput(os.Stdout)
	loggy.SetLevel(loggy.InfoLevel)
}

//
func setupBrake() {

}

func setupDB() {
	DB, dberr = gorm.Open("postgres", "host=127.0.0.1 port=5432 user=admin dbname=pg_test sslmode=disable")
	if dberr != nil {
		panic("failed to connect database")
	}
	models.Setup(DB)
}

func main() {
	defer DB.Close()
	natsConn, err := nats.Connect(constant.NatURL)
	if err != nil {
		log.Fatal(err)
	}

	streamingConn, err = snats.Connect("test-cluster", constant.ConsumerID, snats.NatsConn(natsConn), snats.SetConnectionLostHandler(func(_ snats.Conn, reason error) {
		log.Fatalf("Connection lost, reason: %v", reason)
	}))
	if err != nil {
		panic(err)
	}
	fmt.Println("connecting to nats... ")
	defer func() {
		streamingConn.Close()
	}()

	_, err = streamingConn.Subscribe(constant.Subject,
		filterMessage,
		snats.DurableName(constant.DurableName),
		snats.StartAtSequence(0),
		snats.MaxInflight(1),
		snats.SetManualAckMode(),
	)

	if err != nil {
		log.Fatal(err)
	}
	select {}
}

// type walMessage struct {
// 	Kind      string        `json:"kind"`
// 	Schema    string        `json:"schema"`
// 	Table     string        `json:"table"`
// 	Names     []string      `json:"columnnames"`
// 	Types     []string      `json:"columntypes"`
// 	Values    []interface{} `json:"columnvalues"`
// 	Optionals []bool        `json:"columnoptionals"`
// }

// {"nextlsn":"0/16CC638","change":[{"kind":"insert","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","character varying"],"columnoptionals":[false,true],"columnvalues":[1,"werain"]}]}
// {"nextlsn":"0/16CEC20","change":[{"kind":"update","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","character varying"],"columnoptionals":[false,true],"columnvalues":[1,"Werain"],"oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[1]}},{"kind":"update","schema":"public","table":"users","columnnames":["id","name"],"columntypes":["integer","character varying"],"columnoptionals":[false,true],"columnvalues":[2,"Werain"],"oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[2]}}]}
// {"nextlsn":"0/16CEFB8","change":[{"kind":"delete","schema":"public","table":"users","oldkeys":{"keynames":["id"],"keytypes":["integer"],"keyvalues":[3]}}]}

type rawWal struct {
	NextLsn string        `json:"nextlsn"`
	Changes []wal.Message `json:"change"`
}

func filterMessage(message *snats.Msg) {
	var walData wal.Message

	err := json.Unmarshal(message.Data, &walData)
	if err != nil {
		panic(err)
	}

	// do a lookup of table
	model := models.Find(walData.Table)
	switch walData.Kind {
	case "insert":
		// perform insert
		model.Insert(walData)
	case "update":
		// perform update
		model.Update(walData)
	case "delete":
		// perform delete
		model.Delete(walData)
	default:
		fmt.Println("we should not get here")
	}

	// for _, walData := range orgWal.Changes {
	// 	model := models.Find(walData.Table)

	// 	switch walData.Kind {
	// 	case "insert":
	// 		model.Insert(walData)
	// 	case "update":
	// 		model.Update(walData)
	// 	case "delete":
	// 		model.Delete(walData)
	// 	default:
	// 		fmt.Println("we should not get here")
	// 	}
	// }

}
