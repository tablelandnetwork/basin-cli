package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
)

func main() {
	replicator, err := pgrepl.New(os.Getenv("CONNSTR"), pgrepl.Publication(os.Getenv("TABLE")))
	if err != nil {
		log.Fatal(err)
	}

	txs, _, err := replicator.StartReplication(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	for tx := range txs {
		bytes, _ := json.MarshalIndent(tx, "", "    ")
		fmt.Println(string(bytes))

		replicator.Commit(context.Background(), tx.CommitLSN)
	}
}
