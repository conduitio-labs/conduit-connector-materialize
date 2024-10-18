// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package materialize

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/conduitio-labs/conduit-connector-materialize/config"
	"github.com/conduitio-labs/conduit-connector-materialize/test"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/doug-martin/goqu/v9"
	"github.com/jackc/pgx/v4"
)

var (
	testConn  *pgx.Conn
	dsn       = "postgres://materialize@localhost:6875/materialize?sslmode=disable"
	testTable = "acceptance_test_users"
)

func TestMain(m *testing.M) {
	os.Exit(testMainWrapper(m))
}

func testMainWrapper(m *testing.M) int {
	var err error
	testConn, err = test.SetupTestConnection(dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to setup test connection: %s", err.Error())

		return 1
	}
	defer testConn.Close(context.Background())

	if err = test.MigrateTestDB(context.Background(), testConn, testTable); err != nil {
		fmt.Fprintf(os.Stderr, "failed to migrate test db: %s", err.Error())

		return 1
	}

	return m.Run()
}

type AcceptanceTestDriver struct {
	sdk.ConfigurableAcceptanceTestDriver
}

func TestAcceptance(t *testing.T) {
	sdk.AcceptanceTest(t, AcceptanceTestDriver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:    Connector,
				SourceConfig: nil,
				DestinationConfig: map[string]string{
					config.KeyURL:   dsn,
					config.KeyTable: testTable,
					config.KeyKey:   "id",
				},
			},
		},
	},
	)
}

func (d AcceptanceTestDriver) ReadFromDestination(t *testing.T, records []opencdc.Record) []opencdc.Record {
	type key struct {
		ID int `json:"id"`
	}

	keys := make([]int, len(records))
	for i, record := range records {
		var k key
		if err := json.Unmarshal(record.Key.Bytes(), &k); err != nil {
			t.Fatalf("decode key: %v", err)

			return nil
		}

		keys[i] = k.ID
	}

	var outRecords []opencdc.Record
	for _, key := range keys {
		// make select one by one in order to keep the original order
		sql, _, err := goqu.
			Select("id", "name").
			From(testTable).
			Where(goqu.Ex{
				"id": key,
			}).ToSQL()
		if err != nil {
			t.Fatalf("build a SQL query: %v", err)

			return nil
		}

		row := testConn.QueryRow(context.Background(), sql)

		var id, name = 0, ""
		if err := row.Scan(&id, &name); err != nil {
			t.Fatalf("scan row: %v", err)

			return nil
		}

		outRecords = append(outRecords, opencdc.Record{
			Metadata:  nil,
			Operation: opencdc.OperationCreate,
			Key: opencdc.StructuredData(map[string]any{
				"id": int32(id),
			}),
			Payload: opencdc.Change{
				After: opencdc.StructuredData(map[string]any{
					"id":   int32(id),
					"name": name,
				}),
			},
		})
	}

	return outRecords
}

func (d AcceptanceTestDriver) GenerateRecord(_ *testing.T, _ opencdc.Operation) opencdc.Record {
	id := gofakeit.Int32()

	position := make([]byte, binary.MaxVarintLen64)
	_ = binary.PutVarint(position, int64(id))

	metatada := make(opencdc.Metadata)
	metatada.SetCreatedAt(gofakeit.Date())

	return opencdc.Record{
		Position:  opencdc.Position(position),
		Operation: opencdc.OperationCreate,
		Metadata:  metatada,
		Key: opencdc.StructuredData(map[string]any{
			"id": id,
		}),
		Payload: opencdc.Change{
			After: opencdc.StructuredData(map[string]any{
				"id":   id,
				"name": gofakeit.FirstName(),
			}),
		},
	}
}
