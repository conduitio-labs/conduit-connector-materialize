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

package destination

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/conduitio-labs/conduit-connector-materialize/config"
	"github.com/conduitio-labs/conduit-connector-materialize/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v4"
)

var (
	conn      *pgx.Conn
	dsn       = "postgres://materialize@localhost:6875/materialize?sslmode=disable"
	testTable = "users"
)

func TestMain(m *testing.M) {
	os.Exit(testMainWrapper(m))
}

func testMainWrapper(m *testing.M) int {
	var err error
	conn, err = test.SetupTestConnection(dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to setup test connection: %s", err.Error())

		return 1
	}
	defer conn.Close(context.Background())

	if err = test.MigrateTestDB(context.Background(), conn, testTable); err != nil {
		fmt.Fprintf(os.Stderr, "failed to migrate test db: %s", err.Error())

		return 1
	}

	return m.Run()
}

func TestDestination_Configure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	destination := &Destination{}

	expectedConfiguration := config.Config{
		URL:   dsn,
		Table: "footable",
		Key:   "id",
	}

	err := destination.Configure(ctx, map[string]string{
		config.KeyURL:   expectedConfiguration.URL,
		config.KeyTable: expectedConfiguration.Table,
		config.KeyKey:   expectedConfiguration.Key,
	})
	if err != nil {
		t.Fatalf("failed to parse the Configuration: %v", err)
	}

	if !reflect.DeepEqual(destination.config, expectedConfiguration) {
		t.Fatalf("expected destination.Config to be %v, got %v", expectedConfiguration, destination.config)
	}
}

func TestDestination_Write(t *testing.T) {
	t.Parallel()

	if conn == nil {
		t.Skip()
	}

	type fields struct {
		UnimplementedDestination sdk.UnimplementedDestination
		conn                     *pgx.Conn
		config                   config.Config
	}
	type args struct {
		ctx    context.Context
		record sdk.Record
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "should insert",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL:   dsn,
					Table: "users",
					Key:   "id",
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position:  sdk.Position("999"),
					Operation: sdk.OperationCreate,
					Payload: sdk.Change{
						After: sdk.StructuredData{
							"id":   1,
							"name": "Anon",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "should insert, table within a metadata",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL: dsn,
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position:  sdk.Position("999"),
					Operation: sdk.OperationCreate,
					Metadata: map[string]string{
						metadataTable: "users",
					},
					Payload: sdk.Change{
						After: sdk.StructuredData{
							"id":   2,
							"name": "Anon",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "should insert, operation insert",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL:   dsn,
					Table: "users",
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position:  sdk.Position("999"),
					Operation: sdk.OperationCreate,
					Payload: sdk.Change{
						After: sdk.StructuredData{
							"id":   3,
							"name": "Anon",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "should insert, columns in UPPERCASE will be converted to lowercase",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL:   dsn,
					Table: "users",
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position:  sdk.Position("999"),
					Operation: sdk.OperationCreate,
					Payload: sdk.Change{
						After: sdk.StructuredData{
							"ID":   3,
							"NAME": "Anon",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "should return err, unknown operation",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL:   dsn,
					Table: "users",
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position: sdk.Position("999"),
					Payload: sdk.Change{
						After: sdk.StructuredData{
							"id":   4,
							"name": "Anon",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "should return err, empty table name",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL: dsn,
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position:  sdk.Position("999"),
					Operation: sdk.OperationCreate,
					Payload: sdk.Change{
						After: sdk.StructuredData{
							"id":   5,
							"name": "Anon",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "should return err, empty payload",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL:   dsn,
					Table: "users",
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position:  sdk.Position("999"),
					Operation: sdk.OperationCreate,
				},
			},
			wantErr: true,
		},
		{
			name: "should return err, invalid payload",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL:   dsn,
					Table: "users",
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position:  sdk.Position("999"),
					Operation: sdk.OperationCreate,
					Payload: sdk.Change{
						After: sdk.RawData([]byte("id:1,name:anon")),
					},
				},
			},
			wantErr: true,
		},
		{
			name: "should delete, operation delete",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL:   dsn,
					Table: "users",
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position:  sdk.Position("999"),
					Operation: sdk.OperationDelete,
					Key: sdk.StructuredData{
						"id": 3,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "should return err, operation delete, value for a key is not found",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL:   dsn,
					Table: "users",
					Key:   "id",
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position:  sdk.Position("999"),
					Operation: sdk.OperationDelete,
				},
			},
			wantErr: true,
		},
		{
			name: "should return err, unknown key, operation delete",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL:   dsn,
					Table: "users",
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position:  sdk.Position("999"),
					Operation: sdk.OperationDelete,
					Key: sdk.StructuredData{
						"unknown": 3,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "should update, operation update",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL:   dsn,
					Table: "users",
					Key:   "id",
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position:  sdk.Position("999"),
					Operation: sdk.OperationUpdate,
					Key: sdk.StructuredData{
						"id": 2,
					},
					Payload: sdk.Change{
						After: sdk.StructuredData{
							"name": "Alex",
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "should return error, empty payload",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL:   dsn,
					Table: "users",
					Key:   "id",
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position:  sdk.Position("999"),
					Operation: sdk.OperationUpdate,
					Key: sdk.StructuredData{
						"id": 4,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "should return error, unknown columns in UPPERCASE",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL:   dsn,
					Table: "users",
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position:  sdk.Position("999"),
					Operation: sdk.OperationCreate,
					Payload: sdk.Change{
						After: sdk.StructuredData{
							"UNKNOWN": 3,
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "should insert, json column",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL:   dsn,
					Table: "users",
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position:  sdk.Position("999"),
					Operation: sdk.OperationCreate,
					Payload: sdk.Change{
						After: sdk.StructuredData{
							"id":     7,
							"name":   "alien",
							"skills": map[string]any{"read": 2, "write": 3},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "should insert, json nested column",
			fields: fields{
				conn: conn,
				config: config.Config{
					URL:   dsn,
					Table: "users",
				},
			},
			args: args{
				ctx: context.Background(),
				record: sdk.Record{
					Position:  sdk.Position("999"),
					Operation: sdk.OperationCreate,
					Payload: sdk.Change{
						After: sdk.StructuredData{
							"id":   7,
							"name": "alien",
							"skills": map[string]any{
								"read": 2,
								"nested": map[string]any{
									"level": 3,
								},
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Destination{
				UnimplementedDestination: tt.fields.UnimplementedDestination,
				conn:                     tt.fields.conn,
				config:                   tt.fields.config,
			}
			if _, err := d.Write(tt.args.ctx, []sdk.Record{tt.args.record}); (err != nil) != tt.wantErr {
				t.Errorf("Destination.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
