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
	"encoding/json"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/conduitio/conduit-connector-materialize/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v4"
)

// Postgres/Materialize requires use of a different variable placeholder.
var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

const (
	// metadata related.
	metadataTable  = "table"
	metadataAction = "action"

	// action names.
	actionInsert = "insert"
)

// Destination Materialize Connector persists records to an Materialize database.
type Destination struct {
	sdk.UnimplementedDestination

	conn   *pgx.Conn
	config config.Config
}

// NewDestination creates new instance of the Destination.
func NewDestination() sdk.Destination {
	return &Destination{}
}

// Configure parses and initializes the config.
func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	configuration, err := config.Parse(cfg)
	if err != nil {
		return err
	}

	d.config = configuration

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, d.config.URL)
	if err != nil {
		return err
	}

	d.conn = conn

	return nil
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, record sdk.Record) error {
	action, ok := record.Metadata[metadataAction]
	if !ok {
		return d.insert(ctx, record)
	}

	switch action {
	case actionInsert:
		return d.insert(ctx, record)
	default:
		return nil
	}
}

// insert is an append-only operation that doesn't care about keys.
func (d *Destination) insert(ctx context.Context, record sdk.Record) error {
	tableName, err := d.getTableName(record.Metadata)
	if err != nil {
		return fmt.Errorf("failed to get table name: %w", err)
	}

	payload, err := d.structurizeData(record.Payload)
	if err != nil {
		return fmt.Errorf("failed to get payload: %w", err)
	}

	colArgs, valArgs := d.extractColumnsAndValues(payload)

	query, args, err := psql.
		Insert(tableName).
		Columns(colArgs...).
		Values(valArgs...).
		ToSql()
	if err != nil {
		return fmt.Errorf("error formating query: %w", err)
	}

	_, err = d.conn.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to exec insert: %w", err)
	}

	return nil
}

// extractColumnsAndValues turns the payload into slices of
// columns and values for upserting into Materialize.
func (d *Destination) extractColumnsAndValues(payload sdk.StructuredData) ([]string, []any) {
	var (
		colArgs []string
		valArgs []any
	)

	for field, value := range payload {
		colArgs = append(colArgs, field)
		valArgs = append(valArgs, value)
	}

	return colArgs, valArgs
}

// structurizeData converts sdk.Data to sdk.StructuredData.
func (d *Destination) structurizeData(data sdk.Data) (sdk.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return sdk.StructuredData{}, nil
	}

	structuredData := make(sdk.StructuredData)
	err := json.Unmarshal(data.Bytes(), &structuredData)
	if err != nil {
		return nil, err
	}

	return structuredData, nil
}

// getTableName returns either the records metadata value for table or the default configured
// value for table. Otherwise it will error since we require some table to be
// set to write into.
func (d *Destination) getTableName(metadata map[string]string) (string, error) {
	tableName, ok := metadata[metadataTable]
	if !ok {
		if d.config.Table == "" {
			return "", errors.New("no table provided for default writes")
		}

		return d.config.Table, nil
	}

	return tableName, nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.conn != nil {
		return d.conn.Close(ctx)
	}

	return nil
}
