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
	"fmt"

	"github.com/conduitio/conduit-connector-materialize/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/doug-martin/goqu/v9"
	"github.com/jackc/pgx/v4"
)

const (
	// metadata related.
	metadataTable  = "table"
	metadataAction = "action"

	// action names.
	actionInsert = "insert"
	actionUpdate = "update"
	actionDelete = "delete"
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
		return fmt.Errorf("failed to parse config: %w", err)
	}

	d.config = configuration

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, d.config.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to materialize: %w", err)
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
	case actionUpdate:
		return d.update(ctx, record)
	case actionDelete:
		return d.delete(ctx, record)
	default:
		return d.insert(ctx, record)
	}
}

// insert is an append-only operation that doesn't care about keys.
func (d *Destination) insert(ctx context.Context, record sdk.Record) error {
	tableName := d.getTableName(record.Metadata)

	payload, err := d.structurizeData(record.Payload)
	if err != nil {
		return fmt.Errorf("failed to get payload: %w", err)
	}

	// if payload is empty we don't need to insert anything
	if payload == nil {
		return ErrEmptyPayload
	}

	colArgs, valArgs := d.extractColumnsAndValues(payload)

	query, args, err := goqu.
		Insert(tableName).
		Cols(colArgs...).
		Vals(valArgs).
		ToSQL()
	if err != nil {
		return fmt.Errorf("error formating query: %w", err)
	}

	_, err = d.conn.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to exec insert: %w", err)
	}

	return nil
}

// update updates records by a key.
//
// Note that Materialize doesn't support primary keys and unique constraints,
// so if there are duplicate keys in Materialize the connector will update all of them.
func (d *Destination) update(ctx context.Context, record sdk.Record) error {
	tableName := d.getTableName(record.Metadata)

	key, err := d.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("failed to get key: %w", err)
	}

	keyColumnName, err := d.getKeyColumnName(key)
	if err != nil {
		return fmt.Errorf("failed to get key column name: %w", err)
	}

	// do nothing if we didn't find a value for the key
	if _, ok := key[keyColumnName]; !ok {
		return ErrEmptyKey
	}

	payload, err := d.structurizeData(record.Payload)
	if err != nil {
		return fmt.Errorf("failed to get payload: %w", err)
	}

	// if payload is empty we don't need to insert anything
	if payload == nil {
		return ErrEmptyPayload
	}

	// remove key from the payload, we will use the key inside a WHERE clause.
	delete(payload, keyColumnName)

	query, args, err := goqu.
		Update(tableName).
		Set(payload).
		Where(goqu.Ex{keyColumnName: key[keyColumnName]}).
		ToSQL()
	if err != nil {
		return fmt.Errorf("error formating query: %w", err)
	}

	_, err = d.conn.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to exec update: %w", err)
	}

	return nil
}

// delete deletes records by a key. First it looks in the sdk.Record.Key,
// if it doesn't find a key there it will use the default configured value for a key.
//
// Note that Materialize doesn't support primary keys and unique constraints,
// so if there are duplicate keys in Materialize the connector will delete them all.
func (d *Destination) delete(ctx context.Context, record sdk.Record) error {
	tableName := d.getTableName(record.Metadata)

	key, err := d.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("failed to get key: %w", err)
	}

	keyColumnName, err := d.getKeyColumnName(key)
	if err != nil {
		return fmt.Errorf("failed to get key column name: %w", err)
	}

	// do nothing if we didn't find a value for the key
	if _, ok := key[keyColumnName]; !ok {
		return ErrEmptyKey
	}

	query, args, err := goqu.
		Delete(tableName).
		Where(goqu.Ex{keyColumnName: key[keyColumnName]}).
		ToSQL()
	if err != nil {
		return fmt.Errorf("error formating query: %w", err)
	}

	_, err = d.conn.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to exec delete: %w", err)
	}

	return nil
}

// extractColumnsAndValues turns the payload into slices of
// columns and values for upserting into Materialize.
func (d *Destination) extractColumnsAndValues(payload sdk.StructuredData) ([]any, []any) {
	var colArgs, valArgs []any

	for field, value := range payload {
		colArgs = append(colArgs, field)
		valArgs = append(valArgs, value)
	}

	return colArgs, valArgs
}

// structurizeData converts sdk.Data to sdk.StructuredData.
func (d *Destination) structurizeData(data sdk.Data) (sdk.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return nil, nil
	}

	structuredData := make(sdk.StructuredData)
	err := json.Unmarshal(data.Bytes(), &structuredData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal data into structured data: %w", err)
	}

	return structuredData, nil
}

// getTableName returns either the records metadata value for table
// or the default configured value for table.
func (d *Destination) getTableName(metadata map[string]string) string {
	tableName, ok := metadata[metadataTable]
	if !ok {
		return d.config.Table
	}

	return tableName
}

// getKeyColumnName returns either the first key within the Key structured data
// or the default key configured value for key.
func (d *Destination) getKeyColumnName(key sdk.StructuredData) (string, error) {
	if len(key) > 1 {
		return "", ErrCompositeKeysNotSupported
	}

	for k := range key {
		return k, nil
	}

	return d.config.Key, nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.conn != nil {
		return d.conn.Close(ctx)
	}

	return nil
}
