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

package coltypes

import (
	"context"
	"fmt"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v4"
)

const (
	// Postgres/Materialize data types names.
	timeDataType = "time"

	// timeDataTypeLayout is a time format for the TIME data type.
	timeDataTypeLayout = "15:04:05"
)

var (
	// querySchemaColumnTypes is a query that selects column names and
	// their data and column types from the information_schema.
	querySchemaColumnTypes = "select column_name, data_type " +
		"from information_schema.columns where table_name = $1;"
)

// Querier is a database querier interface needed for the GetColumnTypes function.
type Querier interface {
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
}

// ConvertStructureData converts an sdk.StructureData values to a proper database types
// based on the provided columnTypes.
// For now it converts just TIME values.
func ConvertStructureData(
	ctx context.Context, columnTypes map[string]string, data sdk.StructuredData,
) (sdk.StructuredData, error) {
	result := make(sdk.StructuredData, len(data))

	for key, value := range data {
		if value == nil {
			result[key] = value

			continue
		}

		switch columnTypes[key] {
		case timeDataType:
			parsedValue, err := parseTime(value)
			if err != nil {
				return sdk.StructuredData{}, fmt.Errorf("parse time: %w", err)
			}

			result[key] = parsedValue

		default:
			result[key] = value
		}
	}

	return result, nil
}

// GetColumnTypes returns a map containing all table's columns and their database types.
func GetColumnTypes(ctx context.Context, querier Querier, tableName string) (map[string]string, error) {
	rows, err := querier.Query(ctx, querySchemaColumnTypes, tableName)
	if err != nil {
		return nil, fmt.Errorf("query column types: %w", err)
	}

	columnTypes := make(map[string]string)
	for rows.Next() {
		var columnName, dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			return nil, fmt.Errorf("scan rows: %w", err)
		}

		columnTypes[columnName] = strings.ToLower(dataType)
	}

	return columnTypes, nil
}

// parseTime parses a value trying to extract a time.Time from it and
// formats the resulting value according to the TIME layout.
func parseTime(value any) (string, error) {
	switch t := value.(type) {
	case string:
		// first, check if the string fits the TIME data type layout
		_, err := time.Parse(timeDataTypeLayout, t)
		if err == nil {
			return t, nil
		}

		// if it's not - try to parse according to RFC 3339.
		parsed, err := time.Parse(time.RFC3339, t)
		if err != nil {
			return "", fmt.Errorf("parse rfc3339 value: %w", err)
		}

		return parsed.Format(timeDataTypeLayout), nil

	case time.Time:
		return t.Format(timeDataTypeLayout), nil

	default:
		return "", fmt.Errorf("convert value %q to time", value)
	}
}
