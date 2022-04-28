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

package config

const (
	// ConfigKeyURL is the config name for a connection URL.
	ConfigKeyURL = "url"
	// ConfigKeyTable is the config name for a table.
	ConfigKeyTable = "table"
	// ConfigKeyKey is the config name for a key.
	ConfigKeyKey = "key"
)

// Config represents configuration needed for Materialize.
type Config struct {
	URL string `validate:"required,url"`
	// The maximum identifier length is 63.
	// See https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS.
	Table string `validate:"required,max=63"`
	Key   string `validate:"required,max=63"`
}

// Parse attempts to parse a provided map[string]string into a Config struct.
func Parse(cfg map[string]string) (Config, error) {
	config := Config{
		URL:   cfg[ConfigKeyURL],
		Table: cfg[ConfigKeyTable],
		Key:   cfg[ConfigKeyKey],
	}

	if err := config.Validate(); err != nil {
		return Config{}, err
	}

	return config, nil
}
