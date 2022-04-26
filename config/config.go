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

import "fmt"

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
	URL   string
	Table string
	Key   string
}

// Parse attempts to parse plugins.Config into a Config struct
func Parse(cfg map[string]string) (Config, error) {
	url, ok := cfg[ConfigKeyURL]
	if !ok {
		return Config{}, requiredConfigErr(ConfigKeyURL)
	}

	table, ok := cfg[ConfigKeyTable]
	if !ok {
		return Config{}, requiredConfigErr(ConfigKeyTable)
	}

	key, ok := cfg[ConfigKeyKey]
	if !ok {
		return Config{}, requiredConfigErr(ConfigKeyKey)
	}

	config := Config{
		URL:   url,
		Table: table,
		Key:   key,
	}

	return config, nil
}

func requiredConfigErr(name string) error {
	return fmt.Errorf("%q config value must be set", name)
}
