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
	"github.com/conduitio/conduit-connector-materialize/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Spec struct{}

// Specification returns the Plugin's Specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "materialize",
		Summary: "A Materialize destination plugin for Conduit, written in Go.",
		Version: "v0.1.0",
		Author:  "Meroxa, Inc.",
		DestinationParams: map[string]sdk.Parameter{
			config.ConfigKeyURL: {
				Default:     "",
				Required:    true,
				Description: "The connection URL for Materialize instance.",
			},
			config.ConfigKeyTable: {
				Default:     "",
				Required:    true,
				Description: "The table name of the table in Materialize that the connector should write to, by default.",
			},
			config.ConfigKeyKey: {
				Default:     "",
				Required:    true,
				Description: "The column name used when updating and deleting records.",
			},
		},
	}
}
