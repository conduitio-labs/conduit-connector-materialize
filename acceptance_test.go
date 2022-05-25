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
	"fmt"
	"os"
	"testing"

	"github.com/conduitio/conduit-connector-materialize/config"
	"github.com/conduitio/conduit-connector-materialize/destination"
	"github.com/conduitio/conduit-connector-materialize/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var (
	dsn       = "postgres://materialize@localhost:6875/materialize?sslmode=disable"
	testTable = "acceptance_test_users"
)

func TestMain(m *testing.M) {
	os.Exit(testMainWrapper(m))
}

func testMainWrapper(m *testing.M) int {
	conn, err := test.SetupTestConnection(dsn)
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

func TestAcceptance(t *testing.T) {
	sdk.AcceptanceTest(t, sdk.ConfigurableAcceptanceTestDriver{
		Config: sdk.ConfigurableAcceptanceTestDriverConfig{
			Connector: sdk.Connector{
				NewSpecification: Specification,
				NewSource:        nil,
				NewDestination:   destination.NewDestination,
			},
			SourceConfig: nil,
			DestinationConfig: map[string]string{
				config.ConfigKeyURL:   dsn,
				config.ConfigKeyTable: testTable,
				config.ConfigKeyKey:   "id",
			},
			Skip: []string{"TestDestination_Write*"},
		},
	},
	)
}
