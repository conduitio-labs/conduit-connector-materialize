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

	"github.com/conduitio/conduit-connector-materialize/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Destination Materialize Connector persists records to an Materialize database.
type Destination struct {
	sdk.UnimplementedDestination

	Config config.Config
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

	d.Config = configuration

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	return nil
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, record sdk.Record) error {
	return nil
}

// Teardown gracefully close connections.
func (d *Destination) Teardown(ctx context.Context) error {
	return nil
}
