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

package main

import (
	materialize "github.com/conduitio/conduit-connector-materialize"
	"github.com/conduitio/conduit-connector-materialize/destination"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	sdk.Serve(materialize.Specification, nil, destination.NewDestination)
}
