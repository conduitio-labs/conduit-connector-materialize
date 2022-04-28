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

import (
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name        string
		cfg         map[string]string
		want        Config
		wantErr     bool
		expectedErr string
	}{
		{
			name: "successfull, all fields",
			cfg: map[string]string{
				"url":   "postgres://materialize@localhost:6875/materialize?sslmode=disable",
				"table": "footable",
				"key":   "id",
			},
			want: Config{
				URL:   "postgres://materialize@localhost:6875/materialize?sslmode=disable",
				Table: "footable",
				Key:   "id",
			},
			wantErr: false,
		},
		{
			name: "successfull, only url",
			cfg: map[string]string{
				"url": "postgres://materialize@localhost:6875/materialize?sslmode=disable",
			},
			want: Config{
				URL: "postgres://materialize@localhost:6875/materialize?sslmode=disable",
			},
			wantErr: false,
		},
		{
			name: "missing url",
			cfg: map[string]string{
				"table": "footable",
				"key":   "id",
			},
			want:        Config{},
			wantErr:     true,
			expectedErr: "\"url\" config value must be set",
		},
		{
			name: "invalid url",
			cfg: map[string]string{
				"url":   "not a url",
				"table": "footable",
				"key":   "id",
			},
			want:        Config{},
			wantErr:     true,
			expectedErr: "\"url\" config value must be a valid url",
		},
		{
			name: "table name is too long",
			cfg: map[string]string{
				"url":   "postgres://materialize@localhost:6875/materialize?sslmode=disable",
				"table": "a_very_long_identifier_name_that_does_not_fit_within_the_limits_of_a_database",
				"key":   "id",
			},
			want:        Config{},
			wantErr:     true,
			expectedErr: "\"table\" config value is too long",
		},
		{
			name: "key name is too long",
			cfg: map[string]string{
				"url":   "postgres://materialize@localhost:6875/materialize?sslmode=disable",
				"table": "footable",
				"key":   "a_very_long_identifier_name_that_does_not_fit_within_the_limits_of_a_database",
			},
			want:        Config{},
			wantErr:     true,
			expectedErr: "\"key\" config value is too long",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.cfg)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("parse error = \"%s\", wantErr %t", err.Error(), tt.wantErr)
					return
				}

				if err.Error() != tt.expectedErr {
					t.Errorf("expected error \"%s\", got \"%s\"", tt.expectedErr, err.Error())
					return
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parse = %v, want %v", got, tt.want)
			}
		})
	}
}
