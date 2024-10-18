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
	"reflect"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
)

func TestConvertStructureData(t *testing.T) {
	t.Parallel()

	type args struct {
		columnTypes map[string]string
		data        opencdc.StructuredData
	}

	tests := []struct {
		name    string
		args    args
		want    opencdc.StructuredData
		wantErr bool
	}{
		{
			name: "success_time_layout",
			args: args{
				columnTypes: map[string]string{
					"id":         "integer",
					"created_at": "time",
				},
				data: opencdc.StructuredData{
					"id":         1,
					"created_at": "10:34:54",
				},
			},
			want: opencdc.StructuredData{
				"id":         1,
				"created_at": "10:34:54",
			},
		},
		{
			name: "success_rfc3339",
			args: args{
				columnTypes: map[string]string{
					"id":         "integer",
					"created_at": "time",
				},
				data: opencdc.StructuredData{
					"id":         1,
					"created_at": "0000-01-01T11:12:00Z",
				},
			},
			want: opencdc.StructuredData{
				"id":         1,
				"created_at": "11:12:00",
			},
		},
		{
			name: "fail",
			args: args{
				columnTypes: map[string]string{
					"id":         "integer",
					"created_at": "time",
				},
				data: opencdc.StructuredData{
					"id":         1,
					"created_at": "11l12",
				},
			},
			want:    opencdc.StructuredData{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ConvertStructureData(context.Background(), tt.args.columnTypes, tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertStructureData() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConvertStructureData() = %v, want %v", got, tt.want)
			}
		})
	}
}
