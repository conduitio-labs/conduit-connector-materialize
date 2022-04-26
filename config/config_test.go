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

import "testing"

var exampleConfig = map[string]string{
	"url":   "postgres://materialize@localhost:6875/materialize?sslmode=disable",
	"table": "footable",
	"key":   "id",
}

func configWith(pairs ...string) map[string]string {
	cfg := make(map[string]string)

	for key, value := range exampleConfig {
		cfg[key] = value
	}

	for i := 0; i < len(pairs); i += 2 {
		key := pairs[i]
		value := pairs[i+1]
		cfg[key] = value
	}

	return cfg
}

func configWithout(keys ...string) map[string]string {
	cfg := make(map[string]string)

	for key, value := range exampleConfig {
		cfg[key] = value
	}

	for _, key := range keys {
		delete(cfg, key)
	}

	return cfg
}

func Test_URL(t *testing.T) {
	t.Run("Successful", func(t *testing.T) {
		c, err := Parse(configWith("url", "some-value"))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if c.URL != "some-value" {
			t.Fatalf("expected URL to be %q, got %q", "some-value", c.URL)
		}
	})

	t.Run("Missing", func(t *testing.T) {
		_, err := Parse(configWithout("url"))
		if err == nil {
			t.Fatal("expected error, got nothing")
		}

		expectedErrMsg := `"url" config value must be set`
		if err.Error() != expectedErrMsg {
			t.Fatalf("expected error msg to be %q, got %q", expectedErrMsg, err.Error())
		}
	})
}

func Test_Table(t *testing.T) {
	t.Run("Successful", func(t *testing.T) {
		c, err := Parse(configWith("table", "some-value"))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if c.Table != "some-value" {
			t.Fatalf("expected Table to be %q, got %q", "some-value", c.Table)
		}
	})

	t.Run("Missing", func(t *testing.T) {
		_, err := Parse(configWithout("table"))
		if err == nil {
			t.Fatal("expected error, got nothing")
		}

		expectedErrMsg := `"table" config value must be set`
		if err.Error() != expectedErrMsg {
			t.Fatalf("expected error msg to be %q, got %q", expectedErrMsg, err.Error())
		}
	})
}

func Test_Key(t *testing.T) {
	t.Run("Successful", func(t *testing.T) {
		c, err := Parse(configWith("key", "some-value"))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if c.Key != "some-value" {
			t.Fatalf("expected Key to be %q, got %q", "some-value", c.Key)
		}
	})

	t.Run("Missing", func(t *testing.T) {
		_, err := Parse(configWithout("key"))
		if err == nil {
			t.Fatal("expected error, got nothing")
		}

		expectedErrMsg := `"key" config value must be set`
		if err.Error() != expectedErrMsg {
			t.Fatalf("expected error msg to be %q, got %q", expectedErrMsg, err.Error())
		}
	})
}
