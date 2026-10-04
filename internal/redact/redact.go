// Package redact removes sensitive configuration values from diagnostics.
package redact

import (
	"encoding/base64"
	"reflect"
	"sort"
	"strings"

	"github.com/fxamacker/cbor/v2"
)

type Filter struct {
	values []any
	texts  []string
}

func New(values [][]byte) *Filter {
	f := &Filter{}
	var collect func(any)
	collect = func(v any) {
		f.values = append(f.values, v)
		switch v := v.(type) {
		case string:
			if v != "" {
				f.texts = append(f.texts, v)
			}
		case []byte:
			if len(v) > 0 {
				f.texts = append(f.texts, string(v), base64.StdEncoding.EncodeToString(v))
			}
		case []any:
			for _, item := range v {
				collect(item)
			}
		case map[any]any:
			for key, item := range v {
				collect(key)
				collect(item)
			}
		}
	}
	for _, raw := range values {
		var value any
		if cbor.Unmarshal(raw, &value) == nil {
			collect(value)
		}
	}
	sort.Slice(f.texts, func(i, j int) bool { return len(f.texts[i]) > len(f.texts[j]) })
	return f
}

func (f *Filter) String(value string) string {
	if f == nil {
		return value
	}
	for _, secret := range f.texts {
		value = strings.ReplaceAll(value, secret, "[redacted]")
	}
	return value
}

func (f *Filter) CBOR(raw []byte) ([]byte, error) {
	if f == nil || len(f.values) == 0 {
		return raw, nil
	}
	var value any
	if err := cbor.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	var clean func(any) any
	clean = func(v any) any {
		for _, secret := range f.values {
			if reflect.DeepEqual(v, secret) {
				return "[redacted]"
			}
		}
		switch v := v.(type) {
		case string:
			return f.String(v)
		case []byte:
			return []byte(f.String(string(v)))
		case []any:
			for i, item := range v {
				v[i] = clean(item)
			}
			return v
		case map[any]any:
			out := map[any]any{}
			for key, item := range v {
				out[clean(key)] = clean(item)
			}
			return out
		case cbor.Tag:
			v.Content = clean(v.Content)
			return v
		}
		return v
	}
	enc, _ := cbor.CanonicalEncOptions().EncMode()
	return enc.Marshal(clean(value))
}
