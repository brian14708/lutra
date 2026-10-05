package result

import (
	"bytes"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

func TestTerminalFailureRoundTrip(t *testing.T) {
	encoded, err := EncodeFailure(Failure{Terminal: true, Code: 409, Message: "already closed"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, tagged, err := DecodeFailure(encoded)
	if err != nil || !tagged || !decoded.Terminal || decoded.Cacheable || decoded.Code != 409 || decoded.Message != "already closed" || !bytes.Equal(decoded.Details, []byte{0xf6}) {
		t.Fatalf("terminal result = %+v, tagged=%v, err=%v", decoded, tagged, err)
	}
}

func TestInvalidTerminalFailures(t *testing.T) {
	for _, fields := range []map[string]any{
		{"terminal": "true", "code": 500},
		{"terminal": nil, "code": 500},
		{"terminal": true, "code": 399},
		{"terminal": true, "code": 600},
		{"terminal": true, "code": nil},
		{"terminal": true, "code": 500, "cacheable": true},
	} {
		if _, exists := fields["cacheable"]; !exists {
			fields["cacheable"] = false
		}
		fields["message"], fields["details"] = "invalid", nil
		encoded, err := cbor.Marshal(cbor.Tag{Number: ErrorTag, Content: fields})
		if err != nil {
			t.Fatal(err)
		}
		if _, tagged, err := DecodeFailure(encoded); !tagged || err == nil {
			t.Fatalf("accepted malformed terminal failure: %v", fields)
		}
	}
}
