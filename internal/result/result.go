package result

import (
	"errors"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

const ErrorTag = 30001

const MaxResultSize = 1 << 20

type Failure struct {
	Cacheable bool
	Terminal  bool
	Code      int
	Message   string
	Details   cbor.RawMessage
}

func EncodeFailure(f Failure) ([]byte, error) {
	if f.Message == "" {
		return nil, errors.New("result failure message is empty")
	}
	if f.Details == nil {
		f.Details = cbor.RawMessage{0xf6}
	}
	if f.Cacheable && len(f.Details) > 64<<10 {
		return nil, errors.New("failure details exceed 64 KiB")
	}
	var details any
	if err := cbor.Unmarshal(f.Details, &details); err != nil {
		return nil, err
	}
	mode, err := cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	fields := map[string]any{
		"cacheable": f.Cacheable, "message": f.Message, "details": details,
	}
	if f.Terminal {
		if f.Cacheable || f.Code < 400 || f.Code > 599 {
			return nil, errors.New("invalid terminal failure")
		}
		fields["terminal"], fields["code"] = true, f.Code
	}
	encoded, err := mode.Marshal(cbor.Tag{Number: ErrorTag, Content: fields})
	if err != nil {
		return nil, err
	}
	if len(encoded) > MaxResultSize {
		return nil, errors.New("result CBOR exceeds 1 MiB")
	}
	return encoded, nil
}

func DecodeFailure(encoded []byte) (Failure, bool, error) {
	var tag cbor.RawTag
	if err := cbor.Unmarshal(encoded, &tag); err != nil {
		return Failure{}, false, nil
	}
	if tag.Number != ErrorTag {
		return Failure{}, false, nil
	}
	var fields map[string]cbor.RawMessage
	if err := cbor.Unmarshal(tag.Content, &fields); err != nil {
		return Failure{}, true, errors.New("invalid result failure")
	}
	var f Failure
	if terminal, exists := fields["terminal"]; exists {
		if len(terminal) != 1 || (terminal[0] != 0xf4 && terminal[0] != 0xf5) {
			return Failure{}, true, errors.New("invalid result failure terminal flag")
		}
		f.Terminal = terminal[0] == 0xf5
		if f.Terminal {
			if err := cbor.Unmarshal(fields["code"], &f.Code); err != nil || f.Code < 400 || f.Code > 599 {
				return Failure{}, true, errors.New("invalid result failure terminal code")
			}
		}
	}
	if err := cbor.Unmarshal(fields["cacheable"], &f.Cacheable); err != nil {
		return Failure{}, true, errors.New("invalid result failure cacheability")
	}
	if f.Terminal && f.Cacheable {
		return Failure{}, true, errors.New("terminal failure cannot be cacheable")
	}
	if err := cbor.Unmarshal(fields["message"], &f.Message); err != nil || f.Message == "" {
		return Failure{}, true, errors.New("invalid result failure message")
	}
	f.Details = fields["details"]
	if len(f.Details) == 0 {
		return Failure{}, true, fmt.Errorf("invalid result failure details")
	}
	if f.Cacheable && len(f.Details) > 64<<10 {
		return Failure{}, true, errors.New("failure details exceed 64 KiB")
	}
	return f, true, nil
}

// Validate checks size bounds and CBOR wellformedness, returning the decoded
// failure when the value carries the error tag.
func Validate(encoded []byte) (Failure, bool, error) {
	if len(encoded) == 0 || len(encoded) > MaxResultSize {
		return Failure{}, false, errors.New("result CBOR size is out of bounds")
	}
	if err := cbor.Wellformed(encoded); err != nil {
		return Failure{}, false, fmt.Errorf("invalid result CBOR: %w", err)
	}
	return DecodeFailure(encoded)
}
