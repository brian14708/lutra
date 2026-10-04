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
	encoded, err := mode.Marshal(cbor.Tag{Number: ErrorTag, Content: map[string]any{
		"cacheable": f.Cacheable, "message": f.Message, "details": details,
	}})
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
	flag := fields["cacheable"]
	if len(flag) != 1 || (flag[0] != 0xf4 && flag[0] != 0xf5) {
		return Failure{}, true, errors.New("invalid result failure cacheability")
	}
	if err := cbor.Unmarshal(fields["cacheable"], &f.Cacheable); err != nil {
		return Failure{}, true, errors.New("invalid result failure cacheability")
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

func Validate(encoded []byte) error {
	if len(encoded) == 0 || len(encoded) > MaxResultSize {
		return errors.New("result CBOR size is out of bounds")
	}
	var value any
	if err := cbor.Unmarshal(encoded, &value); err != nil {
		return fmt.Errorf("invalid result CBOR: %w", err)
	}
	if _, tagged, err := DecodeFailure(encoded); err != nil {
		return err
	} else if tagged {
		return nil
	}
	return nil
}
