package domain

import (
	"encoding/json"
	"fmt"
)

// Normalizing every DBOS input representation prevents a duplicate workflow
// ID with different logical input from being silently accepted.
func DecodeFilingInput(value any) (FilingInput, error) {
	if typed, ok := value.(FilingInput); ok {
		return typed, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return FilingInput{}, err
	}
	var direct FilingInput
	if err := json.Unmarshal(raw, &direct); err == nil && direct.FilingID != "" {
		return direct, nil
	}
	var args []json.RawMessage
	if err := json.Unmarshal(raw, &args); err == nil && len(args) == 1 {
		if err := json.Unmarshal(args[0], &direct); err == nil && direct.FilingID != "" {
			return direct, nil
		}
	}
	if encoded, ok := value.(string); ok {
		if err := json.Unmarshal([]byte(encoded), &direct); err == nil && direct.FilingID != "" {
			return direct, nil
		}
		if err := json.Unmarshal([]byte(encoded), &args); err == nil && len(args) == 1 {
			if err := json.Unmarshal(args[0], &direct); err == nil && direct.FilingID != "" {
				return direct, nil
			}
		}
	}
	return FilingInput{}, fmt.Errorf("unsupported filing workflow input %T: %s", value, raw)
}
