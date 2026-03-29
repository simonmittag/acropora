package acropora

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
)

// Session is a version-bound runtime session.
type Session struct {
	db      *DB
	version OntologyVersion
}

// NewSession creates a new session bound to exactly one ontology version.
func (d *DB) NewSession(version OntologyVersion) *Session {
	return &Session{
		db:      d,
		version: version,
	}
}

// normalizeCanonicalName helper for entity-name normalization.
func normalizeCanonicalName(name string) string {
	// 1. Remove non-printable / control characters
	var b strings.Builder
	for _, r := range name {
		if unicode.IsPrint(r) && r != 0 {
			b.WriteRune(r)
		}
	}
	s := b.String()

	// 2. Lowercase
	s = strings.ToLower(s)

	// 3. Trim leading/trailing whitespace
	s = strings.TrimSpace(s)

	// 4. Collapse repeated internal whitespace to single spaces
	fields := strings.Fields(s)
	return strings.Join(fields, " ")
}

// MergeEntityMetadata merges metadata from multiple entities, with canonical winning on conflict.
func MergeEntityMetadata(canonical json.RawMessage, aliases ...json.RawMessage) (json.RawMessage, error) {
	var dest map[string]interface{}
	if len(canonical) > 0 {
		if err := json.Unmarshal(canonical, &dest); err != nil {
			return nil, fmt.Errorf("unmarshaling canonical metadata: %w", err)
		}
	}
	if dest == nil {
		dest = make(map[string]interface{})
	}

	for _, a := range aliases {
		if len(a) == 0 {
			continue
		}
		var src map[string]interface{}
		if err := json.Unmarshal(a, &src); err != nil {
			return nil, fmt.Errorf("unmarshaling alias metadata: %w", err)
		}
		mergeMaps(dest, src)
	}

	return json.Marshal(dest)
}

func mergeMaps(dest, src map[string]interface{}) {
	for k, v := range src {
		if existing, ok := dest[k]; ok {
			destMap, destIsMap := existing.(map[string]interface{})
			srcMap, srcIsMap := v.(map[string]interface{})
			if destIsMap && srcIsMap {
				mergeMaps(destMap, srcMap)
			}
			// if types differ or not both maps, dest (canonical) wins
		} else {
			dest[k] = v
		}
	}
}
