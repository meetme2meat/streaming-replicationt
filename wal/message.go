package wal

type Message struct {
	Kind      string        `json:"kind"`
	Schema    string        `json:"schema"`
	Table     string        `json:"table"`
	Names     []string      `json:"columnnames"`
	Types     []string      `json:"columntypes"`
	Values    []interface{} `json:"columnvalues"`
	OldKey    OldKeys       `json:"oldkeys"`
	Optionals []bool        `json:"columnoptionals"`
}

type OldKeys struct {
	Names  []string      `json:"keynames"`
	Types  []string      `json:"keytypes"`
	Values []interface{} `json:"keyvalues"`
}

func (m Message) GetValues() []interface{} {
	if m.Kind == "delete" {
		return m.OldKey.Values
	} else {
		return m.Values
	}
}

func (m Message) GetNames() []string {
	if m.Kind == "delete" {
		return m.OldKey.Names
	} else {
		return m.Names
	}
}
