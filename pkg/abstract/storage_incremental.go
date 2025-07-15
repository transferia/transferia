package abstract

type IncrementalState struct {
	Schema  string // for example - for mysql here are database name
	Name    string
	Payload WhereStatement
}

func (s *IncrementalState) ID() TableID {
	return TableID{Namespace: s.Schema, Name: s.Name}
}

func TableDescriptionToIncrementalState(in []TableDescription) []IncrementalState {
	result := make([]IncrementalState, 0, len(in))
	for i := range in {
		result = append(result, IncrementalState{
			Schema:  in[i].Schema,
			Name:    in[i].Name,
			Payload: in[i].Filter,
		})
	}
	return result
}

func IncrementalStateToTableDescription(in []IncrementalState) []TableDescription {
	result := make([]TableDescription, 0, len(in))
	for i := range in {
		result = append(result, TableDescription{
			Schema: in[i].Schema,
			Name:   in[i].Name,
			Filter: in[i].Payload,
			EtaRow: 0,
			Offset: 0,
		})
	}
	return result
}
