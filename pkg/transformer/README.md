## What is transformer

We can apply stateless transformation on our `inflight` data.
This is based on our data model

![data-model](./assets/data_model_transformer.png)

Each batch of changes can be transformed into new batch of changes:

![transformation](./assets/transformer_data_flow.png)

### How to add new transformer

1. Create new package
2. Implemenet `abstract.Transformer` interface
3. Add `proto` mesage for transformer config
3. Register implementation

Example:

#### Implementation of `abstract.Transformer`

```go
type DummyTransformer struct {
}

func (r *DummyTransformer) Apply(input []abstract.ChangeItem) abstract.TransformerResult {
	return abstract.TransformerResult{
		Transformed: input,
		Errors:      nil,
	}
}

func (r *DummyTransformer) Suitable(table abstract.TableID, schema *abstract.TableSchema) bool {
	return true
}

func (r *DummyTransformer) ResultSchema(original *abstract.TableSchema) (*abstract.TableSchema, error) {
	return original, nil
}

func (r *DummyTransformer) Description() string {
	return "this transformer do nothing"
}
```

#### Proto interface

```go
message DummyTransformer {
	// here some params can be placed
}
```

#### Registration

```go

func init() {
	transformer.Register(new(api.DummyTransformer), func(protoConfig any, lgr log.Logger) (abstract.Transformer, error) {
		return &DummyTransformer{}, nil
	})
}
```

#### Prefillment on transfer create

If you want your transformers to prefill on transfer create, you should insert your typeswitch in this function
called `prefillTransformationDefaults` in `transfer_service.go`:
https://a.yandex-team.ru/arcadia/transfer_manager/go/pkg/controlplane/transfer_service.go?rev=r11799915#L127
