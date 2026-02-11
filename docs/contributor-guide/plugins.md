# Plugins and Registries

This document explains the plugin system and registries in Transferia.

## Plugin Types

Transferia uses three main types of plugins:

1. **Providers** - Core plugins for data sources and destinations
2. **Parsers** - Data format parsers
3. **Transformers** - Data transformation plugins (see [Transformers](./transformers.md))

## Provider System

### Provider Interfaces

Providers in Transferia use interface-based duck typing to define their capabilities:

```go
// Base provider interface
type Provider interface {
    Type() abstract.ProviderType
}

// Feature interfaces
type Snapshot interface {
    Provider
    Storage() (abstract.Storage, error)
}

type Replication interface {
    Provider
    Source() (abstract.Source, error)
}

type Sinker interface {
    Provider
    Sink(config middlewares.Config) (abstract.Sinker, error)
}

type AsyncSinker interface {
    Provider
    AsyncSink(middleware abstract.Middleware) (abstract.AsyncSink, error)
}

type SnapshotSinker interface {
    Provider
    SnapshotSink(config middlewares.Config) (abstract.Sinker, error)
}

type Checksumable interface {
    Provider
    SourceChecksumableStorage() (abstract.ChecksumableStorage, []abstract.TableDescription, error)
    DestinationChecksumableStorage() (abstract.ChecksumableStorage, error)
}
```

### Provider Registration

Providers register themselves in the global registry during initialization:

```go
// Provider factory type
type ProviderFactory func(
    lgr log.Logger, 
    registry metrics.Registry, 
    cp coordinator.Coordinator, 
    transfer *model.Transfer,
) Provider

// Global registry
var knownProviders = map[abstract.ProviderType]ProviderFactory{}

// Registration function
func Register(providerType abstract.ProviderType, fac ProviderFactory) {
    knownProviders[providerType] = fac
}
```

### Example Provider Implementation

Here's an example of implementing a provider:

```go
const ProviderType = abstract.ProviderType("myprovider")

// Provider struct
type Provider struct {
    logger   log.Logger
    registry metrics.Registry
    cp       coordinator.Coordinator
    transfer *model.Transfer
}

// Interface implementations
var (
    _ providers.Replication = (*Provider)(nil)
    _ providers.Snapshot    = (*Provider)(nil)
)

// Provider methods
func (p *Provider) Type() abstract.ProviderType {
    return ProviderType
}

func (p *Provider) Storage() (abstract.Storage, error) {
    // Implementation
}

func (p *Provider) Source() (abstract.Source, error) {
    // Implementation
}

// Registration in init()
func init() {
    // Register source factory
    model.RegisterSource(ProviderType, func() model.Source {
        return new(MySource)
    })
    
    // Register provider name
    abstract.RegisterProviderName(ProviderType, "My Provider")
    
    // Register provider factory
    providers.Register(ProviderType, func(
        lgr log.Logger,
        registry metrics.Registry,
        cp coordinator.Coordinator,
        transfer *model.Transfer,
    ) providers.Provider {
        return &Provider{
            logger:   lgr,
            registry: registry,
            cp:       cp,
            transfer: transfer,
        }
    })
}
```

### Provider Resolution

The system resolves providers based on their implemented interfaces:

```go
// Source resolution
func Source[T Provider](
    lgr log.Logger,
    registry metrics.Registry,
    cp coordinator.Coordinator,
    transfer *model.Transfer,
) (T, bool) {
    var defRes T
    f, ok := knownProviders[transfer.SrcType()]
    if !ok {
        return defRes, false
    }
    res := f(lgr, registry, cp, transfer)
    typedRes, ok := res.(T)
    return typedRes, ok
}

// Destination resolution
func Destination[T Provider](
    lgr log.Logger,
    registry metrics.Registry,
    cp coordinator.Coordinator,
    transfer *model.Transfer,
) (T, bool) {
    var defRes T
    f, ok := knownProviders[transfer.DstType()]
    if !ok {
        return defRes, false
    }
    res := f(lgr, registry, cp, transfer)
    typedRes, ok := res.(T)
    return typedRes, ok
}
```

### Best Practices

1. **Provider Implementation**:
   - Implement only needed interfaces
   - Use interface assertions for type safety
   - Follow error handling patterns
   - Keep provider state minimal

2. **Registration**:
   - Register all components in `init()`
   - Register source/destination factories
   - Register provider name
   - Register provider factory

3. **Configuration**:
   - Use transfer configuration
   - Validate configuration early
   - Handle errors gracefully
   - Support feature flags

## Parser System

### Parser Registration

Parsers register themselves in a central registry:

```go
type Parser interface {
    Parse(data []byte) ([]ChangeItem, error)
    Name() string
}

func RegisterParser(name string, factory func() Parser)
```

### Parser Configuration

Parsers can be configured through a generic config map:

```go
type ParserConfig struct {
    Type string
    Config map[string]interface{}
}
```

## Registry System

### Global Registries

The system maintains several global registries:

1. **Provider Registry**
```go
var knownProviders = map[abstract.ProviderType]ProviderFactory{}
```

2. **Source Registry**
```go
var knownSources = map[abstract.ProviderType]func() Source{}
```

3. **Destination Registry**
```go
var knownDestinations = map[abstract.ProviderType]func() Destination{}
```

4. **Parser Registry**
```go
var knownParsers = map[string]func() Parser{}
```

### Registry Operations

Key registry operations:

1. **Registration**
```go
func Register(providerType abstract.ProviderType, factory ProviderFactory)
func RegisterSource(typ abstract.ProviderType, factory func() Source)
func RegisterDestination(typ abstract.ProviderType, factory func() Destination)
```

2. **Lookup**
```go
func Source[T Provider](logger, registry, coordinator, transfer) (T, bool)
func Destination[T Provider](logger, registry, coordinator, transfer) (T, bool)
```

3. **Discovery**
```go
func KnownSources() []string
func KnownDestinations() []string
```

## Best Practices

1. **Provider Implementation**
   - Implement only needed interfaces
   - Use composition over inheritance
   - Follow error handling patterns

2. **Parser Implementation**
   - Handle partial data
   - Support streaming
   - Validate input data

## Example: Adding New Provider

1. Define provider type:
```go
const ProviderType = abstract.ProviderType("myprovider")
```

2. Implement interfaces:
```go
type Provider struct {
    logger   log.Logger
    registry metrics.Registry
    transfer *model.Transfer
}

func (p *Provider) Type() abstract.ProviderType {
    return ProviderType
}

func (p *Provider) Source() (abstract.Source, error) {
    // Implementation
}
```

3. Register provider:
```go
func init() {
    model.RegisterSource(ProviderType, func() model.Source {
        return new(MySource)
    })
    providers.Register(ProviderType, New)
}
```

## Next Steps

- Learn about [Transformers](./transformers.md)
- Explore [Advanced Topics](./advanced.md)
- Check out [Examples](../../examples) 