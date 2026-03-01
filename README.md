# Static Type-Safe Declarative Ingestion Framework (Prisma-Bound YAML)

A declarative ingestion system where YAML pipeline definitions are statically validated against the Prisma schema at authoring time via JSON Schema and YAML LSP integration, eliminating runtime schema mismatch errors.

## Core Principle

**Prisma schema is the single source of truth** — YAML does not define types and only maps source fields into existing Prisma models; all validation derives from Prisma metadata.

## Architecture Flow

```
schema.prisma → Prisma DMMF extraction → Generate JSON Schema → YAML Language Server 
→ Runtime ingestion engine (validated YAML) → Database
```

## Features

- ✅ **Static Validation**: YAML pipeline definitions are validated at authoring time
- ✅ **Type-Safe Transforms**: Transform functions are scoped to Prisma field types
- ✅ **Required Field Detection**: Missing required Prisma fields are flagged in the editor
- ✅ **Unknown Field Prevention**: `additionalProperties: false` prevents mapping unknown fields
- ✅ **VSCode Integration**: Red Hat YAML LSP with autocomplete and validation
- ✅ **Runtime Efficiency**: Engine assumes schema correctness and skips structural validation

## Quick Start

### 1. Install Dependencies

```bash
npm install
```

### 2. Generate JSON Schema from Prisma

```bash
npm run generate-schema
```

This extracts Prisma DMMF and generates `generated/pipeline.schema.json`.

### 3. Create Pipeline YAML

See `pipelines/` for examples:

```yaml
$schema: ./generated/pipeline.schema.json

model: Trade
source:
  type: http
  url: https://api.exchange.com/v1/trades
  method: GET
  jsonPath: $.data
mapping:
  id:
    from: "$.tradeId"
    transform: toString
  price:
    from: "$.price"
    transform: toFloat
  timestamp:
    from: "$.executedAt"
    transform: toDate
  symbol:
    from: "$.symbol"
options:
  batchSize: 100
  onConflict: update
```

### 4. Run Ingestion

```bash
npm run ingest
```

## Project Structure

```
/workspace/project
├── prisma/
│   └── schema.prisma          # Prisma data model (source of truth)
├── scripts/
│   └── generate-pipeline-schema.ts  # Schema generator
├── generated/
│   └── pipeline.schema.json   # Generated JSON Schema
├── pipelines/
│   ├── trade-ingestion.yaml   # Example pipeline
│   └── market-ingestion.yaml  # Example pipeline
├── src/
│   ├── runtime/
│   │   ├── ingest.ts          # Runtime ingestion engine
│   │   └── types.ts          # Type definitions
│   └── transforms/
│       └── index.ts           # Transform functions registry
├── .vscode/
│   └── settings.json          # YAML LSP configuration
├── package.json
├── tsconfig.json
└── README.md
```

## Static Validation Errors Detected in Editor

| Error | Description |
|-------|-------------|
| Unknown model name | Model not defined in Prisma schema |
| Unknown field in mapping | Field doesn't exist in selected Prisma model |
| Missing required field | Required Prisma field not mapped |
| Invalid transform | Transform not compatible with Prisma field type |
| Extra unmapped properties | `additionalProperties: false` prevents typos |
| Invalid YAML structure | Schema validation errors |

## Transform Functions by Type

### String
`trim`, `toUpperCase`, `toLowerCase`, `toString`, `substring`, `replace`

### Int / Float
`toInt`, `toFloat`, `toString`, `parseInt`, `parseFloat`

### Boolean
`toBoolean`

### DateTime
`toDate`, `toISOString`, `toTimestamp` (supports `format`: ISO8601, unix, unix毫秒)

### Json
`toJson`, `parseJson`

## VSCode Integration

The `.vscode/settings.json` configures the YAML Language Server:

```json
{
  "yaml.schemas": {
    "generated/pipeline.schema.json": "pipelines/*.yaml"
  },
  "yaml.validate": true
}
```

With this setup, you'll see:
- ✨ Autocomplete for model names and field names
- ⚠️ Inline validation errors
- 📝 Rich descriptions for each field

## Design Constraints

1. **Prisma is authoritative** for data model and types
2. **YAML is declarative mapping only** — no arbitrary JavaScript
3. **Transformation registry is predefined and type-scoped**
4. **JSON Schema is regenerated** whenever Prisma schema changes

## Runtime Engine

The ingestion engine (`src/runtime/ingest.ts`) assumes schema correctness and performs:

1. **Extraction**: Fetch data from HTTP, file, stream, or webhook sources
2. **Transformation**: Apply JSONPath extraction and type transforms
3. **Database Write**: Write to Prisma (direct, bulk, or upsert modes)

No structural validation is performed at runtime — errors are caught at authoring time.

## License

MIT
