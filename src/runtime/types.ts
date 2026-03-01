// Type definitions for Prisma-Bound YAML Ingestion Framework

export interface PipelineDefinition {
  $schema?: string;
  model: string;
  source: SourceDefinition;
  mapping: FieldMappingDefinition;
  options?: PipelineOptions;
}

export interface SourceDefinition {
  type: 'http' | 'file' | 'stream' | 'webhook';
  url?: string;
  path?: string;
  headers?: Record<string, string>;
  method?: 'GET' | 'POST' | 'PUT' | 'PATCH';
  body?: any;
  jsonPath?: string;
}

export interface FieldMappingDefinition {
  [fieldName: string]: FieldMapping;
}

export interface FieldMapping {
  from: string;
  transform?: TransformType;
  format?: DateTimeFormat;
  default?: any;
}

export type TransformType = 
  // String transforms
  | 'trim' 
  | 'toUpperCase' 
  | 'toLowerCase' 
  | 'toString' 
  | 'substring' 
  | 'replace'
  // Number transforms
  | 'toInt' 
  | 'toFloat' 
  | 'parseInt' 
  | 'parseFloat'
  // Boolean transforms
  | 'toBoolean'
  // DateTime transforms
  | 'toDate' 
  | 'toISOString' 
  | 'toTimestamp'
  // BigInt transforms
  | 'toBigInt'
  // JSON transforms
  | 'toJson' 
  | 'parseJson'
  // Bytes transforms
  | 'toBytes'
  | 'toBase64';

export type DateTimeFormat = 'ISO8601' | 'unix' | 'unix毫秒';

export interface PipelineOptions {
  batchSize?: number;
  retryAttempts?: number;
  retryDelay?: number;
  onConflict?: 'ignore' | 'update' | 'error';
  transform?: 'direct' | 'bulk' | 'upsert';
}

// Runtime types
export interface RuntimeRecord {
  [key: string]: any;
}

export interface IngestionResult {
  success: boolean;
  recordsProcessed: number;
  errors: string[];
}

// Prisma model info (loaded at runtime)
export interface PrismaModelInfo {
  name: string;
  fields: Record<string, string>; // field name -> Prisma type
  requiredFields: string[];
  idFields: string[];
}

// Schema generator types
export interface SchemaGeneratorOptions {
  schemaPath: string;
  outputPath: string;
  schemaId?: string;
}
