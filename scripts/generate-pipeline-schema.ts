/**
 * Pipeline Schema Generator
 * 
 * This script extracts Prisma DMMF metadata and generates a JSON Schema
 * for validating YAML pipeline definitions against the Prisma schema.
 * 
 * Core Principle: Prisma schema is the single source of truth.
 * YAML only maps source fields into existing Prisma models.
 */

import { getDMMF } from '@prisma/internals';
import * as fs from 'fs';
import * as path from 'path';

// Prisma scalar types and their allowed transform functions
const SCALAR_TRANSFORMS: Record<string, string[]> = {
  String: ['trim', 'toUpperCase', 'toLowerCase', 'toString', 'substring', 'replace'],
  Int: ['toInt', 'toString', 'parseInt'],
  Float: ['toFloat', 'toInt', 'toString', 'parseFloat'],
  Boolean: ['toBoolean', 'toString'],
  DateTime: ['toDate', 'toISOString', 'toTimestamp'],
  BigInt: ['toBigInt', 'toString'],
  Decimal: ['toDecimal', 'toFloat', 'toString'],
  Json: ['toJson', 'parseJson'],
  Bytes: ['toBytes', 'toBase64'],
};

// Map Prisma types to JSON Schema types
function prismaTypeToJsonSchemaType(prismaType: string): string {
  const typeMap: Record<string, string> = {
    String: 'string',
    Int: 'integer',
    Float: 'number',
    Boolean: 'boolean',
    DateTime: 'string', // ISO 8601
    BigInt: 'integer',
    Decimal: 'number',
    Json: 'object',
    Bytes: 'string',
  };
  return typeMap[prismaType] || 'string';
}

interface DMMFModel {
  name: string;
  fields: DMMFField[];
}

interface DMMFField {
  name: string;
  type: string;
  isRequired: boolean;
  isId: boolean;
  isList: boolean;
  isUnique: boolean;
  default?: any;
  relationName?: string;
}

interface JSONSchemaProperty {
  type?: string;
  description?: string;
  enum?: string[];
  properties?: Record<string, any>;
  required?: string[];
  additionalProperties?: boolean | Record<string, any>;
  $ref?: string;
  items?: JSONSchemaProperty;
  examples?: any[];
  minimum?: number;
  maximum?: number;
  default?: any;
  oneOf?: JSONSchemaProperty[];
}

/**
 * Extract Prisma DMMF from schema.prisma
 */
async function extractPrismaDMMF(schemaPath: string): Promise<{
  models: any[];
  enums: any[];
}> {
  const schemaContent = fs.readFileSync(schemaPath, 'utf-8');
  const datamodel = await getDMMF({
    datamodel: schemaContent,
  });
  
  return {
    models: [...datamodel.datamodel.models],
    enums: [...datamodel.datamodel.enums],
  };
}

/**
 * Generate mapping schema for a specific Prisma field type
 */
function generateMappingSchemaForType(fieldType: string): JSONSchemaProperty {
  const transforms = SCALAR_TRANSFORMS[fieldType] || [];
  
  const mappingSchema: JSONSchemaProperty = {
    type: 'object',
    properties: {
      from: {
        type: 'string',
        description: 'JSONPath or field name to extract from source data',
        examples: ['$.id', '$.price', 'data.name'],
      },
      transform: {
        type: 'string',
        description: `Transform function to apply. Allowed: ${transforms.join(', ') || 'none'}`,
        enum: transforms.length > 0 ? transforms : undefined,
      },
      default: {
        description: 'Default value if source field is missing',
      },
    },
    required: ['from'],
    additionalProperties: false,
  };
  
  // Add type-specific property constraints
  if (fieldType === 'DateTime') {
    mappingSchema.properties!['format'] = {
      type: 'string',
      enum: ['ISO8601', 'unix', 'unix毫秒'],
      description: 'DateTime format hint',
    };
  }
  
  return mappingSchema;
}

/**
 * Generate the complete JSON Schema for pipeline YAML
 */
function generatePipelineSchema(
  models: DMMFModel[],
  enums: any[]
): object {
  // Build enum of model names
  const modelNames = models.map(m => m.name);
  
  // Build mapping definitions for each model's fields
  const mappingDefinitions: Record<string, JSONSchemaProperty> = {};
  const modelPropertyRefs: Record<string, JSONSchemaProperty> = {};
  
  for (const model of models) {
    // Determine required fields (non-optional, non-generated)
    const requiredFields = model.fields
      .filter(f => f.isRequired && !f.default && !f.isId)
      .map(f => f.name);
    
    // Build mapping properties for each field
    const mappingProperties: Record<string, JSONSchemaProperty> = {};
    
    for (const field of model.fields) {
      // Skip relation fields
      if (field.relationName) continue;
      
      // Handle list types
      if (field.isList) {
        mappingProperties[field.name] = {
          type: 'array',
          description: `List mapping for ${model.name}.${field.name} (${field.type})`,
          items: generateMappingSchemaForType(field.type),
        };
      } else {
        mappingProperties[field.name] = generateMappingSchemaForType(field.type);
        mappingProperties[field.name].description = 
          `Mapping for ${model.name}.${field.name} (${field.type}${field.isRequired ? ', required' : ''})`;
      }
    }
    
    // Create schema for this model's mapping
    mappingDefinitions[model.name] = {
      type: 'object',
      description: `Field mappings for ${model.name} model`,
      properties: mappingProperties,
      required: requiredFields,
      additionalProperties: false, // Strict: only allow known Prisma fields
    };
    
    // Build oneOf schema for dynamic model selection
    modelPropertyRefs[model.name] = {
      $ref: `#/definitions/${model.name}Mapping`,
    };
  }
  
  // Build the complete JSON Schema
  const jsonSchema = {
    $schema: 'http://json-schema.org/draft-07/schema#',
    $id: 'https://prisma-yaml-ingestion.framework/pipeline.schema.json',
    title: 'Prisma-Bound Pipeline Definition',
    description: 'Declarative ingestion pipeline YAML validated against Prisma schema',
    type: 'object',
    properties: {
      model: {
        type: 'string',
        description: 'Target Prisma model name',
        enum: modelNames,
      },
      source: {
        type: 'object',
        description: 'Source configuration (API endpoint, file path, etc.)',
        properties: {
          type: {
            type: 'string',
            enum: ['http', 'file', 'stream', 'webhook'],
          },
          url: {
            type: 'string',
            description: 'URL for HTTP/stream sources',
          },
          path: {
            type: 'string',
            description: 'File path for file sources',
          },
          headers: {
            type: 'object',
            description: 'HTTP headers',
            additionalProperties: { type: 'string' },
          },
          method: {
            type: 'string',
            enum: ['GET', 'POST', 'PUT', 'PATCH'],
            default: 'GET',
          },
          body: {
            description: 'Request body for POST/PUT/PATCH',
          },
          jsonPath: {
            type: 'string',
            description: 'JSONPath to extract array from response',
            examples: ['$.data', '$.results[*]', '$.items'],
          },
        },
        required: ['type'],
        additionalProperties: false,
      },
      mapping: {
        oneOf: Object.values(modelPropertyRefs),
        description: 'Field mappings to Prisma model fields',
      },
      options: {
        type: 'object',
        description: 'Ingestion options',
        properties: {
          batchSize: {
            type: 'integer',
            minimum: 1,
            maximum: 10000,
            default: 100,
          },
          retryAttempts: {
            type: 'integer',
            minimum: 0,
            maximum: 10,
            default: 3,
          },
          retryDelay: {
            type: 'integer',
            minimum: 0,
            description: 'Delay in milliseconds',
            default: 1000,
          },
          onConflict: {
            type: 'string',
            enum: ['ignore', 'update', 'error'],
            default: 'ignore',
          },
          transform: {
            type: 'string',
            enum: ['direct', 'bulk', 'upsert'],
            default: 'direct',
          },
        },
        additionalProperties: false,
      },
    },
    required: ['model', 'source', 'mapping'],
    additionalProperties: false, // Strict: no extra top-level properties
    definitions: mappingDefinitions,
  };
  
  return jsonSchema;
}

/**
 * Main execution
 */
async function main() {
  const schemaPath = path.resolve(process.cwd(), 'prisma/schema.prisma');
  const outputPath = path.resolve(process.cwd(), 'generated/pipeline.schema.json');
  
  console.log('🔍 Extracting Prisma DMMF...');
  const { models, enums } = await extractPrismaDMMF(schemaPath);
  
  console.log(`📋 Found ${models.length} models:`);
  for (const model of models) {
    const requiredCount = model.fields.filter((f: any) => f.isRequired && !f.default).length;
    console.log(`   - ${model.name} (${model.fields.length} fields, ${requiredCount} required)`);
  }
  
  console.log('\n🔧 Generating JSON Schema...');
  const jsonSchema = generatePipelineSchema(models, enums) as any;
  
  // Ensure output directory exists
  const outputDir = path.dirname(outputPath);
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }
  
  // Write schema to file
  fs.writeFileSync(outputPath, JSON.stringify(jsonSchema, null, 2));
  
  console.log(`✅ Schema written to: ${outputPath}`);
  console.log('\n📝 Usage in YAML files:');
  console.log(`   # Add this directive at the top of your pipeline YAML:`);
  console.log(`   $schema: ${jsonSchema.$id || 'path/to/generated/pipeline.schema.json'}`);
}

/**
 * Export for programmatic use
 */
export { generatePipelineSchema, extractPrismaDMMF, DMMFModel, DMMFField };

main().catch(console.error);
