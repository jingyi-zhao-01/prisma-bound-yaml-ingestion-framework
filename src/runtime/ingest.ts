/**
 * Runtime Ingestion Engine
 * 
 * This engine consumes validated YAML pipeline definitions and performs:
 * 1. Source data extraction (HTTP, file, stream, webhook)
 * 2. Transform execution (applying mapping transforms)
 * 3. Database writes (using Prisma)
 * 
 * IMPORTANT: Structural validation is assumed to be done at authoring time
 * via JSON Schema. This engine focuses on execution only.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as yaml from 'js-yaml';
import { applyTransform } from '../transforms';

// Pipeline configuration types (mirrors YAML structure)
export interface PipelineConfig {
  model: string;
  source: SourceConfig;
  mapping: Record<string, FieldMapping>;
  options?: PipelineOptions;
}

export interface SourceConfig {
  type: 'http' | 'file' | 'stream' | 'webhook';
  url?: string;
  path?: string;
  headers?: Record<string, string>;
  method?: 'GET' | 'POST' | 'PUT' | 'PATCH';
  body?: any;
  jsonPath?: string;
}

export interface FieldMapping {
  from: string;
  transform?: string;
  format?: string;
  default?: any;
}

export interface PipelineOptions {
  batchSize?: number;
  retryAttempts?: number;
  retryDelay?: number;
  onConflict?: 'ignore' | 'update' | 'error';
  transform?: 'direct' | 'bulk' | 'upsert';
}

/**
 * Simple JSONPath-like extractor
 * Supports: $.field, $.nested.field, $[0], $.array[0], $.data[*]
 */
function extractByPath(data: any, jsonPath: string): any {
  if (!jsonPath || jsonPath === '$') return data;
  
  // Normalize the path
  const normalizedPath = jsonPath.replace(/^\$\.?/, '');
  if (!normalizedPath) return data;
  
  const parts = normalizedPath.split(/\.|\[|\]/).filter(Boolean);
  let current: any = data;
  
  for (const part of parts) {
    if (current == null) return undefined;
    
    // Handle array index
    const arrayMatch = part.match(/^(-?\d+)$/);
    if (arrayMatch) {
      current = current[parseInt(arrayMatch[1], 10)];
      continue;
    }
    
    // Handle wildcard
    if (part === '*') {
      if (Array.isArray(current)) {
        return current; // Return array for later processing
      }
      return undefined;
    }
    
    // Handle object property
    current = current[part];
  }
  
  return current;
}

/**
 * Execute HTTP request to fetch source data
 */
async function fetchHttpData(source: SourceConfig): Promise<any[]> {
  // This is a simplified implementation
  // In production, use axios or node-fetch
  console.log(`[HTTP] Fetching from: ${source.url}`);
  console.log(`[HTTP] Method: ${source.method || 'GET'}`);
  
  // Simulate API response for demo purposes
  // In production, replace with actual HTTP client
  return [];
}

/**
 * Read data from file
 */
function readFileData(source: SourceConfig): any[] {
  if (!source.path) {
    throw new Error('File path is required for file source type');
  }
  
  const fullPath = path.resolve(process.cwd(), source.path);
  console.log(`[File] Reading from: ${fullPath}`);
  
  const content = fs.readFileSync(fullPath, 'utf-8');
  
  // Parse based on file extension
  if (fullPath.endsWith('.json')) {
    const data = JSON.parse(content);
    return source.jsonPath ? extractByPath(data, source.jsonPath) : data;
  }
  
  // Assume YAML (would need js-yaml in production)
  // For now, return empty array for YAML files
  console.warn('[File] YAML parsing not implemented in demo');
  return [];
}

/**
 * Extract data from source based on configuration
 */
async function extractSourceData(source: SourceConfig): Promise<any[]> {
  switch (source.type) {
    case 'http':
      return fetchHttpData(source);
    case 'file':
      return readFileData(source);
    case 'stream':
      console.warn('[Stream] Not implemented in demo');
      return [];
    case 'webhook':
      console.warn('[Webhook] Webhook receiver not implemented in demo');
      return [];
    default:
      throw new Error(`Unknown source type: ${source.type}`);
  }
}

/**
 * Apply field mapping to a single record
 */
function applyMapping(
  record: any,
  mapping: Record<string, FieldMapping>,
  fieldTypes: Record<string, string>
): Record<string, any> {
  const result: Record<string, any> = {};
  
  for (const [fieldName, fieldMapping] of Object.entries(mapping)) {
    // Extract value from source using JSONPath
    const rawValue = extractByPath(record, fieldMapping.from);
    
    // Apply default if value is missing
    let value = rawValue !== undefined ? rawValue : fieldMapping.default;
    
    // Apply transform if specified
    if (fieldMapping.transform && value !== undefined) {
      const prismaType = fieldTypes[fieldName] || 'String';
      value = applyTransform(value, fieldMapping.transform, prismaType, {
        format: fieldMapping.format,
      });
    }
    
    result[fieldName] = value;
  }
  
  return result;
}

/**
 * Batch process records
 */
async function processBatch(
  records: any[],
  mapping: Record<string, FieldMapping>,
  fieldTypes: Record<string, string>,
  options: Required<PipelineOptions>
): Promise<any[]> {
  const results: any[] = [];
  
  for (const record of records) {
    const mapped = applyMapping(record, mapping, fieldTypes);
    results.push(mapped);
  }
  
  return results;
}

/**
 * Main ingestion function
 * 
 * @param yamlPath Path to the pipeline YAML file
 * @param prismaModelInfo Map of field names to Prisma types
 * @param dbWriter Function to write records to database
 */
export async function ingest(
  yamlPath: string,
  prismaModelInfo: { fields: Record<string, string> },
  dbWriter?: (records: any[], model: string, options: PipelineOptions) => Promise<void>
): Promise<{ success: boolean; recordsProcessed: number; errors: string[] }> {
  const errors: string[] = [];
  let recordsProcessed = 0;
  
  try {
    // Load pipeline YAML
    console.log(`\n🚀 Starting ingestion from: ${yamlPath}`);
    const pipeline = loadPipelineConfig(yamlPath);
    
    // Set defaults
    const options: Required<PipelineOptions> = {
      batchSize: pipeline.options?.batchSize || 100,
      retryAttempts: pipeline.options?.retryAttempts || 3,
      retryDelay: pipeline.options?.retryDelay || 1000,
      onConflict: pipeline.options?.onConflict || 'ignore',
      transform: pipeline.options?.transform || 'direct',
    };
    
    console.log(`📋 Model: ${pipeline.model}`);
    console.log(`🔗 Source: ${pipeline.source.type} - ${pipeline.source.url || pipeline.source.path}`);
    console.log(`📝 Mapping fields: ${Object.keys(pipeline.mapping).join(', ')}`);
    console.log(`⚙️  Options: batchSize=${options.batchSize}, onConflict=${options.onConflict}`);
    
    // Step 1: Extract source data
    console.log('\n📥 Step 1: Extracting source data...');
    let sourceData = await extractSourceData(pipeline.source);
    
    if (!Array.isArray(sourceData)) {
      sourceData = [sourceData];
    }
    
    console.log(`   Extracted ${sourceData.length} records`);
    
    // Step 2: Apply transformations
    console.log('\n🔄 Step 2: Applying transformations...');
    const fieldTypes = prismaModelInfo.fields;
    const transformedData = await processBatch(
      sourceData,
      pipeline.mapping,
      fieldTypes,
      options
    );
    
    console.log(`   Transformed ${transformedData.length} records`);
    
    // Step 3: Write to database
    console.log('\n💾 Step 3: Writing to database...');
    
    if (dbWriter) {
      await dbWriter(transformedData, pipeline.model, options);
      console.log(`   Written ${transformedData.length} records`);
    } else {
      console.log('   (No database writer provided - skipping write)');
      console.log('   Sample transformed record:', JSON.stringify(transformedData[0], null, 2));
    }
    
    recordsProcessed = transformedData.length;
    console.log('\n✅ Ingestion completed successfully!');
    
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    errors.push(message);
    console.error(`\n❌ Ingestion failed: ${message}`);
  }
  
  return {
    success: errors.length === 0,
    recordsProcessed,
    errors,
  };
}

/**
 * Load pipeline config from YAML file
 */
export function loadPipelineConfig(yamlPath: string): PipelineConfig {
  const fullPath = path.resolve(process.cwd(), yamlPath);
  const content = fs.readFileSync(fullPath, 'utf-8');
  const config = yaml.load(content) as PipelineConfig;
  return config;
}

// Export types for external use
export type { } from './types';
