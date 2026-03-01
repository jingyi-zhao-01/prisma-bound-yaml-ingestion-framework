/**
 * Doctor Command - Validate YAML pipelines against Prisma schema
 * 
 * This script validates all pipeline YAML files against the generated JSON Schema
 * to ensure they conform to the Prisma schema at authoring time.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as yaml from 'js-yaml';
import Ajv from 'ajv';

interface ValidationError {
  file: string;
  errors: Array<{
    path: string;
    message: string;
  }>;
}

interface JSONSchemaProperty {
  type?: string;
  properties?: Record<string, any>;
  required?: string[];
  additionalProperties?: boolean | Record<string, any>;
  $ref?: string;
  oneOf?: JSONSchemaProperty[];
  enum?: string[];
  items?: JSONSchemaProperty;
  minimum?: number;
  maximum?: number;
}

/**
 * Check if a model exists in the schema
 */
function validateModelExists(schema: any, modelName: string): string | null {
  const modelEnum = schema.properties?.model?.enum;
  if (!modelEnum) return 'Model enum not found in schema';
  if (!modelEnum.includes(modelName)) {
    return `Unknown model "${modelName}". Valid models: ${modelEnum.join(', ')}`;
  }
  return null;
}

/**
 * Validate mapping fields against the model definition
 */
function validateMappingFields(
  schema: any,
  modelName: string,
  mapping: Record<string, any>
): string[] {
  const errors: string[] = [];
  
  // Find the model definition in oneOf
  const definitions = schema.definitions;
  const modelDef = definitions?.[modelName];
  
  if (!modelDef) {
    errors.push(`Model "${modelName}" definition not found`);
    return errors;
  }
  
  const modelProperties = modelDef.properties || {};
  const requiredFields = modelDef.required || [];
  
  // Check for required fields
  for (const requiredField of requiredFields) {
    if (!mapping[requiredField]) {
      errors.push(`Missing required mapping for field "${requiredField}"`);
    }
  }
  
  // Check each mapped field
  for (const [fieldName, fieldMapping] of Object.entries(mapping)) {
    // Check if field exists in model
    if (!modelProperties[fieldName]) {
      errors.push(`Unknown field "${fieldName}" - not in ${modelName} model`);
      continue;
    }
    
    // Check transform compatibility
    const fieldSchema = modelProperties[fieldName];
    if (fieldMapping.transform && fieldSchema.properties?.transform) {
      const allowedTransforms = fieldSchema.properties.transform.enum;
      if (allowedTransforms && !allowedTransforms.includes(fieldMapping.transform)) {
        errors.push(
          `Invalid transform "${fieldMapping.transform}" for field "${fieldName}". Allowed: ${allowedTransforms.join(', ')}`
        );
      }
    }
    
    // Check for extra fields (additionalProperties: false)
    if (modelDef.additionalProperties === false && fieldMapping.from === undefined) {
      // This is handled elsewhere
    }
  }
  
  return errors;
}

/**
 * Validate a single YAML file
 */
function validateYamlFile(
  filePath: string,
  schema: any
): ValidationError {
  const errors: Array<{ path: string; message: string }> = [];
  
  try {
    const content = fs.readFileSync(filePath, 'utf-8');
    const pipeline = yaml.load(content) as any;
    
    if (!pipeline) {
      errors.push({ path: filePath, message: 'Empty YAML file' });
      return { file: filePath, errors };
    }
    
    // Validate model exists
    if (pipeline.model) {
      const modelError = validateModelExists(schema, pipeline.model);
      if (modelError) {
        errors.push({ path: 'model', message: modelError });
      }
    } else {
      errors.push({ path: 'model', message: 'Missing required "model" field' });
    }
    
    // Validate source
    if (!pipeline.source) {
      errors.push({ path: 'source', message: 'Missing required "source" field' });
    } else if (!pipeline.source.type) {
      errors.push({ path: 'source.type', message: 'Missing required "source.type" field' });
    }
    
    // Validate mapping
    if (!pipeline.mapping) {
      errors.push({ path: 'mapping', message: 'Missing required "mapping" field' });
    } else if (pipeline.model) {
      const mappingErrors = validateMappingFields(schema, pipeline.model, pipeline.mapping);
      errors.push(...mappingErrors.map(m => ({ path: `mapping.${m}`, message: m })));
    }
    
    // Validate options
    if (pipeline.options) {
      if (pipeline.options.batchSize !== undefined) {
        if (typeof pipeline.options.batchSize !== 'number' || pipeline.options.batchSize < 1) {
          errors.push({ path: 'options.batchSize', message: 'batchSize must be a positive number' });
        }
      }
    }
    
  } catch (err: any) {
    errors.push({ path: filePath, message: `YAML parse error: ${err.message}` });
  }
  
  return { file: filePath, errors };
}

/**
 * Main validation function
 */
async function main() {
  console.log('🔍 Running pipeline doctor...\n');
  
  const pipelineDir = path.resolve(process.cwd(), 'pipelines');
  const schemaPath = path.resolve(process.cwd(), 'generated/pipeline.schema.json');
  
  // Check if schema exists
  if (!fs.existsSync(schemaPath)) {
    console.error('❌ Error: pipeline.schema.json not found.');
    console.error('   Run "pnpm run generate-schema" first to generate the schema.\n');
    process.exit(1);
  }
  
  // Load schema
  const schemaContent = fs.readFileSync(schemaPath, 'utf-8');
  const schema = JSON.parse(schemaContent);
  
  // Find all YAML files
  const yamlFiles = fs.readdirSync(pipelineDir)
    .filter(f => f.endsWith('.yaml') || f.endsWith('.yml'));
  
  if (yamlFiles.length === 0) {
    console.log('📁 No pipeline YAML files found in pipelines/');
    process.exit(0);
  }
  
  console.log(`📁 Found ${yamlFiles.length} pipeline file(s):`);
  yamlFiles.forEach(f => console.log(`   - ${f}`));
  console.log('');
  
  let totalErrors = 0;
  let filesWithErrors = 0;
  
  // Validate each file
  for (const yamlFile of yamlFiles) {
    const filePath = path.join(pipelineDir, yamlFile);
    const result = validateYamlFile(filePath, schema);
    
    if (result.errors.length > 0) {
      filesWithErrors++;
      totalErrors += result.errors.length;
      console.log(`❌ ${yamlFile}:`);
      result.errors.forEach(err => {
        console.log(`   ${err.path}: ${err.message}`);
      });
      console.log('');
    } else {
      console.log(`✅ ${yamlFile} - Valid!`);
    }
  }
  
  console.log('─'.repeat(50));
  
  if (totalErrors > 0) {
    console.log(`\n❌ Found ${totalErrors} error(s) in ${filesWithErrors} file(s)\n`);
    process.exit(1);
  } else {
    console.log(`\n✅ All ${yamlFiles.length} pipeline file(s) are valid!\n`);
    process.exit(0);
  }
}

main().catch(console.error);
