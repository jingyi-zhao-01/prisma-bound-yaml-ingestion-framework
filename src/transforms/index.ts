/**
 * Transform Functions Registry
 * 
 * Type-scoped transform functions that can be used in YAML pipeline mappings.
 * Each transform is constrained by the Prisma field type via JSON Schema.
 */

export type TransformFunction = (value: any, options?: any) => any;

// String transforms
export const StringTransforms: Record<string, TransformFunction> = {
  trim: (value: any) => {
    if (value == null) return value;
    return String(value).trim();
  },
  toUpperCase: (value: any) => {
    if (value == null) return value;
    return String(value).toUpperCase();
  },
  toLowerCase: (value: any) => {
    if (value == null) return value;
    return String(value).toLowerCase();
  },
  toString: (value: any) => {
    if (value == null) return value;
    return String(value);
  },
  substring: (value: any, options?: { start?: number; end?: number }) => {
    if (value == null) return value;
    const str = String(value);
    return options?.start !== undefined 
      ? str.substring(options.start, options.end)
      : str;
  },
  replace: (value: any, options?: { pattern: string | RegExp; replacement: string }) => {
    if (value == null) return value;
    if (!options?.pattern) return value;
    return String(value).replace(options.pattern, options.replacement || '');
  },
};

// Number transforms (Int, Float)
export const NumberTransforms: Record<string, TransformFunction> = {
  toInt: (value: any) => {
    if (value == null) return value;
    const num = Number(value);
    return Number.isInteger(num) ? num : Math.floor(num);
  },
  toFloat: (value: any) => {
    if (value == null) return value;
    return Number(value);
  },
  parseInt: (value: any, radix?: number) => {
    if (value == null) return value;
    const result = parseInt(String(value), radix || 10);
    return isNaN(result) ? value : result;
  },
  parseFloat: (value: any) => {
    if (value == null) return value;
    const result = parseFloat(String(value));
    return isNaN(result) ? value : result;
  },
};

// Boolean transforms
export const BooleanTransforms: Record<string, TransformFunction> = {
  toBoolean: (value: any) => {
    if (value == null) return value;
    if (typeof value === 'boolean') return value;
    const str = String(value).toLowerCase();
    if (str === 'true' || str === '1' || str === 'yes') return true;
    if (str === 'false' || str === '0' || str === 'no') return false;
    return value;
  },
};

// DateTime transforms
export const DateTimeTransforms: Record<string, TransformFunction> = {
  toDate: (value: any, format?: string) => {
    if (value == null) return value;
    
    // Handle Unix timestamp (seconds)
    if (format === 'unix' && typeof value === 'number') {
      return new Date(value * 1000);
    }
    
    // Handle Unix timestamp in milliseconds
    if (format === 'unix毫秒' && typeof value === 'number') {
      return new Date(value);
    }
    
    // Handle ISO string or date string
    const date = new Date(value);
    return isNaN(date.getTime()) ? value : date;
  },
  toISOString: (value: any) => {
    if (value == null) return value;
    const date = value instanceof Date ? value : new Date(value);
    return isNaN(date.getTime()) ? value : date.toISOString();
  },
  toTimestamp: (value: any) => {
    if (value == null) return value;
    const date = value instanceof Date ? value : new Date(value);
    return isNaN(date.getTime()) ? value : date.getTime();
  },
};

// BigInt transforms
export const BigIntTransforms: Record<string, TransformFunction> = {
  toBigInt: (value: any) => {
    if (value == null) return value;
    try {
      return BigInt(value);
    } catch {
      return value;
    }
  },
};

// JSON transforms
export const JsonTransforms: Record<string, TransformFunction> = {
  toJson: (value: any) => {
    if (value == null) return value;
    if (typeof value === 'string') return value;
    return JSON.stringify(value);
  },
  parseJson: (value: any) => {
    if (value == null) return value;
    if (typeof value !== 'string') return value;
    try {
      return JSON.parse(value);
    } catch {
      return value;
    }
  },
};

// Combined transforms registry by Prisma type
export const TransformRegistry: Record<string, Record<string, TransformFunction>> = {
  String: StringTransforms,
  Int: NumberTransforms,
  Float: NumberTransforms,
  Boolean: BooleanTransforms,
  DateTime: DateTimeTransforms,
  BigInt: BigIntTransforms,
  Decimal: NumberTransforms,
  Json: JsonTransforms,
  Bytes: {
    toBytes: (value: any) => Buffer.from(value),
    toBase64: (value: any) => Buffer.from(value).toString('base64'),
  },
};

/**
 * Apply a single transform to a value
 */
export function applyTransform(
  value: any,
  transformName: string,
  prismaType: string,
  options?: any
): any {
  const transforms = TransformRegistry[prismaType];
  if (!transforms) {
    console.warn(`No transforms defined for Prisma type: ${prismaType}`);
    return value;
  }
  
  const transform = transforms[transformName];
  if (!transform) {
    console.warn(`Transform "${transformName}" not found for type: ${prismaType}`);
    return value;
  }
  
  return transform(value, options);
}

/**
 * Apply multiple transforms in sequence
 */
export function applyTransforms(
  value: any,
  transforms: Array<{ name: string; options?: any }>,
  prismaType: string
): any {
  let result = value;
  for (const { name, options } of transforms) {
    result = applyTransform(result, name, prismaType, options);
  }
  return result;
}
