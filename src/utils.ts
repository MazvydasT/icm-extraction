import { from } from 'ix/iterable';
import { map } from 'ix/iterable/operators';

export function parseIntClamp(
  value: string,
  options?: { radix?: number; min?: number; max?: number }
) {
  const parsed = parseInt(value, options?.radix);

  return clamp(parsed, options?.min ?? parsed, options?.max ?? parsed);
}

export function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}

// https://stackoverflow.com/a/175787/2358659
export function isNumeric(value: any) {
  if (typeof value != `string`) return false; // we only process strings!
  return (
    /^-?\d+\.?\d*$/.test(value.trim()) && // check if value consists only of digits, an optional single dot and an optional minus sign
    !isNaN(value as any) && // use type coercion to parse the _entirety_ of the string (`parseFloat` alone does not do this)...
    !isNaN(parseFloat(value)) // ...and ensure strings of whitespace fail
  );
}

export function getAdditionalProperties(obj: Record<string, unknown>) {
  return from(Object.entries<any>(obj)).pipe(map(([key, value]) => `${key}: ${value}`));
}
