import { createHash } from 'node:crypto';

/**
 * Each rule is [regex, replacement]. They are applied IN ORDER, so a rule
 * that runs early can consume text a later rule needed intact.
 *
 * The trap: UUIDs and hex strings contain digits. If a plain "any number"
 * rule runs before the UUID rule, then
 *   "550e8400-e29b-41d4-a716-446655440000"
 * becomes
 *   "<*>e<*>-e<*>b-<*>d<*>-a<*>-<*>"
 * instead of a single "<*>". Broad-but-specific patterns must come first;
 * the catch-all number rule must come last.
 *
 * TODO(you): fill in the RULES array. See the request in chat.
 */
const RULES = [
  // UUID first — it contains hex + digits + dashes, so it MUST beat the
  // hex-literal and number rules. Otherwise "550e8400-..." gets shredded.
  [/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, '<*>'],

  // IPv4 next — four dot-separated numbers. Must beat the number rule or
  // "192.168.1.14" becomes "<*>.<*>.<*>.<*>" instead of a single "<*>".
  [/(?:\d{1,3}\.){3}\d{1,3}/g, '<*>'],

  // Hex literal (0xdeadbeef). Must beat the number rule; the leading "0"
  // would otherwise be eaten first, leaving "xdeadbeef" behind.
  [/0x[0-9a-f]+/gi, '<*>'],

  // Double-quoted string — treated as opaque payload. We don't try to
  // preserve its contents; if two logs quote different tokens, they should
  // still cluster together.
  [/"[^"]*"/g, '<*>'],

  // Number catch-all — runs last so the specific rules above can grab
  // their digits first. Handles things like port numbers, durations, ids.
  [/\d+/g, '<*>'],

  // Whitespace collapse — normalizes tabs, double spaces, etc. down to a
  // single space so cosmetic formatting doesn't fork the fingerprint.
  [/\s+/g, ' '],
];

export function normalize(message) {
  let out = message;
  for (const [pattern, replacement] of RULES) {
    out = out.replace(pattern, replacement);
  }
  return out.trim();
}

export function fingerprint(message) {
  return createHash('sha1').update(normalize(message)).digest('hex');
}
