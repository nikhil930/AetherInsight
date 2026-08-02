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
  // TODO: add [regex, replacement] pairs here
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
