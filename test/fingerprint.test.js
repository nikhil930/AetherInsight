import { describe, it, expect } from 'vitest';
import { normalize, fingerprint } from '../src/shared/fingerprint.js';

const cases = [
  ['Connection to db-7 timed out after 3021ms for user 4471',
   'Connection to db-<*> timed out after <*>ms for user <*>'],
  ['Request 550e8400-e29b-41d4-a716-446655440000 failed',
   'Request <*> failed'],
  ['Cannot reach 192.168.1.14 on port 5432',
   'Cannot reach <*> on port <*>'],
  ['Invalid token "abc123xyz" supplied',
   'Invalid token <*> supplied'],
  ['Segfault at 0xdeadbeef',
   'Segfault at <*>'],
];

describe('normalize', () => {
  it.each(cases)('normalizes %s', (input, expected) => {
    expect(normalize(input)).toBe(expected);
  });

  it('collapses repeated whitespace', () => {
    expect(normalize('too    many\tspaces')).toBe('too many spaces');
  });
});

describe('fingerprint', () => {
  it('returns a 40-char sha1 hex string', () => {
    expect(fingerprint('anything')).toMatch(/^[0-9a-f]{40}$/);
  });

  it('gives the same fingerprint to structurally identical messages', () => {
    const a = fingerprint('Timeout after 30ms for user 1');
    const b = fingerprint('Timeout after 9999ms for user 88888');
    expect(a).toBe(b);
  });

  it('gives different fingerprints to structurally different messages', () => {
    expect(fingerprint('Timeout after 30ms')).not.toBe(fingerprint('Refused after 30ms'));
  });
});
