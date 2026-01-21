export function normalizeConfidence(score) {
  return Math.min(Math.max(score, 0), 1);
}
