import assert from "node:assert/strict";
import { collect } from "./worker.mjs";

const saved = [];
const env = {
  OTD_API_KEY: "test-key",
  OTD_BASE_URL: "https://otd.delhi.gov.in",
  SNAPSHOTS: { put: async (...args) => saved.push(args) },
};
const collectedAt = new Date("2026-07-30T15:19:32.000Z");
const fetcher = async (url) => {
  assert.equal(url.pathname, "/api/realtime/VehiclePositions.pb");
  assert.equal(url.searchParams.get("key"), "test-key");
  return new Response(new Uint8Array([1, 2, 3]));
};

const result = await collect(env, collectedAt, fetcher);
assert.deepEqual(result, {
  key: "vehicle_positions/2026-07-30/2026-07-30T15-19-32.000Z.pb",
  bytes: 3,
});
assert.equal(saved.length, 1);
assert.equal(saved[0][2].customMetadata.collection_timestamp, collectedAt.toISOString());
await assert.rejects(
  collect(env, collectedAt, async () => new Response(null, { status: 503 })),
  /HTTP 503/,
);
let attempts = 0;
await collect(env, collectedAt, async () => {
  if (++attempts === 1) throw new Error("network error");
  return new Response(new Uint8Array([1]));
});
assert.equal(attempts, 2);
console.log("worker test passed");
