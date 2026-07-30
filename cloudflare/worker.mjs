export async function collect(env, collectedAt, fetcher = fetch) {
  if (!env.OTD_API_KEY) throw new Error("OTD_API_KEY is missing");

  const url = new URL(
    "/api/realtime/VehiclePositions.pb",
    env.OTD_BASE_URL,
  );
  url.searchParams.set("key", env.OTD_API_KEY);

  const options = { headers: { Accept: "application/x-protobuf" } };
  let response;
  try {
    response = await fetcher(url, options);
  } catch {
    response = await fetcher(url, options);
  }
  if (!response.ok) throw new Error(`OTD returned HTTP ${response.status}`);

  const content = await response.arrayBuffer();
  if (!content.byteLength) throw new Error("OTD returned an empty response");

  const timestamp = collectedAt.toISOString();
  const key = `vehicle_positions/${timestamp.slice(0, 10)}/${timestamp.replaceAll(":", "-")}.pb`;
  await env.SNAPSHOTS.put(key, content, {
    httpMetadata: { contentType: "application/x-protobuf" },
    customMetadata: { collection_timestamp: timestamp },
  });
  return { key, bytes: content.byteLength };
}

export default {
  async scheduled(controller, env) {
    console.log(
      JSON.stringify(await collect(env, new Date(controller.scheduledTime))),
    );
  },
};
