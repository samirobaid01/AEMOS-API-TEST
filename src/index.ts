import fetch from "node-fetch";
import { Config, DataStreamPayload, BatchDataStreamPayload } from "./type";
import configJson from "../config.json";

const config: Config = configJson;
const BASE_URL = "http://localhost:3000/api/v1";

async function login(): Promise<string> {
  const response = await fetch(`${BASE_URL}/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(config.user),
  });

  if (!response.ok) throw new Error(`Login failed: ${response.status}`);

  const json = await response.json();

  const token = json?.data?.token;

  if (!token) {
    console.error(
      "❌ Token missing in login response:",
      JSON.stringify(json, null, 2)
    );
    throw new Error("Login succeeded but no token found in response.");
  }

  console.log("✅ Received user token:", token);
  return token;
}

async function getSensorToken(userToken: string): Promise<string> {
  console.log("🔑 Getting sensor token...");
  const response = await fetch(`${BASE_URL}/device-tokens`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${userToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(config.sensor),
  });

  if (!response.ok)
    throw new Error(`Sensor token request failed: ${response.status}`);
  const json = await response.json();
  const token = json?.data?.token;
  if (!token) {
    console.error(
      "❌ Token missing in sensor token response:",
      JSON.stringify(json, null, 2)
    );
    throw new Error(
      "Sensor token request succeeded but no token found in response."
    );
  }
  console.log("✅ Received sensor token:", token);
  return token;
}

async function postDataStream(sensorToken: string): Promise<void> {
  console.log("🔑 Token for posting data stream:", sensorToken);
  const basePayload = config.datastream;
  // const payload: DataStreamPayload = {
  //   telemetryDataId: basePayload.telemetryDataId,
  //   value: (Math.random() * 100).toFixed(1),
  //   recievedAt: new Date().toISOString(),
  // };
  const allowedValues = [6, 7, 8];
  const payload: DataStreamPayload = {
    telemetryDataId: basePayload.telemetryDataId,
    value: allowedValues[Math.floor(Math.random() * allowedValues.length)].toString(),
    recievedAt: new Date().toISOString(),
  };

  const response = await fetch(`${BASE_URL}/datastreams/token`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${sensorToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(
      `fetch token in postDataStream failed: ${response.status} - ${errorText}`
    );
  }

  const data = await response.json();
  console.log("✅ Datastream submitted:", data);
}

async function postBatchDataStream(sensorToken: string): Promise<void> {
  const basePayload = config.datastream;
  const payload: BatchDataStreamPayload = {
    dataStreams: [
      {
        telemetryDataId:
          config.batchDataStreams[0].dataStreams[0].telemetryDataId,
        value: (Math.random() * 100).toFixed(1),
        recievedAt: new Date().toISOString(),
      },
      {
        telemetryDataId:
          config.batchDataStreams[0].dataStreams[1].telemetryDataId,
        value: (Math.random() * 100).toFixed(1),
        recievedAt: new Date().toISOString(),
      },
      {
        telemetryDataId:
          config.batchDataStreams[0].dataStreams[2].telemetryDataId,
        value: (Math.random() * 100).toFixed(1),
        recievedAt: new Date().toISOString(),
      },
      {
        telemetryDataId:
          config.batchDataStreams[0].dataStreams[3].telemetryDataId,
        value: (Math.random() * 100).toFixed(1),
        recievedAt: new Date().toISOString(),
      },
    ],
  };

  const response = await fetch(`${BASE_URL}/datastreams/batch`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${sensorToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(
      `Datastream post failed (/datastreams/batch): ${response.status} - ${errorText}`
    );
  }

  const data = await response.json();
  console.log("✅ Datastream submitted:", data);
}

async function main() {
  try {
    console.log("🔐 Logging in...");
    const userToken = await login();
    console.log("✅ User token acquired");

    const sensorToken = await getSensorToken(userToken);
    console.log("✅ Sensor token acquired");

    console.log("🚀 Starting telemetry loop every 10 seconds...");
    while (true) {
      await postDataStream(sensorToken);
      // await postBatchDataStream(sensorToken);
      await new Promise((resolve) => setTimeout(resolve, 10000));
    }
  } catch (err) {
    console.error("❌ Error:", err);
  }
}

main();
