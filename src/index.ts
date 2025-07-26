import fetch from "node-fetch";
import { Config, DataStreamPayload, BatchDataStreamPayload } from "./type";
import configJson from "../config.json";
import io from "socket.io-client";
import { Socket } from "socket.io-client";
import { startMqttTestLoop, incrementMqttSocketEventCount } from "./mqtt-client";

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

async function getSensorTokenAndUuid(userToken: string): Promise<{ token: string, deviceUuid: string }> {
  console.log("🔑 Getting sensor token...", config.sensor);
  const response = await fetch(`${BASE_URL}/device-tokens`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${userToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(config.sensor),
  });

  if (!response.ok)
    throw new Error(`Sensor token request failed: ${response.status} ${response.statusText}`);
  const json = await response.json();
  
  console.log("📋 Full device-tokens response:", JSON.stringify(json, null, 2));
  
  const token = json?.data?.token;
  console.log(" Device DeviceUuid:", json.data);
  // Try different possible locations for deviceUuid
  const deviceUuid = json?.data?.deviceUuid || 
                    json?.data?.uuid || 
                    json?.data?.device?.uuid || 
                    json?.data?.device?.deviceUuid ||
                    json?.deviceUuid ||
                    json?.uuid;
                    
  if (!token) {
    console.error("❌ Token missing in sensor token response");
    throw new Error("Sensor token request succeeded but no token found in response.");
  }
  
  if (!deviceUuid) {
    console.error("❌ Device UUID missing in sensor token response");
    console.error("Available fields in response:", Object.keys(json?.data || {}));
    throw new Error("Sensor token request succeeded but no deviceUuid found in response.");
  }
  
  console.log("✅ Received sensor token:", token);
  console.log("✅ Received device UUID:", deviceUuid);
  return { token, deviceUuid };
}

async function postDataStream(sensorToken: string): Promise<void> {
  console.log("🔑 Token for posting data stream:", sensorToken);
  const basePayload = config.datastream;
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

function listenToSocketIO() {
  const socket = io("http://localhost:3000", {
    path: "/socket.io",
    transports: ["websocket", "polling"],
    reconnection: true,
    reconnectionAttempts: 10,
    reconnectionDelay: 1000,
  });

  // Catch-all event listener for debugging
  (socket as any).onAny((event: string, ...args: unknown[]) => {
    console.log(`[SOCKET EVENT] ${event}:`, ...args);
    // Track MQTT-triggered Socket.IO events
    if (event === 'new-datastream' || event === 'datastreams-update') {
      incrementMqttSocketEventCount();
      console.log(`✅ Socket.IO event received for MQTT data: ${event}`);
    }
  });

  socket.on("connect", () => {
    console.log(`🟢 Connected to Socket.IO server (ID: ${socket.id})`);
    // Join the telemetry-1 room for debugging (replace with actual ID if needed)
    socket.emit("join", "telemetry-1");
  });

  socket.on("connect_error", (error: Error) => {
    console.error("❌ Socket.IO connection error:", error);
  });

  socket.on("disconnect", () => {
    console.log("🔴 Disconnected from Socket.IO server");
  });

  socket.on("datastreams-update", (data: unknown) => {
    console.log("📡 [datastreams-update]", data);
    incrementMqttSocketEventCount();
    console.log("✅ Socket.IO datastreams-update event received for MQTT data");
  });

  socket.on("new-datastream", (data: unknown) => {
    console.log("🆕 [new-datastream]", data);
    incrementMqttSocketEventCount();
    console.log("✅ Socket.IO new-datastream event received for MQTT data");
  });

  socket.on("update-datastream", (data: unknown) => {
    console.log("✏️ [update-datastream]", data);
  });

  socket.on("delete-datastream", (data: unknown) => {
    console.log("❌ [delete-datastream]", data);
  });
}

async function main() {
  try {
    listenToSocketIO();
    console.log("🔐 Logging in...");
    const userToken = await login();
    console.log("✅ User token acquired");

    const { token: sensorToken, deviceUuid } = await getSensorTokenAndUuid(userToken);
    console.log("✅ Sensor token acquired");
    console.log("✅ Device UUID acquired");

    const telemetryDataIds = [
      config.datastream.telemetryDataId,
      ...(config.batchDataStreams?.[0]?.dataStreams?.map(ds => ds.telemetryDataId) || [])
    ];

    const testMode = (config as any).testMode || "both";
    if (testMode === "mqtt" || testMode === "both") {
      startMqttTestLoop({
        brokerUrl: "mqtt://localhost:1883",
        deviceUuid,
        deviceToken: sensorToken,
        orgId: undefined // set if you want to test org broadcast
      }, telemetryDataIds);
    }

    if (testMode === "http" || testMode === "both") {
      console.log("🚀 Starting telemetry loop every 10 seconds...");
      while (true) {
        await postDataStream(sensorToken);
        // await postBatchDataStream(sensorToken);
        await new Promise((resolve) => setTimeout(resolve, 10000));
      }
    }
  } catch (err) {
    console.error("❌ Error:", err);
  }
}

main();
