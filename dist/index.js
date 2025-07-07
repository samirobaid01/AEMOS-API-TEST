"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_fetch_1 = __importDefault(require("node-fetch"));
const config_json_1 = __importDefault(require("../config.json"));
const config = config_json_1.default;
const BASE_URL = "http://localhost:3000/api/v1";
async function login() {
    var _a;
    const response = await (0, node_fetch_1.default)(`${BASE_URL}/auth/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(config.user),
    });
    if (!response.ok)
        throw new Error(`Login failed: ${response.status}`);
    const json = await response.json();
    const token = (_a = json === null || json === void 0 ? void 0 : json.data) === null || _a === void 0 ? void 0 : _a.token;
    if (!token) {
        console.error("❌ Token missing in login response:", JSON.stringify(json, null, 2));
        throw new Error("Login succeeded but no token found in response.");
    }
    console.log("✅ Received user token:", token);
    return token;
}
async function getSensorToken(userToken) {
    var _a;
    console.log("🔑 Getting sensor token...");
    const response = await (0, node_fetch_1.default)(`${BASE_URL}/device-tokens`, {
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
    const token = (_a = json === null || json === void 0 ? void 0 : json.data) === null || _a === void 0 ? void 0 : _a.token;
    if (!token) {
        console.error("❌ Token missing in sensor token response:", JSON.stringify(json, null, 2));
        throw new Error("Sensor token request succeeded but no token found in response.");
    }
    console.log("✅ Received sensor token:", token);
    return token;
}
async function postDataStream(sensorToken) {
    console.log("🔑 Token for posting data stream:", sensorToken);
    const basePayload = config.datastream;
    // const payload: DataStreamPayload = {
    //   telemetryDataId: basePayload.telemetryDataId,
    //   value: (Math.random() * 100).toFixed(1),
    //   recievedAt: new Date().toISOString(),
    // };
    const allowedValues = [6, 7, 8];
    const payload = {
        telemetryDataId: basePayload.telemetryDataId,
        value: allowedValues[Math.floor(Math.random() * allowedValues.length)].toString(),
        recievedAt: new Date().toISOString(),
    };
    const response = await (0, node_fetch_1.default)(`${BASE_URL}/datastreams/token`, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${sensorToken}`,
            "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
    });
    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`fetch token in postDataStream failed: ${response.status} - ${errorText}`);
    }
    const data = await response.json();
    console.log("✅ Datastream submitted:", data);
}
async function postBatchDataStream(sensorToken) {
    const basePayload = config.datastream;
    const payload = {
        dataStreams: [
            {
                telemetryDataId: config.batchDataStreams[0].dataStreams[0].telemetryDataId,
                value: (Math.random() * 100).toFixed(1),
                recievedAt: new Date().toISOString(),
            },
            {
                telemetryDataId: config.batchDataStreams[0].dataStreams[1].telemetryDataId,
                value: (Math.random() * 100).toFixed(1),
                recievedAt: new Date().toISOString(),
            },
            {
                telemetryDataId: config.batchDataStreams[0].dataStreams[2].telemetryDataId,
                value: (Math.random() * 100).toFixed(1),
                recievedAt: new Date().toISOString(),
            },
            {
                telemetryDataId: config.batchDataStreams[0].dataStreams[3].telemetryDataId,
                value: (Math.random() * 100).toFixed(1),
                recievedAt: new Date().toISOString(),
            },
        ],
    };
    const response = await (0, node_fetch_1.default)(`${BASE_URL}/datastreams/batch`, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${sensorToken}`,
            "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
    });
    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Datastream post failed (/datastreams/batch): ${response.status} - ${errorText}`);
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
    }
    catch (err) {
        console.error("❌ Error:", err);
    }
}
main();
