import mqtt, { MqttClient } from 'mqtt';
import { DataStreamPayload, BatchDataStreamPayload } from './type';

export interface MqttTestOptions {
  brokerUrl: string;
  deviceUuid: string;
  deviceToken: string;
  orgId?: string;
}

// Track if Socket.IO events are received for MQTT data
let mqttSocketEventsReceived = 0;
let lastMqttPublishTime = 0;

export function getMqttSocketEventCount(): number {
  return mqttSocketEventsReceived;
}

export function resetMqttSocketEventCount(): void {
  mqttSocketEventsReceived = 0;
}

export function incrementMqttSocketEventCount(): void {
  mqttSocketEventsReceived++;
}

function getRandomValue(): string {
  const allowedValues = [6, 7, 8];
  return allowedValues[Math.floor(Math.random() * allowedValues.length)].toString();
}

function getSinglePayload(telemetryDataId: number, deviceToken: string): DataStreamPayload & { token?: string } {
  return {
    telemetryDataId,
    value: getRandomValue(),
    recievedAt: new Date().toISOString(),
    token: deviceToken, // Include token in payload for device authentication
  };
}

function getBatchPayload(telemetryDataIds: number[], deviceToken: string): BatchDataStreamPayload & { token?: string } {
  return {
    dataStreams: telemetryDataIds.map(id => ({
      telemetryDataId: id,
      value: getRandomValue(),
      recievedAt: new Date().toISOString(),
    })),
    token: deviceToken, // Include token in payload for device authentication
  };
}

export function startMqttTestLoop(options: MqttTestOptions, telemetryDataIds: number[]): void {
  const { brokerUrl, deviceUuid, deviceToken, orgId } = options;
  const clientId = deviceUuid;
  const topicBase = `devices/${deviceUuid}`;
  const publishTopic = `${topicBase}/datastream`;
  const subscribeTopics = [`${topicBase}/#`];
  if (orgId) subscribeTopics.push(`organizations/${orgId}/broadcast`);

  console.log(`🔌 Connecting to MQTT broker: ${brokerUrl}`);
  console.log(`🔑 Using device UUID: ${deviceUuid}`);
  console.log(`🔑 Using device token: ${deviceToken}`);

  const client: MqttClient = mqtt.connect(brokerUrl, {
    clientId,
    username: deviceUuid,
    password: deviceToken,
    clean: true,
    connectTimeout: 4000,
    reconnectPeriod: 1000,
    keepalive: 60,
  });

  client.on('connect', () => {
    console.log(`🟢 MQTT connected as ${clientId}`);
    subscribeTopics.forEach(topic => {
      client.subscribe(topic, { qos: 1 }, (err: unknown) => {
        if (err) console.error(`❌ MQTT subscribe error for ${topic}:`, err);
        else console.log(`📡 MQTT subscribed to ${topic}`);
      });
    });
  });

  client.on('error', (err: unknown) => {
    console.error('❌ MQTT error:', err);
  });

  client.on('close', () => {
    console.log('🔴 MQTT connection closed');
  });

  client.on('reconnect', () => {
    console.log('🔄 MQTT reconnecting...');
  });

  client.on('message', (topic: string, message: Buffer) => {
    console.log(`📥 MQTT message on ${topic}:`, message.toString());
  });

  // MQTT test loop - similar to HTTP tests
  let count = 0;
  setInterval(() => {
    const qos = count % 3 as 0 | 1 | 2;
    lastMqttPublishTime = Date.now();
    
    if (count % 2 === 0) {
      // Single datastream (like HTTP postDataStream)
      const payload = getSinglePayload(telemetryDataIds[0], deviceToken);
      client.publish(publishTopic, JSON.stringify(payload), { qos }, (err: unknown) => {
        if (err) console.error('❌ MQTT publish error (single):', err);
        else {
          console.log(`🚀 MQTT published single datastream (QoS ${qos}):`, payload);
          console.log(`🔍 Waiting for Socket.IO event for MQTT single datastream...`);
          
          // Check for Socket.IO event after 2 seconds
          setTimeout(() => {
            const timeSincePublish = Date.now() - lastMqttPublishTime;
            console.log(`⏱️ Time since MQTT publish: ${timeSincePublish}ms`);
            console.log(`📊 MQTT Socket.IO events received so far: ${mqttSocketEventsReceived}`);
          }, 2000);
        }
      });
    } else {
      // Batch datastream (like HTTP postBatchDataStream)
      const payload = getBatchPayload(telemetryDataIds, deviceToken);
      client.publish(publishTopic, JSON.stringify(payload), { qos }, (err: unknown) => {
        if (err) console.error('❌ MQTT publish error (batch):', err);
        else {
          console.log(`🚀 MQTT published batch datastream (QoS ${qos}):`, payload);
          console.log(`🔍 Waiting for Socket.IO event for MQTT batch datastream...`);
          
          // Check for Socket.IO event after 2 seconds
          setTimeout(() => {
            const timeSincePublish = Date.now() - lastMqttPublishTime;
            console.log(`⏱️ Time since MQTT publish: ${timeSincePublish}ms`);
            console.log(`📊 MQTT Socket.IO events received so far: ${mqttSocketEventsReceived}`);
          }, 2000);
        }
      });
    }
    count++;
  }, 10000);
} 