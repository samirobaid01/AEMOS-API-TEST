const mqtt = require('mqtt');

// Use the exact values from your logs
const deviceUuid = '5374f780-32fa-11f0-ad04-70f787be2478';
const deviceToken = '6e3ce5758eab0f110e6d153bbc900bcb7cbc6b484bc38a354972ac470f80c273';

console.log('Testing MQTT connection with:');
console.log('Device UUID:', deviceUuid);
console.log('Device Token:', deviceToken);

const client = mqtt.connect('mqtt://localhost:1883', {
  clientId: deviceUuid,
  username: deviceUuid,
  password: deviceToken,
  clean: true,
  connectTimeout: 4000,
  reconnectPeriod: 1000,
  keepalive: 60,
});

client.on('connect', () => {
  console.log('✅ MQTT Connected successfully!');
  client.end();
});

client.on('error', (err) => {
  console.error('❌ MQTT Error:', err.message);
  console.error('Error details:', err);
});

client.on('close', () => {
  console.log('🔌 MQTT Connection closed');
}); 