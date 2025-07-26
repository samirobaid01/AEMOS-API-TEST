const mqtt = require('mqtt');

console.log('Testing MQTT broker connectivity without authentication...');

const client = mqtt.connect('mqtt://localhost:1883', {
  clientId: 'test-client-' + Math.random().toString(36).substr(2, 9),
  clean: true,
  connectTimeout: 4000,
});

client.on('connect', () => {
  console.log('✅ MQTT broker is reachable (no auth required)');
  client.end();
});

client.on('error', (err) => {
  console.error('❌ MQTT Error:', err.message);
});

client.on('close', () => {
  console.log('🔌 MQTT Connection closed');
}); 