iot-sensor-client/
├── config.json            # Configuration (user, sensor, telemetry)
├── package.json
├── tsconfig.json
└── src/
    ├── index.ts           # Main script logic
    └── types.ts           # TypeScript interfaces

🛠️ Setup Instructions
1. Clone or Create Directory
mkdir iot-sensor-client
cd iot-sensor-client

2. Initialize Project
npm init -y

3. Install Dependencies
npm install typescript ts-node node-fetch @types/node

4. Create tsconfig.json
npx tsc --init

5. Run the Project
npx ts-node src/index.ts

