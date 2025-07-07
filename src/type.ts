export interface Config {
    user: User;
    sensor: SensorTokenRequest;
    datastream: DataStreamPayload;
    batchDataStreams: BatchDataStreamPayload[];
  }
  
  export interface User {
    email: string;
    password: string;
  }
  
  export interface SensorTokenRequest {
    sensorId: number;
    expiresAt: string;
  }
  
  export interface DataStreamPayload {
    value: string;
    telemetryDataId: number;
    recievedAt: string;
  }

  export interface BatchDataStreamPayload {
    dataStreams: DataStreamPayload[];
  }
  