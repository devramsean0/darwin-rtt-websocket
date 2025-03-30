import { Kafka, logLevel } from "kafkajs";
import "dotenv/config";
import { createServer } from "http";
import { WebSocketServer } from "ws";


// Configuration (replace with your credentials)
const KAFKA_BROKER = process.env.KAFKA_BROKER!;
const KAFKA_USERNAME = process.env.CONFLUENT_USERNAME!
const KAFKA_PASSWORD = process.env.CONFLUENT_PASSWORD!
const TOPIC_NAME = "prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON";

const kafka = new Kafka({
  clientId: "darwin-consumer",
  brokers: [KAFKA_BROKER],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: KAFKA_USERNAME,
    password: KAFKA_PASSWORD,
  },
  logLevel: logLevel.ERROR,
});

const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID! });

const server = createServer();
const wss = new WebSocketServer({ server });

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const value = JSON.parse(message.value?.toString() || "{}");
        const decodedBytes = Buffer.from(value.bytes, "binary").toString("utf-8");
        const jsonBytes = JSON.parse(decodedBytes);
        //console.log("Event Received:", jsonBytes);
        if (jsonBytes.uR && jsonBytes.uR.TS && jsonBytes.uR.TS.Location) {
            console.log("Recieved Valid event", jsonBytes.uR.TS.rid)
            wss.clients.forEach(client => {
                if (client.readyState === 1) {
                    client.send(JSON.stringify(jsonBytes.uR.TS));
                }
            });
        }
      } catch (err) {
        console.error("Error processing message:", err);
      }
    },
  });
}

run().catch(async (e) => {
  console.error("Kafka consumer error:", e);
  await consumer.disconnect();
  process.exit(1);
});

server.on("connection", (ws) => {
    console.log("New client connected");
    ws.on("close", () => {
        console.log("Client disconnected");
    })
});
server.listen(8080, () => {
    console.log("WebSocket server is running on port 8080");
  });
  

// Handle graceful shutdown
process.on("SIGTERM", async () => {
  await consumer.disconnect();
  process.exit(0);
});