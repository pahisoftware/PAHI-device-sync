require("dotenv").config();

const mqtt = require("mqtt");
const { MongoClient } = require("mongodb");

// ================= ENV CONFIG =================
const MQTT_BROKER = process.env.MQTT_BROKER;
const MQTT_TOPIC = process.env.MQTT_TOPIC;
const MQTT_USERNAME = process.env.MQTT_USERNAME;
const MQTT_PASSWORD = process.env.MQTT_PASSWORD;

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.MONGO_DB;

const RAW_COLLECTION = process.env.RAW_COLLECTION;
const LATEST_COLLECTION = process.env.LATEST_COLLECTION;


let db;

// ================= CONNECT MONGO =================
async function connectMongo() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  db = client.db(DB_NAME);
  console.log("Connected to MongoDB");
}

// ================= CONNECT MQTT =================
function connectMQTT() {
  const client = mqtt.connect(MQTT_BROKER, {
    username: MQTT_USERNAME,
    password: MQTT_PASSWORD,
    reconnectPeriod: 5000 // optional: auto reconnect
  });

  client.on("connect", () => {
    console.log("Connected to MQTT");

    client.subscribe(MQTT_TOPIC, (err) => {
      if (err) console.error("Subscribe error:", err);
      else console.log("Subscribed to:", MQTT_TOPIC);
    });
  });

  client.on("message", async (topic, message) => {
    try {
      const payload = JSON.parse(message.toString());

      // Extract device_id from topic
      const topicParts = topic.split("/");
      const device_id = topicParts[2];

      // Convert timestamp
      const timestamp = payload.timestamp
        ? new Date(payload.timestamp * 1000)
        : new Date();

      // ================= RAW DATA =================
      const rawDoc = {
        device_id,
        timestamp,
        ...payload
      };

      await db.collection(RAW_COLLECTION).insertOne(rawDoc);

      // ================= LATEST STATE =================
      const latestDoc = {
        _id: device_id,

        device_id,
        timestamp,

        gps: payload.gps || null,
        battery: payload.battery || null,
        storage: payload.storage || null,

        msg_type: payload.msg_type || null,

        last_updated: new Date(),
        status: "online"
      };

      await db.collection(LATEST_COLLECTION).updateOne(
        { _id: device_id },
        { $set: latestDoc },
        { upsert: true }
      );

      console.log(`✅ Stored data for ${device_id}`);
    } catch (err) {
      console.error("Error:", err.message);
    }
  });

  client.on("error", (err) => {
    console.error("MQTT Error:", err.message);
  });
}

// ================= INIT =================
(async () => {
  try {
    await connectMongo();
    connectMQTT();
  } catch (err) {
    console.error("❌ Startup Error:", err);
  }
})();