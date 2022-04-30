const { Kafka } = require("kafkajs");
const app = require("express")();
const server = require("http").createServer(app);
const io = require("socket.io")(server, {
  cors: { origin: "*" },
  path: "/chart",
});
const kafka = new Kafka({
  clientId: "Server Node",
  brokers: ["localhost:9092"],
});

io.on("connection", async (socket) => {
  console.log("connection");

  const consumer = kafka.consumer({ groupId: "client" });
  await consumer.connect();
  await consumer.subscribe({ topic: "price", fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      socket.emit("price", {
        data: JSON.parse(message.value.toString()),
        date: new Date().toLocaleTimeString(),
      });
      console.log("emitting...");
    },
  });
  socket.on("disconnect", () => {
    console.log("client disconnected");
  });
});

server.listen(4000, () => console.log("Server is running"));

app.get("/ws", async (req, res) => {
  const consumer = kafka.consumer({ groupId: "client" });

  await consumer.connect();
  await consumer.subscribe({ topic: "price", fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(JSON.parse(message.value.toString()));
    },
  });
});
