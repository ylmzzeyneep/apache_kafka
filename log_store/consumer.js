const { Kafka } = require("kafkajs");

createConsumer();

async function createConsumer() {
    try {
        const kafka = new Kafka({
            clientId: "kafka_log_store_client",
            brokers: ["localhost:9092"]
        });

        const consumer = kafka.consumer({
            groupId: "log_store_consumer_group",
        });

        console.log("Connecting to Consumer...");
        await consumer.connect();
        console.log("Connected is successful!!");

        await consumer.subscribe({
            topic: "logStoreTopic",
            fromBeginning: true,
        });

        await consumer.run({
            eachMessage: async ({ partition, message }) => {
                const value = message.value.toString();
                console.log(`Received message: ${value}, Partition => ${partition}`);
            },
        });

    } catch (error) {
        console.log("Error in consumer:", error);
    }
}
