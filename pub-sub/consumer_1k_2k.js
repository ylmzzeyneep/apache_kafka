const { Kafka } = require("kafkajs");

createConsumer();

async function createConsumer() {
    try {
        const kafka = new Kafka({
            clientId: "kafka_pub_sub_client",
            brokers: ["localhost:9092"]
        });

        const consumer = kafka.consumer({
            groupId: "hd_1k_2k_encoder_consumer_group",
        });

        console.log("Connecting to Consumer...");
        await consumer.connect();
        console.log("Connected is successful!!");

        await consumer.subscribe({
            topic: "rawVideoTopic",
            fromBeginning: true,
        });

        await consumer.run({
            eachMessage: async ({ message }) => {
                const value = message.value.toString();
                console.log(`Received message: ${value}_1k_2k_encoder`);
            },
        });

    } catch (error) {
        console.log("Error in consumer:", error);
    }
}
