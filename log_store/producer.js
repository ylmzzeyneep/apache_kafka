const { Kafka }  = require("kafkajs");

const log_data = require ("./system_logs.json");

createProducer();

async function createProducer() {
    try {
        const kafka = new Kafka({
            clientId: "kafka_log_store_client",
            brokers: ["localhost:9092"]
        });
    
        const producer = kafka.producer();
        console.log("Connecting to Producer...");

        await producer.connect();
        console.log("Connected is successful!!");
    
        let messages = log_data.map((item) => {
            return {
                value: JSON.stringify(item),
                partition: item.type === "system" ? 0 : 1,
            };
        });
        const message_result = await producer.send({
            topic: "logStoreTopic",
            messages: messages
        });

        console.log("Message sent successfully", JSON.stringify(message_result));
        await producer.disconnect();

    } catch (error) {
        console.log("Error creating topic:", error);
        
    }finally{
        process.exit(0);
    }
}