const { Kafka }  = require("kafkajs");

createProducer();

async function createProducer() {
    try {
        const kafka = new Kafka({
            clientId: "kafka_pub_sub_client",
            brokers: ["localhost:9092"]
        });
    
        const producer = kafka.producer();
        console.log("Connecting to Producer...");

        await producer.connect();
        console.log("Connected is successful!!");
    
    
        const message_result = await producer.send({
            topic: "rawVideoTopic",
            messages: [
                { value: "New Video Streaming",
                    partition: 0,
                }
            ]
        });
        
        console.log("Message sent successfully", JSON.stringify(message_result));
        await producer.disconnect();

    } catch (error) {
        console.log("Error creating topic:", error);
        
    }finally{
        process.exit(0);
    }
}