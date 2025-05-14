const { Kafka }  = require("kafkajs");

createTopic();

async function createTopic() {
    try {
        const kafka = new Kafka({
            clientId: "kafka_log_store_client",
            brokers: ["localhost:9092"]
        });
    
        const admin = kafka.admin();
        console.log("Connecting to Kafka...");

        await admin.connect();
        console.log("Connected to Kafka is successful!!");
    
        await admin.createTopics({
            topics: [
                {
                    topic: "logStoreTopic",
                    numPartitions: 2,
                },
            ]
        }); 
        
        console.log("Topics created successfully"); 
        await admin.disconnect();
        
    } catch (error) {
        console.error("Error creating topic:", error);
        
    }finally{
        process.exit(0);
    }
}