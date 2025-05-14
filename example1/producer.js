const { Kafka }  = require('kafkajs');

const topic_name = process.argv[2] || "logs2";
const partition = process.argv[3] || 0;

createProducer();

async function createProducer() {
    try {
        const kafka = new Kafka({
            clientId: 'kafka_example_1',
            brokers: ['localhost:9092']
        });
    
        const producer = kafka.producer();
        console.log('Connecting to Producer...');

        await producer.connect();
        console.log('Connected is successful!!');
    
        const message_result = await producer.send({
            topic: topic_name,
            messages: [
                { value: 'Hello KafkaJS user!',
                    partition: partition,
                 },              
            ]
        })
        console.log('Message sent successfully', JSON.stringify(message_result));
        await producer.disconnect();

    } catch (error) {
        console.log('Error creating topic:', error);
        
    }finally{
        process.exit(0);
    }
}