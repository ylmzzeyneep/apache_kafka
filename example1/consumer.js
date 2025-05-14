const { Kafka } = require('kafkajs');

const topic_name = process.argv[2] || "logs2";

createConsumer();

async function createConsumer() {
    try {
        const kafka = new Kafka({
            clientId: 'kafka_example_1',
            brokers: ['localhost:9092']
        });

        const consumer = kafka.consumer({
            groupId: 'test-group',
        });

        console.log('Connecting to Consumer...');
        await consumer.connect();
        console.log('Connected is successful!');

        await consumer.subscribe({
            topic: topic_name,
            fromBeginning: true,
        });

        await consumer.run({
            eachMessage: async ({partition, message }) => {
                const value = message.value.toString();
                console.log(`Received message: ${value}, Partition => ${partition}`);
            },
        });

    } catch (error) {
        console.log('Error in consumer:', error);
    }
}
