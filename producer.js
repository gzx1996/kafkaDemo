const  kafka = require('kafka-node');

const client = new kafka.KafkaClient({
    kafkaHost: 'localhost:9092'
});

const producer = new kafka.HighLevelProducer(client);


producer.on('ready', () => {
    console.log('kafka server is ready, listened on localhost:9092');
});

producer.on('error', (err) => {
    console.log('An error occurred, error message:' + err.message);
})

const sendMessage = (message, topic, partition ) => {
    let o;
    if (Array.isArray(message)) {
        o = [];
        message.forEach(m => {
            m = Buffer.from(JSON.stringify(m));
            o.push({
                topic: topic || 'test-kafka',
                message: m,
                partition: partition || 0,
                attributes: 1,
                timestamp: Date.now()
            })
        })
    } else {
        message = Buffer.from(JSON.stringify(message));
        o = [{
            topic: topic || 'test-kafka',
            message,
            partition: partition || 0,
            attributes: 1,
            timestamp: Date.now()
        }]
    }
    producer.send(o, (err, data)=> {
        console.log(err, data)
    });
}

const run = () => {
    let count = 0;
    setInterval(async () => {
        let data = `current count is ${count}`;
        count ++;
        await sendMessage(data);
    }, 1000);
}

run();

module.exports = {
    sendMessage
}