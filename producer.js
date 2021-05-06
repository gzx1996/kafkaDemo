const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  brokers: ['localhost:9092']
})

const producer = kafka.producer()

const func = async() => {
    await producer.connect()
    for(let i = 0; i < 10000; i++) {
        await producer.send({
            topic: 'test-topic',
            messages: [
                { value: 'now my count is ' + i },
            ],
        });
        await sleep(1000);
    }
}
func();

const sleep = (delay) => {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            try {
                resolve(true)
            } catch (e) {
                reject(false)
            }
        }, delay);
    }) 
}