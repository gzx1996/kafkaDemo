const { Kafka } = require('kafkajs')
const kafka = new Kafka({
  brokers: ['121.40.252.80:9092']
})

const producer = kafka.producer()

const func = async() => {
    await producer.connect()
    let i = 0;
    while(true) {
         i ++;
        await producer.send({
            topic: 'test-topic',
            messages: [
                { value: 'now my count is ' + i },
            ],
        }).catch(e => {
            console.log(e)
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