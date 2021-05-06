const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  brokers: ['localhost:9092']
})

const consumer = kafka.consumer({ groupId: 'test-group' })



const func = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        value: message.value.toString(),
      })
    },
  })
}

func();