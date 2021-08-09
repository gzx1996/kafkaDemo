const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  brokers: ['121.40.252.80:9092']
})

const consumer = kafka.consumer({ groupId: 'test-group'})

const func = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true })

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        topic,
        partition,
        value: message.value.toString(),
        m: JSON.stringify(message)
      });
      await commit(topic, partition, message.offset);
    },
  }).catch(e => {
    console.log(e)
  })
}

const commit = async (topic, partition, offset) => {
  await consumer.commitOffsets([{
      topic,
      partition,
      offset: String(Number(offset) + 1)
  }]);
  console.log('commited' + offset);
}

func();