// Consumer.ts
const  kafka = require('kafka-node');

const client = new kafka.KafkaClient({kafkaHost: 'localhost:9092'})

const topics = [
  {
    topic: 'test-kafka'
  }
]
const options = {
  autoCommit: true,
  fetchMaxWaitMs: 1000,
  fetchMaxBytes: 1024 * 1024
  // encoding: 'buffer'
}

const consumer = new kafka.Consumer(client, topics, options)

consumer.on('message', function (message) {

  // Read string into a buffer.
  console.log('message recieved:', message,toString('utf8'));
})

consumer.on('error', function (err) {
  console.error('error', err.message)
})

process.on('SIGINT', function () {
  consumer.close(true, function () {
    process.exit()
  })
})

