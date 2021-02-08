const  kafka = require('kafka-node');

const client = new kafka.KafkaClient({kafkaHost: 'localhost:9092'})
const offset = new kafka.Offset(client)

const consumerGoupOptions = {
  kafkaHost: 'localhost:9092',
  groupId: 'groupId1',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
}
const consumer = new kafka.ConsumerGroup(Object.assign({id: 'consumer1'}, consumerGoupOptions),  ['test'])

// 处理消息
consumer.on('message', async function (message) {
    console.log('consumer1 get message: '+ JSON.stringify(message, 'utf8'))
})

// 消息处理错误
consumer.on('error', function (err) {
  console.log('consumer1 error:', err)
})

consumer.on('offsetOutOfRange', function (topic) {
  console.log(`[offsetOutOfRange]:==:>${topic}`)
  topic.maxNum = 2
  offset.fetch([topic], function (err, offsets) {
    if (err) {
      return console.error(err)
    }
    let min = Math.min.apply(null, offsets[topic.topic][topic.partition])
    consumer.setOffset(topic.topic, topic.partition, min)
  })
})

process.on('SIGINT', function () {
  consumer.close(true, function () {
    console.log('consumer colse!')
    process.exit()
  })
})
