import { createKafkaClient } from '../../libs/kafka/src/kafka.provider';

async function bootstrap() {
  const kafka = createKafkaClient([process.env.KAFKA_BROKER || 'kafka:9092']);
  const consumer = kafka.consumer({ groupId: 'credit-group' });
  const producer = kafka.producer();
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: 'kyc.completed', fromBeginning: false });
  console.log('Credit service subscribed to kyc.completed');
  await consumer.run({
    eachMessage: async ({ message }) => {
      const payload = JSON.parse(message.value.toString());
      console.log('Credit received kyc.completed:', payload.applicationId);
      // fake credit scoring
      const score = Math.floor(Math.random() * 801); // 0-800
      let decision = 'PASS';
      if (score < 500) decision = 'FAIL';
      else if (score < 600) decision = 'REVIEW';
      const result = { applicationId: payload.applicationId, userId: payload.userId, score, decision, checkedAt: new Date().toISOString() };
      await producer.send({ topic: 'credit.checked', messages: [{ key: payload.applicationId, value: JSON.stringify(result) }]});
      console.log('Credit emitted credit.checked for', payload.applicationId, 'score=', score);
    }
  });
}

bootstrap().catch(err => {
  console.error('Credit service failed', err);
  process.exit(1);
});
