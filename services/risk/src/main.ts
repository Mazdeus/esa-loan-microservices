import { createKafkaClient } from '../../libs/kafka/src/kafka.provider';

async function bootstrap() {
  const kafka = createKafkaClient([process.env.KAFKA_BROKER || 'kafka:9092']);
  const consumer = kafka.consumer({ groupId: 'risk-group' });
  const producer = kafka.producer();
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: 'credit.checked', fromBeginning: false });
  console.log('Risk service subscribed to credit.checked');
  await consumer.run({
    eachMessage: async ({ message }) => {
      const payload = JSON.parse(message.value.toString());
      console.log('Risk received credit.checked:', payload.applicationId);
      // simple risk rule based on score
      let risk = 'LOW';
      if (payload.score < 600) risk = 'HIGH';
      else if (payload.score < 700) risk = 'MEDIUM';
      const result = { applicationId: payload.applicationId, userId: payload.userId, risk, details: { score: payload.score }, checkedAt: new Date().toISOString() };
      await producer.send({ topic: 'risk.checked', messages: [{ key: payload.applicationId, value: JSON.stringify(result) }]});
      console.log('Risk emitted risk.checked for', payload.applicationId, 'risk=', risk);
    }
  });
}

bootstrap().catch(err => {
  console.error('Risk service failed', err);
  process.exit(1);
});
