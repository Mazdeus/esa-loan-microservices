import { createKafkaClient } from '../../libs/kafka/src/kafka.provider';

async function bootstrap() {
  const kafka = createKafkaClient([process.env.KAFKA_BROKER || 'kafka:9092']);
  const consumer = kafka.consumer({ groupId: 'kyc-group' });
  const producer = kafka.producer();
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: 'loan.requested', fromBeginning: false });
  console.log('KYC service subscribed to loan.requested');
  await consumer.run({
    eachMessage: async ({ message }) => {
      const payload = JSON.parse(message.value.toString());
      console.log('KYC received loan.requested:', payload.applicationId);
      // fake KYC logic (replace with real KYC)
      const result = { applicationId: payload.applicationId, userId: payload.userId, kycStatus: 'PASSED', checkedAt: new Date().toISOString() };
      await producer.send({ topic: 'kyc.completed', messages: [{ key: payload.applicationId, value: JSON.stringify(result) }]});
      console.log('KYC emitted kyc.completed for', payload.applicationId);
    }
  });
}

bootstrap().catch(err => {
  console.error('KYC service failed', err);
  process.exit(1);
});
