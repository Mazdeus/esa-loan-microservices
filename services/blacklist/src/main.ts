import { createKafkaClient } from '../../libs/kafka/src/kafka.provider';

function isBlacklisted(userId: string) {
  if (!userId) return false;
  if (userId.toLowerCase().includes('bad')) return true;
  return Math.random() < 0.15;
}

async function bootstrap() {
  const kafka = createKafkaClient([process.env.KAFKA_BROKER || 'kafka:9092']);
  const consumer = kafka.consumer({ groupId: 'blacklist-group' });
  const producer = kafka.producer();
  await consumer.connect();
  await producer.connect();
  await consumer.subscribe({ topic: 'risk.checked', fromBeginning: false });
  console.log('Blacklist service subscribed to risk.checked');
  await consumer.run({
    eachMessage: async ({ message }) => {
      const payload = JSON.parse(message.value.toString());
      console.log('Blacklist checking', payload.applicationId, payload.userId);
      const blacklisted = isBlacklisted(payload.userId);
      const result = { applicationId: payload.applicationId, userId: payload.userId, blacklisted, reason: blacklisted ? 'MATCHED_BLACKLIST' : undefined, checkedAt: new Date().toISOString() };
      await producer.send({ topic: 'blacklist.checked', messages: [{ key: payload.applicationId, value: JSON.stringify(result) }]});
      console.log('Blacklist emitted blacklist.checked for', payload.applicationId, 'blacklisted=', blacklisted);
    }
  });
}

bootstrap().catch(err => {
  console.error('Blacklist service failed', err);
  process.exit(1);
});
