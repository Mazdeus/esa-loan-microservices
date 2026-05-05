import { createKafkaClient } from '../../../libs/kafka/src/kafka.provider';
import * as fs from 'fs';
import * as path from 'path';
import express from 'express';

const AUDIT_DIR = process.env.AUDIT_DIR || '/app/audit_logs';
if (!fs.existsSync(AUDIT_DIR)) fs.mkdirSync(AUDIT_DIR, { recursive: true });

async function startKafkaConsumer() {
  const kafka = createKafkaClient([process.env.KAFKA_BROKER || 'kafka:9092']);
  const consumer = kafka.consumer({ groupId: 'audit-group' });
  await consumer.connect();
  await consumer.subscribe({ topic: 'audit.logged', fromBeginning: false });
  console.log('Audit service subscribed to audit.logged');
  await consumer.run({
    eachMessage: async ({ message }: any) => {
      const payload = JSON.parse(message.value.toString());
      console.log('AUDIT:', payload);
      const file = path.join(AUDIT_DIR, `${payload.applicationId || 'general'}.log`);
      fs.appendFileSync(file, JSON.stringify(payload) + '\n');
    }
  });
}

function startHttpServer() {
  const app = express();
  app.get('/audit/:id', (req, res) => {
    const id = req.params.id;
    const file = path.join(AUDIT_DIR, `${id}.log`);
    if (!fs.existsSync(file)) {
      return res.status(404).json({ error: 'not found' });
    }
    const lines = fs.readFileSync(file, 'utf8').trim().split('\n').map(l => JSON.parse(l));
    res.json(lines);
  });
  app.get('/health', (_req, res) => res.json({ status: 'ok', service: 'audit' }));
  const port = Number(process.env.AUDIT_HTTP_PORT || 3010);
  app.listen(port, () => console.log('Audit HTTP server listening on', port));
}

async function bootstrap() {
  startHttpServer();
  await startKafkaConsumer();
}

bootstrap().catch(err => {
  console.error('Audit failed', err);
  process.exit(1);
});
