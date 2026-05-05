import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { createKafkaClient }  from '../common/kafka.provider';
import { v4 as uuid } from 'uuid';
import type { Producer, Consumer } from 'kafkajs';

@Injectable()
export class LoanSaga implements OnModuleInit, OnModuleDestroy {
  private logger = new Logger('LoanSaga');
  private kafkaBroker = process.env.KAFKA_BROKER || 'kafka:9092';
  private kafkaProducer!: Producer;
  private kafkaConsumer!: Consumer;
  private pendingRequests = new Map<string, { resolve: Function; reject: Function; timer: NodeJS.Timeout }>();

  async onModuleInit() {
    const kafka = createKafkaClient([this.kafkaBroker]);
    const kp = await import('../common/kafka.provider');

    this.kafkaProducer = await kp.createProducer(kafka);

    // Create a persistent consumer group for loan-core responses
    const groupId = `loan-core-saga-${uuid()}`; 
    this.kafkaConsumer = await kp.createConsumer(kafka, groupId);

    const topics = ['kyc.completed', 'credit.checked', 'risk.checked', 'blacklist.checked'];
    for (const topic of topics) {
      await this.kafkaConsumer.subscribe({ topic, fromBeginning: false });
    }

    await this.kafkaConsumer.run({
      eachMessage: async ({ topic, message }) => {
        const applicationId = message.key?.toString();
        if (!applicationId) return;

        const eventKey = `${topic}:${applicationId}`;
        const pending = this.pendingRequests.get(eventKey);

        if (pending) {
          clearTimeout(pending.timer);
          this.pendingRequests.delete(eventKey);
          try {
            const payload = JSON.parse(message.value!.toString());
            pending.resolve(payload);
          } catch (err) {
            pending.reject(err);
          }
        }
      }
    });
    this.logger.log('LoanSaga persistent consumer initialized and listening to events');
  }

  async onModuleDestroy() {
    if (this.kafkaProducer) await this.kafkaProducer.disconnect();
    if (this.kafkaConsumer) await this.kafkaConsumer.disconnect();
  }

  private waitForEvent(topic: string, applicationId: string, timeoutMs = 20000): Promise<any> {
    const eventKey = `${topic}:${applicationId}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingRequests.delete(eventKey);
        reject(new Error(`Timeout waiting for ${topic} for ${applicationId}`));
      }, timeoutMs);

      this.pendingRequests.set(eventKey, { resolve, reject, timer });
    });
  }

  async execute(applyLoanDto: any) {
    const applicationId = applyLoanDto.applicationId || uuid();

    if (!this.kafkaProducer) throw new Error('Kafka producer not available');

    // STEP 1: emit loan.requested
    await this.kafkaProducer.send({
      topic: 'loan.requested',
      messages: [{ key: applicationId, value: JSON.stringify({ applicationId, ...applyLoanDto }) }],
    });
    this.logger.log(`loan.requested emitted for ${applicationId}`);

    try {
      // Wait KYC
      const kyc = await this.waitForEvent('kyc.completed', applicationId, 20000);
      this.logger.log(`KYC result for ${applicationId}: ${kyc.kycStatus}`);

      if (kyc.kycStatus !== 'PASSED') {
        await this.kafkaProducer.send({
          topic: 'loan.cancelled',
          messages: [{ key: applicationId, value: JSON.stringify({ applicationId, reason: 'KYC_FAILED', cancelledAt: new Date().toISOString() }) }],
        });
        await this.kafkaProducer.send({
          topic: 'audit.logged',
          messages: [{ key: applicationId, value: JSON.stringify({ applicationId, eventName: 'loan.cancelled', payload: kyc, recordedAt: new Date().toISOString() }) }],
        });
        return { applicationId, status: 'REJECTED', reason: 'KYC_FAILED' };
      }

      // Wait Credit
      const credit = await this.waitForEvent('credit.checked', applicationId, 20000);
      this.logger.log(`Credit result for ${applicationId}: score=${credit.score}`);

      if (credit.decision === 'FAIL') {
        await this.kafkaProducer.send({
          topic: 'loan.cancelled',
          messages: [{ key: applicationId, value: JSON.stringify({ applicationId, reason: 'CREDIT_REJECT', cancelledAt: new Date().toISOString() }) }],
        });
        await this.kafkaProducer.send({
          topic: 'audit.logged',
          messages: [{ key: applicationId, value: JSON.stringify({ applicationId, eventName: 'loan.cancelled', payload: credit, recordedAt: new Date().toISOString() }) }],
        });
        return { applicationId, status: 'REJECTED', reason: 'CREDIT_FAIL' };
      }

      // Wait Risk
      const risk = await this.waitForEvent('risk.checked', applicationId, 20000);
      this.logger.log(`Risk result for ${applicationId}: ${risk.risk}`);

      // Wait Blacklist (this one may result in blacklisted = true)
      const blacklist = await this.waitForEvent('blacklist.checked', applicationId, 20000);
      this.logger.log(`Blacklist result for ${applicationId}: blacklisted=${blacklist.blacklisted}`);

      if (blacklist.blacklisted) {
        await this.kafkaProducer.send({
          topic: 'loan.cancelled',
          messages: [{ key: applicationId, value: JSON.stringify({ applicationId, reason: 'BLACKLISTED', cancelledAt: new Date().toISOString() }) }],
        });
        await this.kafkaProducer.send({
          topic: 'audit.logged',
          messages: [{ key: applicationId, value: JSON.stringify({ applicationId, eventName: 'compensation.blacklist', payload: blacklist, recordedAt: new Date().toISOString() }) }],
        });
        await this.kafkaProducer.send({
          topic: 'loan.rolledback',
          messages: [{ key: applicationId, value: JSON.stringify({ applicationId, rolledBackAt: new Date().toISOString() }) }],
        });

        return { applicationId, status: 'REJECTED', reason: 'BLACKLISTED' };
      }

      // If all passed
      await this.kafkaProducer.send({
        topic: 'loan.approved',
        messages: [{ key: applicationId, value: JSON.stringify({ applicationId, approvedAt: new Date().toISOString() }) }],
      });
      await this.kafkaProducer.send({
        topic: 'audit.logged',
        messages: [{ key: applicationId, value: JSON.stringify({ applicationId, eventName: 'loan.approved', payload: {}, recordedAt: new Date().toISOString() }) }],
      });

      return { applicationId, status: 'APPROVED' };
    } catch (err: any) {
      this.logger.error(`Saga error for ${applicationId}: ${err.message || err}`);
      // best-effort compensation
      await this.kafkaProducer.send({
        topic: 'loan.cancelled',
        messages: [{ key: applicationId, value: JSON.stringify({ applicationId, reason: 'SAGA_ERROR', cancelledAt: new Date().toISOString(), error: String(err) }) }],
      });
      await this.kafkaProducer.send({
        topic: 'audit.logged',
        messages: [{ key: applicationId, value: JSON.stringify({ applicationId, eventName: 'saga.error', payload: String(err), recordedAt: new Date().toISOString() }) }],
      });
      return { applicationId, status: 'ERROR', message: String(err) };
    }
  }
}
