import { Module } from '@nestjs/common';
import { LoanController } from './loan.controller';
import { LoanSaga } from './loan.saga';

@Module({
  controllers: [LoanController],
  providers: [LoanSaga],
  exports: [LoanSaga],
})
export class LoanModule {}
