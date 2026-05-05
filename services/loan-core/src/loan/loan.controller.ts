import { Controller, Post, Body, Get } from '@nestjs/common';
import { LoanSaga } from './loan.saga';

@Controller('loans')
export class LoanController {
  constructor(private readonly saga: LoanSaga) {}

  @Post('apply')
  async apply(@Body() dto: any) {
    return this.saga.execute(dto);
  }

  @Get('health')
  health() {
    return { status: 'ok', service: 'loan-core' };
  }
}
