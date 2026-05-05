import { Module } from '@nestjs/common';
import { LoanModule } from './loan/loan.module';
import { TypeOrmModule } from '@nestjs/typeorm';

@Module({
  imports: [
    LoanModule,
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: process.env.PG_LOAN_HOST || 'pg_loan_core',
      port: Number(process.env.PG_LOAN_PORT || 5432),
      username: process.env.PG_LOAN_USER || 'loan',
      password: process.env.PG_LOAN_PASSWORD || 'loanpass',
      database: process.env.PG_LOAN_DB || 'loan_core',
      entities: [__dirname + '/**/*.entity{.ts,.js}'],
      synchronize: true,
    }),
  ],
})
export class AppModule {}
