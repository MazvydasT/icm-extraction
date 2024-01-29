import { Module } from '@nestjs/common';
import { BigQueryModule } from '../big-query/big-query.module';
import { OutputService } from './output.service';

@Module({
  imports: [BigQueryModule],
  providers: [OutputService],
  exports: [OutputService]
})
export class OutputModule {}
