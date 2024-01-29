import { Injectable } from '@nestjs/common';
import { Readable } from 'stream';
import { BigQueryService } from '../big-query/big-query.service';

@Injectable()
export class OutputService {
  constructor(private bigQueryService: BigQueryService) {}

  parquetToBigQuery(data: Buffer) {
    return this.bigQueryService.write(
      new Readable({
        read() {
          this.push(data);
          this.push(null);
        }
      }),
      {
        sourceFormat: `PARQUET`,
        writeDisposition: `WRITE_TRUNCATE`
      }
    );
  }
}
