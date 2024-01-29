import { BigQuery, Job, JobLoadMetadata } from '@google-cloud/bigquery';
import { Injectable } from '@nestjs/common';
import { Observable } from 'rxjs';
import { Readable } from 'stream';
import { ConfigurationService } from '../configuration/configuration.service';

@Injectable()
export class BigQueryService {
  constructor(private configurationService: ConfigurationService) {}

  private bigQuery = new BigQuery({
    projectId: this.configurationService.bigQueryProject,
    keyFilename: this.configurationService.bigQueryKeyFilename
  });
  private bigQueryDataset = this.bigQuery.dataset(this.configurationService.bigQueryDataset);
  private bigQueryTable = this.bigQueryDataset.table(this.configurationService.bigQueryTable);

  write(readable: Readable, metadata: JobLoadMetadata) {
    return new Observable<Job>(subscriber => {
      readable
        .pipe(this.bigQueryTable.createWriteStream(metadata))
        .on(`complete`, job => {
          subscriber.next(job);
          subscriber.complete();
        })
        .on(`error`, err => subscriber.error(err));
    });
  }
}
