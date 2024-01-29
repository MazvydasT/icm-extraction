import { Test, TestingModule } from '@nestjs/testing';
import { BigQueryService } from './big-query.service';

describe('BigQueryService', () => {
  let service: BigQueryService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [BigQueryService],
    }).compile();

    service = module.get<BigQueryService>(BigQueryService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
