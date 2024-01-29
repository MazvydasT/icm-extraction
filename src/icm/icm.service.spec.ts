import { Test, TestingModule } from '@nestjs/testing';
import { ICMService } from './icm.service';

describe('IcmService', () => {
  let service: ICMService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [ICMService]
    }).compile();

    service = module.get<ICMService>(ICMService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
