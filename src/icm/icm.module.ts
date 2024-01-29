import { HttpModule } from '@nestjs/axios';
import { Module } from '@nestjs/common';

import { ICMService } from './icm.service';

@Module({
  imports: [HttpModule],
  providers: [ICMService]
})
export class ICMModule {}
