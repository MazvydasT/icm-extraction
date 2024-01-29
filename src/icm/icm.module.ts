import { HttpModule } from '@nestjs/axios';
import { Module } from '@nestjs/common';

import { ConfigurationModule } from '../configuration/configuration.module';
import { ICMService } from './icm.service';

@Module({
  imports: [HttpModule, ConfigurationModule],
  providers: [ICMService]
})
export class ICMModule {}
