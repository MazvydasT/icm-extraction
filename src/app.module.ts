import { Module } from '@nestjs/common';
import { ConfigurationModule } from './configuration/configuration.module';
import { ICMModule } from './icm/icm.module';
import { OutputModule } from './output/output.module';

@Module({
  imports: [ICMModule, ConfigurationModule, OutputModule]
})
export class AppModule {}
