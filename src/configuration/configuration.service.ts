import { Injectable } from '@nestjs/common';
import { Command, InvalidArgumentError, Option } from 'commander';
import { CronExpression, parseExpression } from 'cron-parser';
import { config } from 'dotenv';
import { parseIntClamp } from '../utils';

@Injectable()
export class ConfigurationService {
  private readonly optionValues = (() => {
    const envOption = new Option(`--env <path>`, `Path to .env file`).env(`ENV`);

    try {
      const envPath = new Command()
        .addOption(envOption)
        .configureOutput({
          writeErr: () => null,
          writeOut: () => null
        })
        .exitOverride()
        .parse()
        .opts<{ env?: string }>().env;

      config({ path: envPath, override: true });
    } catch (_) {}

    const command = new Command()
      .addOption(envOption)

      .addOption(
        new Option(`-u, --username <string>`, `ICM username`)
          .env(`USERNAME`)
          .makeOptionMandatory(true)
      )
      .addOption(
        new Option(`-p, --password <string>`, `ICM password`)
          .env(`PASSWORD`)
          .makeOptionMandatory(true)
      )

      .addOption(new Option(`--https-proxy <string>`, `HTTP proxy`).env(`HTTPS_PROXY`))

      .addOption(
        new Option(`-r, --retry <count>`, `Retry errors`)
          .env(`RETRY`)
          .default(5)
          .argParser(value => {
            try {
              return parseIntClamp(value, { min: 0 });
            } catch (_) {
              throw new InvalidArgumentError(``);
            }
          })
      )
      .addOption(
        new Option(`--retry-delay <ms>`, `Time delay in ms before retrying errors`)
          .env(`RETRY_DELAY`)
          .default(10 /*s*/ * 1000 /*ms*/)
          .argParser(value => {
            try {
              return parseIntClamp(value, { min: 0 });
            } catch (_) {
              throw new InvalidArgumentError(``);
            }
          })
      )

      .addOption(
        new Option(
          `-c, --persistent-error-cooldown <ms>`,
          `Time in ms between re-extarction attempts after persistent error`
        )
          .env(`PERSISTENT_ERROR_COOLDOWN`)
          .default(2 /*min*/ * 60 /*s*/ * 1000 /*ms*/)
          .argParser(parseInt)
      )
      .addOption(
        new Option(
          `--persistent-error-cooldown-max <ms>`,
          `Max time in ms between re-extarction attempts after persistent error`
        )
          .env(`PERSISTENT_ERROR_COOLDOWN_MAX`)
          .default(6 /*h*/ * 60 /*min*/ * 60 /*s*/ * 1000 /*ms*/)
          .argParser(parseInt)
      )

      .addOption(
        new Option(`--cron <expression>`, `Cron expression to schedule extraction`)
          .env(`CRON`)
          .argParser(value => {
            try {
              return !value ? undefined : parseExpression(value, { iterator: true });
            } catch (_) {
              throw new InvalidArgumentError(``);
            }
          })
      )

      .addOption(
        new Option(`--bqkeyfile <filepath>`, 'BigQuery key file')
          .env(`BQKEYFILE`)
          .makeOptionMandatory(true)
      )
      .addOption(
        new Option(`--bqproject <name>`, `BigQuery project name`)
          .env(`BQPROJECT`)
          .makeOptionMandatory(true)
      )
      .addOption(
        new Option(`--bqdataset <name>`, `BigQuery dataset name`)
          .env(`BQDATASET`)
          .makeOptionMandatory(true)
      )
      .addOption(
        new Option(`--bqtable <name>`, `BigQuery table name`)
          .env(`BQTABLE`)
          .makeOptionMandatory(true)
      )

      .showHelpAfterError(true)

      .parse();

    const options = command.opts<{
      username: string;
      password: string;

      httpsProxy?: string;

      retry: number;
      retryDelay: number;

      persistentErrorCooldown: number;
      persistentErrorCooldownMax: number;

      cron?: CronExpression<true>;

      bqkeyfile: string;
      bqproject: string;
      bqdataset: string;
      bqtable: string;
    }>();

    return Object.freeze(options);
  })();

  get username() {
    return this.optionValues.username;
  }
  get password() {
    return this.optionValues.password;
  }

  get httpsProxy() {
    return this.optionValues.httpsProxy;
  }

  get retries() {
    return this.optionValues.retry;
  }
  get retryDelay() {
    return this.optionValues.retryDelay;
  }

  get persistentErrorCooldown() {
    return this.optionValues.persistentErrorCooldown;
  }

  get persistentErrorCooldownMax() {
    return this.optionValues.persistentErrorCooldownMax;
  }

  get cron() {
    return this.optionValues.cron;
  }

  get bigQueryKeyFilename() {
    return this.optionValues.bqkeyfile;
  }
  get bigQueryProject() {
    return this.optionValues.bqproject;
  }
  get bigQueryDataset() {
    return this.optionValues.bqdataset;
  }
  get bigQueryTable() {
    return this.optionValues.bqtable;
  }
}
