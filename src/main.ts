import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { concat, empty, from, reduce } from 'ix/iterable';
import { filter, flatMap, groupBy, map as ixMap } from 'ix/iterable/operators';
import { DateTime } from 'luxon';
import { DataFrame, DataType, Series, lit } from 'nodejs-polars';
import { TimeUnit } from 'nodejs-polars/bin/datatypes';
import { RetryConfig, firstValueFrom, map, retry, tap, timer, zip } from 'rxjs';
import { AppModule } from './app.module';
import { ConfigurationService } from './configuration/configuration.service';
import { ICredentials } from './icm/ICredentials';
import { IUppMatDataItem } from './icm/IUppMatDataItem';
import { ICMService } from './icm/icm.service';
import { OutputService } from './output/output.service';
import { clamp, getAdditionalProperties, isNumeric } from './utils';

const prostructSeq = 3321;
const uppgenerationSeq = 1841;
const uppviewSeq = 81;

const uppViewMatClassesFilterPostBody = {
  prjstructSeq: 0,
  prostructSeq: prostructSeq,
  uppgenerationSeq: uppgenerationSeq,
  uppviewSeq: uppviewSeq
};

const uppMatDataPostBody = [
  {
    additionalCriteria: null,
    dataProviderName: 'DP_UPPVIEWMAT',
    foreignKeyName: 'uppviewMatSeq',
    dpEntityClass: null,
    filterValues: [
      {
        type: 'MsfClassificationFilterSetting',
        additionalCriteria: null,
        filterKey: 'uppStatus',
        filterValue: 'N;|;G;|;Z;|;Y',
        multivalueExactMatch: false,
        useAndConcatenation: false,
        useCaseSensitiveComparision: true,
        useWildCards: false
      }
    ]
  },
  {
    additionalCriteria: null,
    dataProviderName: 'DP_MANDATORY',
    foreignKeyName: null,
    dpEntityClass: null,
    filterValues: [
      {
        type: 'MsfClassificationFilterSetting',
        additionalCriteria: null,
        filterKey: 'MANDATORY_FILTER_UPPGENERATION',
        filterValue: `${uppgenerationSeq}`,
        multivalueExactMatch: false,
        useAndConcatenation: false,
        useCaseSensitiveComparision: true,
        useWildCards: false
      },
      {
        type: 'MsfClassificationFilterSetting',
        additionalCriteria: null,
        filterKey: 'MANDATORY_FILTER_UPPVIEW',
        filterValue: `${uppviewSeq}`,
        multivalueExactMatch: false,
        useAndConcatenation: false,
        useCaseSensitiveComparision: true,
        useWildCards: false
      },
      {
        type: 'MsfClassificationFilterSetting',
        additionalCriteria: null,
        filterKey: 'MANDATORY_FILTER_PRODUCT_TYPE',
        filterValue: 'MOD',
        multivalueExactMatch: false,
        useAndConcatenation: false,
        useCaseSensitiveComparision: true,
        useWildCards: false
      },
      {
        type: 'MsfClassificationFilterSetting',
        additionalCriteria: null,
        filterKey: 'MANDATORY_FILTER_PRODUCT',
        filterValue: `${prostructSeq}`,
        multivalueExactMatch: false,
        useAndConcatenation: false,
        useCaseSensitiveComparision: true,
        useWildCards: false
      }
    ]
  } /*,
  {
    additionalCriteria: null,
    dataProviderName: 'DP_UPPVIEWMAT_CPARAM',
    foreignKeyName: 'uppviewMatSeq',
    dpEntityClass: null,
    filterValues: [
      {
        type: 'BOOLEAN',
        additionalCriteria: null,
        filterKey: '4801',
        filterValue: '1',
        multivalueExactMatch: false,
        useAndConcatenation: false,
        useCaseSensitiveComparision: true,
        useWildCards: false
      }
    ]
  }*/
];

// eslint-disable-next-line @typescript-eslint/no-empty-function
process.on(`unhandledRejection`, () => {});

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule);
  const icmService = app.get(ICMService);
  const configurationService = app.get(ConfigurationService);
  const outputService = app.get(OutputService);

  const logger = new Logger(`main`);

  const retryConfig: RetryConfig = {
    count: configurationService.retries,
    delay: configurationService.retryDelay,
    resetOnSuccess: true
  };

  const credentials: ICredentials = {
    username: configurationService.username,
    password: configurationService.password
  };

  let persistentErrorCount = 0;

  while (true) {
    logger.log(`Starting extraction`);

    const extractionTime = DateTime.now().toUTC().toJSDate();

    try {
      const chgelemColumnNamesPromise = firstValueFrom(
        icmService.getChgelemColumnNames(credentials).pipe(retry(retryConfig))
      );

      const uppViewMatClassParamDataColumnNamesPromise = firstValueFrom(
        icmService
          .getUppViewMatClassParamDataColumnNames(credentials, uppViewMatClassesFilterPostBody)
          .pipe(retry(retryConfig))
      );

      const uppStatusDataToLabelMapPromise = firstValueFrom(
        icmService.getDataToLabelMap(credentials, 'UPP_STATUS').pipe(retry(retryConfig))
      );

      const {
        chgelemChgnoteSeqTechSet,
        uppviewMatSeqSet,
        chgelemChgnoteSeqTechToUppviewMatSeqMap,
        uppDataByChgelemChgnoteSeqTech
      } = await firstValueFrom(
        icmService.getUPPMatData(credentials, uppMatDataPostBody).pipe(
          map(uppMatData => {
            const chgelemChgnoteSeqTechSet = new Set<number>();
            const uppviewMatSeqSet = new Set<number>();
            const chgelemChgnoteSeqTechToUppviewMatSeqMap = new Map<number, number>();
            const uppDataByChgelemChgnoteSeqTech = new Map<number, IUppMatDataItem>();

            for (const uppMatDataItem of uppMatData) {
              const { chgelemChgnoteSeqTech, uppviewMatSeq } = uppMatDataItem;

              chgelemChgnoteSeqTechSet.add(chgelemChgnoteSeqTech);
              uppviewMatSeqSet.add(uppviewMatSeq);
              chgelemChgnoteSeqTechToUppviewMatSeqMap.set(chgelemChgnoteSeqTech, uppviewMatSeq);
              uppDataByChgelemChgnoteSeqTech.set(chgelemChgnoteSeqTech, uppMatDataItem);
            }

            return {
              chgelemChgnoteSeqTechSet,
              uppviewMatSeqSet,
              chgelemChgnoteSeqTechToUppviewMatSeqMap,
              uppDataByChgelemChgnoteSeqTech
            };
          }),
          retry(retryConfig)
        )
      );

      const chgelemPartDataObservable = icmService
        .getChgelemPartData(credentials, [
          ...from(chgelemChgnoteSeqTechSet.keys()).pipe(ixMap(value => (!value ? -1 : value)))
        ])
        .pipe(retry(retryConfig));

      const uppViewMatClassParamDataColumnNames = await uppViewMatClassParamDataColumnNamesPromise;

      const uppViewMatClassParamDataByUppViewMatSeq = await firstValueFrom(
        icmService
          .getUppViewMatClassParamData(
            credentials,
            [...uppViewMatClassParamDataColumnNames.keys()],
            [...uppviewMatSeqSet.keys()]
          )
          .pipe(
            map(
              uppViewMatClassParamData =>
                new Map(
                  from(uppViewMatClassParamData).pipe(
                    groupBy(({ uppviewMatSeq }) => uppviewMatSeq),
                    ixMap(group => [
                      group.key,
                      new Map(
                        group.pipe(ixMap(({ classParamSeq, value }) => [classParamSeq, value]))
                      )
                    ])
                  )
                )
            ),
            retry(retryConfig)
          )
      );

      const data = await firstValueFrom(
        zip([
          chgelemColumnNamesPromise,
          chgelemPartDataObservable,
          uppStatusDataToLabelMapPromise
        ]).pipe(
          tap(() => logger.log(`Data extracted`)),
          map(([chgelemColumnNames, chgelemPartData, uppStatusDataToLabelMap]) => {
            const chgelemPartDataRowCount = chgelemPartData.length;

            return from(chgelemPartData).pipe(
              ixMap((record, rowIndex) => {
                const { chgelemChgnoteSeq } = record;

                let { uppStatus, uppStatusChgDate } =
                  uppDataByChgelemChgnoteSeqTech.get(chgelemChgnoteSeq) ?? {};

                uppStatus = uppStatusDataToLabelMap.get(uppStatus!) ?? uppStatus;

                return concat(
                  from([
                    ...Object.entries({ uppStatus, uppStatusChgDate }),
                    ...Object.entries(record)
                  ]).pipe(
                    ixMap(([key, value]) => ({
                      key: chgelemColumnNames.get(key) ?? key,
                      value
                    }))
                  ),

                  from(
                    uppViewMatClassParamDataByUppViewMatSeq
                      .get(
                        chgelemChgnoteSeqTechToUppviewMatSeqMap.get(chgelemChgnoteSeq) ??
                          Number.MIN_VALUE
                      )
                      ?.entries() ?? empty()
                  ).pipe(
                    ixMap(([key, value]) => ({
                      key: uppViewMatClassParamDataColumnNames.get(key) ?? ``,
                      value
                    })),
                    filter(({ key }) => key.length > 0)
                  )
                ).pipe(
                  ixMap(({ key, value }) => {
                    let newKey = key.replaceAll(/[^A-Za-z0-9]/g, `_`);

                    if (/^\d.*$/.test(newKey)) newKey = `_${newKey}`;

                    let newValue = value;

                    if (typeof value == 'string') {
                      if (/^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}[A-Z0-9\.+:]*)?$/.test(value))
                        newValue = new Date(value);
                      else if (/^\d{2}\.\d{2}\.\d{4}$/.test(value))
                        newValue = new Date(value.split(`.`).reverse().join(`-`));
                      else if (
                        isNumeric(value) &&
                        (!value.trim().startsWith(`0`) || value.length == 1)
                      )
                        newValue = parseFloat(value);
                    }

                    return { key: newKey, value: newValue, rowIndex };
                  })
                );
              }),
              flatMap(item => item),
              groupBy(({ key }) => key),
              ixMap(columnData => {
                let isDateWithoutTime = true;
                let isInt = true;
                let isNull = true;
                let isString = false;

                let values = reduce(columnData, {
                  callback(accumulator, { rowIndex, value }) {
                    if (typeof value == 'number' && !Number.isInteger(value)) {
                      isInt = false;
                    } else if (
                      value instanceof Date &&
                      value.getUTCHours() +
                        value.getUTCMinutes() +
                        value.getUTCSeconds() +
                        value.getUTCMilliseconds() >
                        0
                    ) {
                      isDateWithoutTime = false;
                    }

                    if (value ?? null != null) isNull = false;

                    if (typeof value == 'string') isString = true;

                    accumulator[rowIndex] = value;
                    return accumulator;
                  },
                  seed: new Array(chgelemPartDataRowCount)
                });

                if (isString) values = values.map(v => `${v}`);

                let series = Series(columnData.key, values);

                const seriesType = series.dtype.variant;

                if (isInt && seriesType == DataType.Float64.variant) {
                  series = series.cast(DataType.Int64);
                } else if (
                  isDateWithoutTime &&
                  seriesType == DataType.Datetime(TimeUnit.Milliseconds).variant
                ) {
                  series = series.cast(DataType.Date);
                }

                return { series, isNull };
              }),
              filter(({ isNull }) => !isNull),
              ixMap(({ series }) => series)
            );
          }),
          map(series =>
            DataFrame([...series]).withColumns(lit(extractionTime).alias(`ExtractionTime`))
          )
        )
      );

      const parquetDataBuffer = data.writeParquet({ compression: 'gzip' });

      logger.log(`Writing data to BigQuery`);

      await firstValueFrom(
        outputService.parquetToBigQuery(parquetDataBuffer).pipe(retry(retryConfig))
      );

      logger.log(`Data written to BigQuery`);
    } catch (error) {
      logger.error(error, ...getAdditionalProperties(error), error.stack);

      const cooldown = clamp(
        configurationService.persistentErrorCooldown * Math.pow(++persistentErrorCount, 2),
        configurationService.persistentErrorCooldown,
        configurationService.persistentErrorCooldownMax
      );

      const timerPromise = firstValueFrom(timer(cooldown));

      // Persistent error cooldown
      logger.log(
        `Persistent error occured, will retry ${DateTime.now().plus(cooldown).toRelative()}`
      );

      await timerPromise;

      continue;
    }

    persistentErrorCount = Math.max(--persistentErrorCount, 0);

    const cron = configurationService.cron;

    if (!cron) break;

    const now = DateTime.now();

    cron.reset(now.toJSDate());

    const nextExtractionStart = DateTime.fromJSDate(cron.next().value.toDate());
    const msToStartAnotherExtraction = Math.max(nextExtractionStart.diff(now).toMillis(), 0);

    logger.log(
      `Next extraction will start ${now
        .plus(msToStartAnotherExtraction)
        .toRelative()} on ${nextExtractionStart.toLocaleString(
        DateTime.DATE_MED_WITH_WEEKDAY
      )} at ${nextExtractionStart.toLocaleString(DateTime.TIME_24_SIMPLE)}`
    );

    await firstValueFrom(timer(msToStartAnotherExtraction));
  }
}
bootstrap();
