import { HttpModuleOptions, HttpService } from '@nestjs/axios';
import { Injectable } from '@nestjs/common';
import axios, { AxiosError, AxiosInstance } from 'axios';
import { HttpsProxyAgent } from 'https-proxy-agent';
import { from } from 'ix/iterable';
import { filter as filterIx, flatMap, map as mapIx } from 'ix/iterable/operators';
import { Observable, defer, filter, map, mergeMap, toArray } from 'rxjs';
import { ConfigurationService } from '../configuration/configuration.service';
import { ICredentials } from './ICredentials';
import { IListValueDataItem } from './IListValueDataItem';
import { IUppMatClassesFilterDataItem } from './IUppMatClassesFilterDataItem';
import { IUppMatDataItem } from './IUppMatDataItem';
import { IUppViewMatClassParamDataItem } from './IUppViewMatClassParamDataItem';
import { Ii18nJSONData } from './Ii18nJSONData';

export interface IChgelemPartDataItem {
  chgelemChgnoteSeq: number;
}

const LOGIN_URL = `https://b2b.magnasteyr.com/accessmanager-access/jwt/login`;
const UPP_MAT_DATA_PROVIDER_URL = `https://apps01.magnasteyr.com/icmnfRest/upp/uppMatDataProvider`;
const CHGELEM_PART_URL = `https://apps01.magnasteyr.com/icmnfRest/dataprovider/chgelemPart`;
const UPP_VIEW_MAT_PARAM_DATA_PROVIDER_URL = `https://apps01.magnasteyr.com/icmnfRest/dataprovider/uppviewMatClassParamDataProvider`;

/**
 * URL to get 'uppviewMatClassParamDataProvider' classParamSeq to column name mapping
 */
const UPP_VIEW_MAT_CLASSES_FILTER_URL = `https://apps01.magnasteyr.com/icmnfRest/upp/uppviewMatClassesFilter`;

/**
 * ULR to get 'chgelemPart' property to column english name mapping
 */
const EN_JSON_URL = `https://apps01.magnasteyr.com/icmnfext/assets/i18n/en.json`;

/**
 * URL to get 'list of values' - data to label mapping
 */
const LOV_URL = `https://apps01.magnasteyr.com/icmnfRest/lov/`;

const authorisedAxiosInstances = new WeakMap<ICredentials, Observable<AxiosInstance>>();

@Injectable()
export class ICMService {
  private httpPoxyConfig: HttpModuleOptions = {
    proxy: false,
    httpsAgent: !!this.configurationService.httpsProxy
      ? new HttpsProxyAgent(this.configurationService.httpsProxy)
      : undefined
  };

  constructor(
    private httpService: HttpService,
    private configurationService: ConfigurationService
  ) {}

  private getAuthorisedAxios(credentials: ICredentials) {
    return defer(() => {
      if (authorisedAxiosInstances.has(credentials))
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        return authorisedAxiosInstances.get(credentials)!;

      const authorisedAxiosInstance = defer(() =>
        this.httpService.post(
          LOGIN_URL,
          {
            username: credentials.username,
            password: credentials.password,
            claims: [`name`, `mail`, `sn`, `givenName`, `exp`]
          },
          this.httpPoxyConfig
        )
      ).pipe(
        map(response => {
          const authHeaderKey = `authorization`;
          const authHeaderValue = response.headers[authHeaderKey];

          const axiosInstance = axios.create({
            ...this.httpPoxyConfig,
            headers: {
              ...Object.fromEntries([[authHeaderKey, authHeaderValue]]),
              'Accept-Language': `EN`
            }
          });

          axiosInstance.interceptors.response.use(
            response => response,
            (error?: AxiosError) => {
              if (error?.response?.status == 401) authorisedAxiosInstances.delete(credentials);

              return Promise.reject(error);
            }
          );

          return axiosInstance;
        })
      );

      authorisedAxiosInstances.set(credentials, authorisedAxiosInstance);

      return authorisedAxiosInstance;
    });
  }

  public getDataToLabelMap(credentials: ICredentials, listValuesId: `UPP_STATUS`) {
    const lovULR = new URL(LOV_URL);
    lovULR.searchParams.append(`listValIds`, listValuesId);

    return this.getAuthorisedAxios(credentials).pipe(
      mergeMap(axios => axios.get(lovULR.href)),
      mergeMap(response => (response.data[listValuesId] ?? []) as IListValueDataItem[]),
      filter(({ isValid }) => isValid),
      map(({ data, label }) => [data, label] as const),
      toArray(),
      map(keyValuePairs => new Map(keyValuePairs))
    );
  }

  private getEnJSON(credentials: ICredentials) {
    return this.getAuthorisedAxios(credentials).pipe(
      mergeMap(axios => axios.get<Ii18nJSONData>(EN_JSON_URL)),
      map(response => response.data)
    );
  }

  public getChgelemColumnNames(credentials: ICredentials) {
    return this.getEnJSON(credentials).pipe(
      map(
        data =>
          new Map(
            from([
              //...Object.entries(data.columns.uppMaterial),
              ...Object.entries(data.columns.chgelem)
            ]).pipe(
              filterIx(([, value]) => typeof value == 'string'),
              mapIx(keyValuePair => keyValuePair as [string, string])
            )
          )
      )
    );
  }

  public getUPPMatData(credentials: ICredentials, data: Record<string, any>[]) {
    return this.getAuthorisedAxios(credentials).pipe(
      mergeMap(axios => axios.post<IUppMatDataItem[]>(UPP_MAT_DATA_PROVIDER_URL, data)),
      map(response => response.data)
    );
  }

  public getChgelemPartData(credentials: ICredentials, chgelemChgnoteSeqSet: number[]) {
    return this.getAuthorisedAxios(credentials).pipe(
      mergeMap(axios =>
        axios.post<(IChgelemPartDataItem | Record<string, any>)[]>(
          CHGELEM_PART_URL,
          chgelemChgnoteSeqSet
        )
      ),
      map(response => response.data)
    );
  }

  private getUPPMatClassesFilterData(credentials: ICredentials, data: Record<string, any>) {
    return this.getAuthorisedAxios(credentials).pipe(
      mergeMap(axios =>
        axios.put<IUppMatClassesFilterDataItem[]>(UPP_VIEW_MAT_CLASSES_FILTER_URL, data)
      ),
      map(response => response.data)
    );
  }

  public getUppViewMatClassParamDataColumnNames(
    credentials: ICredentials,
    data: Record<string, any>
  ) {
    return this.getUPPMatClassesFilterData(credentials, data).pipe(
      map(
        data =>
          new Map(
            from(data).pipe(
              flatMap(item =>
                from(item.classParams).pipe(
                  mapIx(
                    ({ classParamSeq, paramSeq }) =>
                      [classParamSeq, paramSeq.descText ?? paramSeq.paramLongName ?? ``] as [
                        number,
                        string
                      ]
                  ),
                  filterIx(([, value]) => value.length > 0)
                )
              )
            )
          )
      )
    );
  }

  public getUppViewMatClassParamData(
    credentials: ICredentials,
    classParamSeqs: number[],
    uppviewMatSeqs: number[]
  ) {
    return this.getAuthorisedAxios(credentials).pipe(
      mergeMap(axios =>
        axios.put<IUppViewMatClassParamDataItem[]>(UPP_VIEW_MAT_PARAM_DATA_PROVIDER_URL, {
          calcPermissions: true,
          calcQuality: false,
          classParamSeqs,
          uppviewMatSeqs
        })
      ),
      map(response => response.data)
    );
  }
}
