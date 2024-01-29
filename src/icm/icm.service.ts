import { HttpService } from '@nestjs/axios';
import { Injectable } from '@nestjs/common';
import axios, { AxiosInstance } from 'axios';
import { from } from 'ix/iterable';
import { filter, flatMap, map as mapIx } from 'ix/iterable/operators';
import { Observable, catchError, defer, map, mergeMap, shareReplay } from 'rxjs';
import { ICredentials } from './ICredentials';
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

const authorisedAxiosInstances = new WeakMap<ICredentials, Observable<AxiosInstance>>();

@Injectable()
export class ICMService {
  constructor(private httpService: HttpService) {}

  private getAuthorisedAxios(credentials: ICredentials) {
    const authorisedAxiosInstance =
      authorisedAxiosInstances.get(credentials) ??
      defer(() =>
        this.httpService.post(LOGIN_URL, {
          username: credentials.username,
          password: credentials.password,
          claims: [`name`, `mail`, `sn`, `givenName`, `exp`]
        })
      ).pipe(
        catchError((err, caught) => {
          return caught;
        }),
        map(response => {
          const authHeaderKey = `authorization`;
          const authHeaderValue = response.headers[authHeaderKey];

          return axios.create({
            headers: {
              ...Object.fromEntries([[authHeaderKey, authHeaderValue]]),
              //'Accept-Language': `en-GB,en;q=0.9`
              'Accept-Language': `EN`
            }
          });
        }),
        shareReplay(1)
      );

    if (!authorisedAxiosInstances.has(credentials))
      authorisedAxiosInstances.set(credentials, authorisedAxiosInstance);

    return authorisedAxiosInstance;
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
            from(Object.entries(data.columns.chgelem)).pipe(
              filter(([, value]) => typeof value == 'string'),
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
                  filter(([, value]) => value.length > 0)
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
