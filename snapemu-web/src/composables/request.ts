import { useSettingStore } from '@/stores/setting';

import { isUndefined } from 'lodash-es';
import { useSuccessMessage, useWarningMessage } from '@/composables/notify'

export enum StatusCode {
  Ok = 0,
  AccessTimeout = 1,
  RefreshTimeout = 2,
  Warn = 3
}
export interface ResponseResult<T> {
  code: StatusCode,
  message?: string,
  notify?: string,
  data: T
}
type HTTPMethod = "GET" | "POST" | "PUT" | "DELETE";


interface ApiOption {
  success?: () => void;
  error?: () => void;
  snackBar?: boolean;
  token?: boolean;
}

interface ApiType {
  path: string,
  method: HTTPMethod,
  parameter?: string[],
  noRedirect?: boolean,
  body?: any
}

export const urlBase = () => import.meta.env.VITE_API_URL;

export const useRequest = async <T>(api: ApiType, option?: ApiOption): Promise<T> => {
  const setting = useSettingStore();
  const includeToken = option?.token ?? true;
  if (location.pathname !== "/user/login") {
    if (includeToken && !setting.token) {
      localStorage.removeItem("access_token");
      location.href = "/user/login";
    }
  }

  const headers = new Headers();
  let path: string = urlBase() + api.path;
  let body = undefined;

  if (includeToken) {
    headers.append("Authorization", "Bearer " + setting.token);
  }

  headers.append("Content-Type", "application/json; charset=utf-8");
  headers.append("Accept-Language", setting.language);

  if (!isUndefined(api.parameter)) {
    path = path.split("/").map(item => item.startsWith(":") ? api.parameter?.shift() : item).join("/");
  }
  if (!isUndefined(api.body)) {
    if (api.body instanceof FormData) {
      body = api.body;
    } else {
      body = JSON.stringify(api.body);
    }
  }

  let resp: ResponseResult<T>;
  try {
    const response = await fetch(path, {
      method: api.method,
      headers,
      body: body,
    });
    resp = await response.json();
  } catch (e) {
    console.log(e)
    throw "network error";
  }
  if (resp.code === StatusCode.Ok) {
    if(typeof option !== 'undefined') {
      if(typeof option.success !== 'undefined') {
        option.success();
      }
    }
    return resp.data
  }
  if(!isUndefined(option)) {
    if(!isUndefined(option.error)) {
      option.error();
    }
  }
  if(!isUndefined(option)) {
    if(!isUndefined(option.snackBar)) {
      if (!option.snackBar) {
        if (!isUndefined(resp.message)) {
          useWarningMessage(resp.message);
        }
        if (!isUndefined(resp.notify)) {
          useSuccessMessage(resp.notify);
        }
      }
    }
  } else {
    if (!isUndefined(resp.message)) {
      useWarningMessage(resp.message);
    }
    if (!isUndefined(resp.notify)) {
      useSuccessMessage(resp.notify);
    }
  }
  if (!api.noRedirect) {
    localStorage.removeItem("access_token");
    location.href = "/user/login";
  }
  throw resp;
}
