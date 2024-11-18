import type {
  DataResp,
  DeviceAllResp,
  DeviceGroup, DeviceGroupBody, DeviceGroupResp,
  DeviceResp, IntegrationBody, IntegrationResp,
  Script, ShowDevice,
  TestScript, TestScriptResult,
  UserInfo,
  UserPutInfo, UserToken
} from '@/type/response'
import { useRequest } from '@/composables/request'
import { useCacheStore } from '@/stores/cache'
import type { Ref } from 'vue'

type PartialPropsOption<T, K extends keyof T> = Partial<Pick<T, K>> & Omit<T, K>;

class Api {

  public async getDevices(): Promise<DeviceAllResp> {
    return await useRequest<DeviceAllResp>({
      path: "/api/v1/device/device",
      method: "GET"
    })
  }

  public async postDevice(body: any): Promise<null> {
    let eui = '';
    if (body.device_type === 'LoRaNode') {
      eui = body.join_parameter.dev_eui;
    } else if (body.device_type === 'LoRaGate') {
      eui = body.eui;
    } else if (body.device_type === 'Snap') {
      eui = body.eui;
    }
    body.eui = eui;
    return await useRequest<null>({
      noRedirect: true,
      path: "/api/v1/device/device",
      method: "POST",
      body
    })
  }
  public async getDownTemplate(device: string): Promise<any[]> {
    return await useRequest<any[]>({
      noRedirect: true,
      path: "/api/v1/device/down/:id/template",
      method: "GET",
      parameter: [device],
    })
  }
  public async deleteDevice(device: string): Promise<null> {
    return await useRequest<null>({
      path: "/api/v1/device/device/:id",
      method: "DELETE",
      parameter: [device]
    })
  }
  public async postDownLink(device: string, data: string): Promise<null> {
    return await useRequest<null>({
      noRedirect: true,
      path: "/api/v1/device/down/:id/down",
      method: "POST",
      parameter: [device],
      body: {
        data
      }
    })
  }
  public async postTemplate(device: string, name: string,  port: number, data: string): Promise<null> {
    return await useRequest<null>({
      noRedirect: true,
      path: "/api/v1/device/down/:id/template",
      method: "POST",
      parameter: [device],
      body: {
        name,
        port,
        data
      }
    })
  }
  public async deleteTemplate(device: string, id: string): Promise<null> {
    return await useRequest<null>({
      noRedirect: true,
      path: "/api/v1/device/down/:device/template/:id",
      method: "DELETE",
      parameter: [device, id],
    })
  }
  public async putDeviceTop(device: string, group: string): Promise<null> {
    return await useRequest<null>({
      path: "/api/v1/device/order/top/:device/:group",
      method: "PUT",
      parameter: [device, group]
    })
  }


  public async getDeviceInfo(deviceID: string): Promise<DeviceResp> {
    return await useRequest<DeviceResp>({
      path: "/api/v1/device/device/:id",
      method: "GET",
      parameter: [deviceID]
    })
  }

  public async getDeviceData(deviceID: string): Promise<DataResp> {
    return await useRequest<DataResp>({
      noRedirect: true,
      path: "/api/v1/data/:id/hour",
      method: "GET",
      parameter: [deviceID]
    })
  }

  public async getUserInfo(): Promise<Ref<UserInfo>> {
    const cache = useCacheStore();
    return await cache.getUserInfo();
  }

  public async getDecodeScript(): Promise<Script[]> {
    return await useRequest<Script[]>({
      path: "/api/v1/decode",
      method: "GET",
    })
  }

  public async postDecodeScript(script: PartialPropsOption<Script, "id">): Promise<Script> {
    return await useRequest<Script>({
      path: "/api/v1/decode",
      method: "POST",
      body: script,
      noRedirect: true,
    })
  }

  public async postTestScript(script: TestScript): Promise<TestScriptResult> {
    return await useRequest<TestScriptResult>({
      path: "/api/v1/decode/test",
      method: "POST",
      body: script
    })
  }
  public async deleteDecodeScript(scriptId: string): Promise<null> {
    return await useRequest<null>({
      path: "/api/v1/decode/:id",
      method: "DELETE",
      parameter: [scriptId]
    })
  }

  public async putDeviceInfo(device: string, info: any): Promise<null> {
    return await useRequest<null>({
      path: "/api/v1/device/device/:id",
      method: "PUT",
      parameter: [device],
      body: info
    })
  }
  public async putDecodeScript(device: string, scriptId: string): Promise<null> {
    return await useRequest<null>({
      path: "/api/v1/device/device/:id",
      method: "PUT",
      parameter: [device],
      body: {
        script: scriptId
      }
    })
  }
  public async postDataMap(device: string, map: any): Promise<null> {
    return await useRequest<null>({
      noRedirect: true,
      path: "/api/v1/device/map/:id",
      method: "POST",
      parameter: [device],
      body: map
    })
  }
  public async getDataMap(device: string): Promise<any> {
    return await useRequest<null>({
      path: "/api/v1/device/map/:id",
      method: "GET",
      parameter: [device],
    })
  }
  public async getProductInfo(): Promise<any> {
    return await useRequest<null>({
      path: "/api/v1/device/product",
      method: "GET",
    })
  }
  public async putUserInfo(info: UserPutInfo): Promise<null> {
    if (!info.old_password || !info.password ) {
      throw "password can`t empty";
    }
    return await useRequest<null>({
      path: "/api/v1/user/info",
      method: "PUT",
      body: info,
    })
  }

  public async postUserPicture(picture: FormData): Promise<null> {
    return await useRequest<null>({
      path: "/api/v1/user/picture",
      method: "POST",
      body: picture,
    })
  }

  public async postUserLogin(username: string, password: string): Promise<UserToken> {
    return await useRequest<UserToken>({
      path: '/api/v1/user/login',
      method: 'POST',
      body: { username, password},
      noRedirect: true
    }, {
      token: false
    })
  }

  public async postUserSignup(username: string, password: string, email: string): Promise<string> {
    return await useRequest<string>({
      path: '/api/v1/user/signup',
      method: 'POST',
      body: { username, password, email },
    }, {
      token: false
    })
  }
  public async getActiveEmail(token: string): Promise<string> {
    return await useRequest<string>({
      path: "/api/v1/user/verify/:token",
      method: "GET",
      parameter: [token]
    }, {
      snackBar: false
    })
  }
  public async getGroups(): Promise<DeviceGroup[]> {
    return await useRequest<DeviceGroup[]>({
      path: "/api/v1/device/group",
      method: "GET"
    })
  }

  public async putGroup(group: string, body: { remove?: string[], devices?: string[], name?: string, description?: string }): Promise<null> {
    return await useRequest<null>({
      path: "/api/v1/device/group/:group_id",
      method: "PUT",
      parameter: [group],
      body: body
    })
  }
  public async getGroupInfo(groupId: string): Promise<DeviceGroupResp> {
    return await useRequest<DeviceGroupResp>({
      path: "/api/v1/device/group/:id",
      method: "GET",
      parameter: [groupId]
    })
  }

  public async postGroup(body: DeviceGroupBody): Promise<null> {
    return await useRequest<null>({
      path: "/api/v1/device/group",
      method: "POST",
      body
    })
  }

  public async getIntegration(): Promise<IntegrationResp> {
    return await useRequest<IntegrationResp>({
      path: "/api/v1/integration/mqtt",
      method: "GET"
    })
  }

  public async postIntegration(body: IntegrationBody): Promise<null> {
    return await useRequest<null>({
      noRedirect: true,
      path: "/api/v1/integration/mqtt",
      method: "POST",
      body: body
    })
  }

  public async getShowDevice(): Promise<ShowDevice> {
    return await useRequest<ShowDevice>({
      path: "/api/v1/show/device",
      method: "GET",
    })
  }
}


export const api = new Api();