import MessageAlert from '@/components/widget/MessageAlert.vue'
import { isUndefined } from 'lodash-es'
import { createVNode, render } from 'vue'

let container: undefined | HTMLDivElement;

type MessageEntity = {
  successAlert: (message: string) => number,
  warningAlert: (message: string) => number,
  clearAlert: (handler: number) => void
}

const sendMessage = (callback: (entity: MessageEntity) => number) => {
  if (isUndefined(container)) {
    container = document.createElement('div');
    document.body.appendChild(container);
  }
  const vNode = createVNode(MessageAlert);
  render(vNode, container);
  const entity = vNode.component?.exposed as MessageEntity;
  const handler = callback(entity);
  return () => {
    entity.clearAlert(handler);
  }
}

export const useSuccessMessage = (message: string) => sendMessage(entity => entity.successAlert(message))

export const useWarningMessage = (message: string) => sendMessage(entity => entity.warningAlert(message))
