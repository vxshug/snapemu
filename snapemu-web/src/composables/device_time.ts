import { useT } from '@/composables/i18n'

export const useActiveTime = () => {
  const t = useT();
  return (time: number) => {
      const second = Math.round((new Date().getTime() - time) / 1000);
      if (second < 60) {
        return t("page.dashboard.device.time.second", { time: second });
      }
      if (second < 3600) {
        const minute = Math.round(second / 60);
        return t("page.dashboard.device.time.minute", { time: minute });
      }
      if (second < 86400) {
        const hour = Math.round(second / 3600);
        return t("page.dashboard.device.time.hour", { time: hour });
      }
      if (second < 1209600) {
        const day = Math.round(second / 86400);
        return t("page.dashboard.device.time.day", { time: day });
      }
      return t("page.dashboard.device.time.timeout");
  }
}