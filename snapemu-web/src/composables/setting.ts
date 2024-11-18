import { useSettingStore } from '@/stores/setting'
import { computed } from 'vue'

export const useSideNavigation = () => {
  const setting = useSettingStore();
  return computed({
      get() {
        return setting.side;
      },
      set(flag) {
        setting.updateSide(flag)
      }
  }
  )
};
