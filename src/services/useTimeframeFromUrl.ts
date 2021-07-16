import { isTimeframe, Timeframe } from '@/services/timeframe';
import { usePreference } from '@/services/usePreference';
import { Ref, watch } from 'vue';
import { useRoute } from 'vue-router';

const NEAR_TIMEFRAME_PREFERENCES_KEY = 'near-timeframe';

export function useTimeframeFromUrl(): Ref<Timeframe> {
  const timeframe = usePreference<Timeframe>(
    NEAR_TIMEFRAME_PREFERENCES_KEY,
    Timeframe.MONTH,
  );
  const route = useRoute();

  watch(
    route,
    currentRoute => {
      const t = currentRoute.query['t'];
      if (t && isTimeframe(t)) {
        timeframe.value = t;
      }
    },
    {
      immediate: true,
    },
  );

  return timeframe;
}
