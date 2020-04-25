package com.eozd;

import java.util.Map;

public class Stats {
    private Map<Long, Integer> minuteToCount;

    public Stats() {

    }

    public Stats(Map<Long, Integer> minuteToCount) {
        this.minuteToCount = minuteToCount;
    }

    public Map<Long, Integer> getMinuteToCount() {
        return minuteToCount;
    }

    public void setMinuteToCount(Map<Long, Integer> minuteToCount) {
        this.minuteToCount = minuteToCount;
    }
}
