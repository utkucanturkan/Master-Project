package project.CacheManagers;

import java.util.Map;

public class LFUCacheManager extends CacheManager {
    private final String NAME = "LFUCacheManager";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void push(long element) {
        //incrementIndexByOne();
        if (elements.containsKey(element)) {
            int newFrequency = elements.get(element) + 1;
            elements.put(element, newFrequency);
        } else {
            elements.put(element, 0);
        }
    }

    public Long peek() {
        long leastFrequentlyUsedElement = -1;
        int theLeastFrequency = Integer.MAX_VALUE;
        for (Map.Entry<Long, Integer> entry : elements.entrySet()) {
            if (entry.getValue() < theLeastFrequency) {
                theLeastFrequency = entry.getValue();
                leastFrequentlyUsedElement = entry.getKey();
            }
        }

        if (leastFrequentlyUsedElement < 0) {
            return null;
        }

        elements.remove(leastFrequentlyUsedElement);

        return leastFrequentlyUsedElement;
    }
}
