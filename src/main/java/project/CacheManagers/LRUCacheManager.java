package project.CacheManagers;

import java.util.Map;

public class LRUCacheManager extends CacheManager {
    private final String NAME = "LRUCacheManager";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void push(long element) {
        incrementIndexByOne();
        elements.put(element, index);
    }

    public Long peek() {
        long leastRecentlyUsedElement = -1;
        int theLeastIndex = Integer.MAX_VALUE;
        for (Map.Entry<Long, Integer> entry : elements.entrySet()) {
            if (entry.getValue() < theLeastIndex) {
                theLeastIndex = entry.getValue();
                leastRecentlyUsedElement = entry.getKey();
            }
        }

        if (leastRecentlyUsedElement < 0) {
            return null;
        }

        elements.remove(leastRecentlyUsedElement);

        return leastRecentlyUsedElement;
    }
}
