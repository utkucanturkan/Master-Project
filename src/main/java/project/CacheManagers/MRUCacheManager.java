package project.CacheManagers;

import java.util.Map;

public class MRUCacheManager extends CacheManager {
    private final String NAME = "MRUCacheManager";

    @Override
    public String name() {
        return NAME;
    }

    public void push(long element) {
        incrementIndexByOne();
        elements.put(element, index);
    }

    public Long peek() {
        long mostRecentlyUsedElement = -1;
        int theBiggestIndex = Integer.MIN_VALUE;
        for (Map.Entry<Long, Integer> entry : elements.entrySet()) {
            if (entry.getValue() > theBiggestIndex) {
                theBiggestIndex = entry.getValue();
                mostRecentlyUsedElement = entry.getKey();
            }
        }

        if (mostRecentlyUsedElement < 0) {
            return null;
        }

        elements.remove(mostRecentlyUsedElement);
        return mostRecentlyUsedElement;
    }
}
