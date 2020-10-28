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
        incrementIndexByOne();
        elements.put(element, index);
    }

    public Long peek() {

        // TODO: compute the least recently used element
        // find the node that has the least value on the dictionary

        long leastRecentlyUsedElement = -1;
        int theLeastIndex = index;
        for(Map.Entry<Long, Integer> entry: elements.entrySet()){
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
