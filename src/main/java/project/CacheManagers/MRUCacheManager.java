package project.CacheManagers;

import java.util.Map;

public class MRUCacheManager extends CacheManager {
    private final String NAME = "MRUCacheManager";

    @Override
    public String name() {
        return NAME;
    }

    public void addElement(long element) {
        incrementIndexByOne();
        elements.put(element, index);
    }

    public Long getNextElement() {

        // TODO: compute the most recently used element
        // find the node that has the biggest value on the dictionary

        long mostRecentlyUsedElement = -1;
        for(Map.Entry<Long, Integer> entry: elements.entrySet()) {
            if (entry.getValue() == index) {
                mostRecentlyUsedElement = entry.getKey();
                break;
            }
        }

        if (mostRecentlyUsedElement < 0) {
            return null;
        }

        elements.remove(mostRecentlyUsedElement);
        return mostRecentlyUsedElement;
    }
}
