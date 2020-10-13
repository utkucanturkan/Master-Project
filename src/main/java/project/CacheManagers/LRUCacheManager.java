package project.CacheManagers;

public class LRUCacheManager extends CacheManager {
    private final String NAME = "LRUCacheManager";

    @Override
    public String name() {
        return NAME;
    }
    
    @Override
    public void addElement(long element) {
        incrementIndexByOne();
        elements.put(element, index);
    }

    public Long getNextElement() {

        // TODO: compute the least recently used element
        long leastRecentlyUsedElement = 0;

        elements.remove(leastRecentlyUsedElement);

        return leastRecentlyUsedElement;
    }
}
