package project.CacheManagers;

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
        long mostRecentlyUsedElement = 0;

        elements.remove(mostRecentlyUsedElement);

        return mostRecentlyUsedElement;
    }
}
