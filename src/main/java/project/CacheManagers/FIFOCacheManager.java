package project.CacheManagers;

public class FIFOCacheManager extends CacheManager {
    private final String NAME = "FIFOCacheManager";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void addElement(long element) {
        incrementIndexByOne();
        if (!elements.containsKey(element)) {
            elements.put(element, index);
        }
    }

    @Override
    public Long getNextElement() {
        // TODO: compute the lowest index and get the value

        long element = 0;

        elements.remove(element);

        return element;
    }
}
