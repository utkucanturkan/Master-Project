package project.CacheManagers;

import java.util.HashMap;
import java.util.Map;

public abstract class CacheManager {
    protected int index = 0;
    protected Map<Long, Integer> elements = new HashMap<>();
    protected float hitCount = 0;
    protected float missCount = 0;

    public void incrementHitCount() {
        hitCount += 1;
    }

    public void incrementMissCount() {
        missCount += 1;
    }

    public float hitRatio() {
        return (hitCount <= 0 && missCount <= 0) ? 0 : hitCount / (hitCount + missCount);
    }

    public float missRatio() {
        return 1 - hitRatio();
    }

    protected void incrementIndexByOne() {
        index += 1;
    }

    public abstract String name();

    public abstract void addElement(long element);

    public abstract Long getNextElement();

    @Override
    public String toString() {
        return name() + " {" +
                "hitRatio= " + hitRatio() +
                ", missRatio= " + missRatio() +
                '}';
    }
}
