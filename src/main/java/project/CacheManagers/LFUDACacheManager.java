package project.CacheManagers;
import java.util.*;

public class LFUDACacheManager extends CacheManager {
    private final String NAME = "LFUDACacheManager";

    private Map<Integer, LinkedList<Long>> cache = new HashMap<>();

    private int age = 0;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void push(long element) {
        boolean isElementCached = false;
        int priorityKey = 1 + age;
        int deletedEntry = -1;
        for (Map.Entry<Integer, LinkedList<Long>> entry : cache.entrySet()) {
            for (Long node: entry.getValue()) {
                if (node == element) {
                    isElementCached = true;
                    priorityKey = 1 + entry.getKey();
                    element = node;
                    break;
                }
            }
            if (isElementCached) {
                entry.getValue().remove(element);
                if (entry.getValue().isEmpty()) {
                    deletedEntry = entry.getKey();
                }
                break;
            }
        }

        if (deletedEntry > -1) {
            cache.remove(deletedEntry);
        }

        if (cache.containsKey(priorityKey)) {
            cache.get(priorityKey).add(element);
        } else {
            LinkedList<Long> newLinkedList = new LinkedList<>();
            newLinkedList.add(element);
            cache.put(priorityKey, newLinkedList);
        }
    }

    @Override
    public Long peek() {
        if (!cache.isEmpty()) {
            int smallestFrequency = Integer.MAX_VALUE;
            for (Map.Entry<Integer, LinkedList<Long>> entry : cache.entrySet()) {
                if (entry.getKey() < smallestFrequency) {
                    smallestFrequency = entry.getKey();
                }
            }
            age = smallestFrequency;
            LinkedList<Long> list = cache.get(smallestFrequency);
            long removedElement = list.removeLast();
            if (list.isEmpty()) {
                cache.remove(smallestFrequency);
            }
            return removedElement;
        }
        return null;
    }
}
