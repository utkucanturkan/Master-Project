package project;

import project.CacheManagers.*;
import org.apache.commons.lang.NullArgumentException;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class LocalRouteSkylineManager {
    private Map<Long, List<Label>> subRoutes = new HashMap<>();
    private DiskManager diskManager;
    private CacheManager cacheManager = new LFUCacheManager();

    public LocalRouteSkylineManager(DiskManager diskManager) {
        this.diskManager = diskManager;
    }

    public CacheManager cacheManager() {
        return cacheManager;
    }

    public Map<Long, List<Label>> getSubRoutesOnMemory() {
        return subRoutes;
    }

    public double getTotalBytesOfPrimitives() {
        double result = 0;
        for (Map.Entry<Long, List<Label>> entry : subRoutes.entrySet()) {
            result += (entry.getValue().size() * Label.getBytesOfPrimitives());
        }
        return result;
    }

    public int getTotalSubRouteCount() {
        int subRouteCount = 0;
        for (Map.Entry<Long, List<Label>> subRouteEntry : subRoutes.entrySet()) {
            subRouteCount += subRouteEntry.getValue().size();
        }
        return subRouteCount;
    }

    public double getBytesOfPrimitives(long nodeId) {
        return hasSubRoutesInMemory(nodeId) ? subRoutes.get(nodeId).size() * Label.getBytesOfPrimitives() : 0d;
    }

    public List<Label> get(long nodeId) throws IOException, ClassNotFoundException {

        if (cacheManager instanceof LFUCacheManager) {
            cacheManager.addElement(nodeId);
        }

        if (!hasSubRoutesInMemory(nodeId)) {
            List<Label> subRoutesOfNode = diskManager.getFromDisc(this, nodeId);
            subRoutes.put(nodeId, subRoutesOfNode == null ? new LinkedList<>() : subRoutesOfNode);
        } else {
            cacheManager.incrementHitCount();
        }

        return subRoutes.get(nodeId);
    }

    public int getSizeOfSubRoutes(long nodeId) throws IOException, ClassNotFoundException {
        return get(nodeId).size();
    }

    public Label getSubRouteByIndex(long nodeId, int index) throws IOException, ClassNotFoundException, IllegalArgumentException {
        if (index < 0) {
            throw new IllegalArgumentException("Index could not be less than zero.");
        }
        return get(nodeId).get(index);
    }

    public void removeSubRoute(long nodeId, Label subRoute) throws IOException, ClassNotFoundException, NullPointerException {
        if (subRoute == null) {
            throw new NullArgumentException("subRoute");
        }
        get(nodeId).remove(subRoute);
    }

    public void removeAllSubRouteFromMemory(long nodeId) {
        if (hasSubRoutesInMemory(nodeId)) {
            subRoutes.remove(nodeId);
        }
    }

    public boolean add(long nodeId, Label label) throws IOException, ClassNotFoundException, NullPointerException {
        if (label == null) {
            throw new NullArgumentException("label");
        }
        for (Label subLabel : get(nodeId)) {
            if (label.equals(subLabel)) {
                return false;
            }
        }
        get(nodeId).add(label);

        if (cacheManager instanceof FIFOCacheManager || cacheManager instanceof LRUCacheManager || cacheManager instanceof  MRUCacheManager) {
            cacheManager().addElement(nodeId);
        }

        diskManager.saveToDisc(this);
        return true;
    }

    public boolean hasSubRoutesInMemory(long nodeId) {
        return subRoutes.containsKey(nodeId);
    }

    public boolean hasSubRoutes(long nodeId) throws IOException, ClassNotFoundException {
        return getSizeOfSubRoutes(nodeId) > 0;
    }
}


