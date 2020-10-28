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
    private CacheManager cacheManager = new LFUDACacheManager();

    public LocalRouteSkylineManager(DiskManager diskManager) {
        this.diskManager = diskManager;
    }

    public CacheManager cacheManager() {
        return cacheManager;
    }

    public Map<Long, List<Label>> getSubRoutesOnMemory() {
        return subRoutes;
    }

    public double getTotalBytesOfPrimitivesInMemory() {
        double result = 0;
        for (Map.Entry<Long, List<Label>> entry : subRoutes.entrySet()) {
            result += (entry.getValue().size() * Label.getBytesOfPrimitives());
        }
        return result;
    }

    public int getTotalLocalRouteSkylineCountInMemory() {
        int subRouteCount = 0;
        for (Map.Entry<Long, List<Label>> subRouteEntry : subRoutes.entrySet()) {
            subRouteCount += subRouteEntry.getValue().size();
        }
        return subRouteCount;
    }

    public double getBytesOfPrimitivesInMemory(long nodeId) {
        return hasSubRoutesInMemory(nodeId) ? subRoutes.get(nodeId).size() * Label.getBytesOfPrimitives() : 0d;
    }

    public List<Label> get(long nodeId) throws IOException, ClassNotFoundException {

        if (cacheManager instanceof LFUCacheManager || cacheManager instanceof LFUDACacheManager) {
            cacheManager.push(nodeId);
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
        if (hasSubRoutesInMemory(nodeId)) {
            return subRoutes.get(nodeId).size();
        }
        return get(nodeId).size();
    }

    public Label getSubRouteByIndex(long nodeId, int index) throws IOException, ClassNotFoundException, IllegalArgumentException {
        if (index < 0) {
            throw new IllegalArgumentException("Index could not be less than zero.");
        }

        if (hasSubRoutesInMemory(nodeId)){
            return subRoutes.get(nodeId).get(index);
        }

        return get(nodeId).get(index);
    }

    public void removeSubRoute(long nodeId, Label subRoute) throws IOException, ClassNotFoundException, NullPointerException {
        if (subRoute == null) {
            throw new NullArgumentException("subRoute");
        }

        if (hasSubRoutesInMemory(nodeId)) {
            subRoutes.get(nodeId).remove(subRoute);
        } else {
            get(nodeId).remove(subRoute);
        }
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

        List<Label> localSkylineRoutes = get(nodeId);
        for (Label subLabel : localSkylineRoutes) {
            if (label.equals(subLabel)) {
                return false;
            }
        }
        localSkylineRoutes.add(label);

        if (cacheManager instanceof FIFOCacheManager || cacheManager instanceof LRUCacheManager || cacheManager instanceof  MRUCacheManager) {
            cacheManager().push(nodeId);
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


