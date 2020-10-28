package project;

import java.io.*;
import java.util.List;
import java.util.Vector;

public class DiskManager {
    private final String SUBROUTE_DIRECTORY_PATH = "src" + File.separator + "subRoutes";
    private final String SUBROUTE_OF_NODE_FILE_PATH = SUBROUTE_DIRECTORY_PATH + File.separator + "Node-";
    private float maximumMemoryPercentageForSubroute;
    private Vector<Long> savedNodesToDisc = new Vector<>();
    private int localSkylineLimit;

    /*
    public DiskManager(float memoryPercentage) {
        setMaximumMemoryPercentage(memoryPercentage);
    }
    */

    public DiskManager(int localSkylineLimit) {
        this.localSkylineLimit = localSkylineLimit;
    }

    public void setMaximumMemoryPercentage(float percentage) {
        maximumMemoryPercentageForSubroute = (percentage > 0f) ? percentage : 5;
    }

    public float getMaximumMemoryPercentageForSubroute() {
        return maximumMemoryPercentageForSubroute;
    }

    private int getLocalRouteSkylineLimit() {
        return localSkylineLimit;
        //return (float) ((Runtime.getRuntime().freeMemory() * getMaximumMemoryPercentageForSubroute() / 100) / Label.getBytesOfPrimitives());
    }

    private void setNodesAsSaved(LocalRouteSkylineManager localRouteSkylineManager, List<Long> nodeIds) {
        for (long nodeId : nodeIds) {
            localRouteSkylineManager.removeAllSubRouteFromMemory(nodeId);
            if (!isNodeSaved(nodeId)) {
                savedNodesToDisc.add(nodeId);
            }
        }
    }

    private void setNodeAsSaved(long nodeId) {
        if (!isNodeSaved(nodeId)) {
            savedNodesToDisc.add(nodeId);
        }
    }

    private void deleteSavedNode(long nodeId) {
        if (isNodeSaved(nodeId)) {
            savedNodesToDisc.remove(nodeId);
        }
    }

    private boolean isNodeSaved(long nodeId) {
        return savedNodesToDisc.contains(nodeId);
    }

    public void saveToDisc(LocalRouteSkylineManager localRouteSkylineManager, int additionalSubRouteCount) throws IOException, IllegalArgumentException {
        if (additionalSubRouteCount < 0) {
            throw new IllegalArgumentException("AdditionalSubRouteCount could not less than zero.");
        }
        if (localRouteSkylineManager.getTotalLocalRouteSkylineCountInMemory() + additionalSubRouteCount > getLocalRouteSkylineLimit()) {
            final File SUBROUTES_OF_NODES_DIR = new File(SUBROUTE_DIRECTORY_PATH);
            if (!SUBROUTES_OF_NODES_DIR.exists()) {
                SUBROUTES_OF_NODES_DIR.mkdir();
            }

            do {
                Long nodeId = localRouteSkylineManager.cacheManager().peek();

                // If the cache memory is empty
                if (nodeId == null) {
                    break;
                }

                File subRouteFile = new File(SUBROUTE_OF_NODE_FILE_PATH + nodeId);
                FileOutputStream fos = new FileOutputStream(subRouteFile, false);
                ObjectOutputStream oos = new ObjectOutputStream(fos);
                if (!subRouteFile.exists()) {
                    subRouteFile.createNewFile();
                }
                oos.writeObject(localRouteSkylineManager.getSubRoutesOnMemory().get(nodeId));
                oos.close();
                fos.close();

                // remove the node from cache or main memory
                localRouteSkylineManager.removeAllSubRouteFromMemory(nodeId);
                setNodeAsSaved(nodeId);
            } while (localRouteSkylineManager.getTotalLocalRouteSkylineCountInMemory() > 0
                    && localRouteSkylineManager.getTotalLocalRouteSkylineCountInMemory() + additionalSubRouteCount > getLocalRouteSkylineLimit());
        }
    }

    public void saveToDisc(LocalRouteSkylineManager localRouteSkylineManager) throws IOException, IllegalArgumentException {
        saveToDisc(localRouteSkylineManager, 0);
    }

    public List<Label> getFromDisc(LocalRouteSkylineManager localRouteSkylineManager, long nodeId) throws IOException, ClassNotFoundException {
        if (isNodeSaved(nodeId)) {
            localRouteSkylineManager.cacheManager().incrementMissCount();
            File subRouteFile = new File(SUBROUTE_OF_NODE_FILE_PATH + nodeId);
            if (subRouteFile.exists()) {

                FileInputStream fis = new FileInputStream(subRouteFile);
                ObjectInputStream ois = new ObjectInputStream(fis);
                List<Label> subRoutes = (List<Label>) ois.readObject();
                ois.close();
                fis.close();
                subRouteFile.delete();

                saveToDisc(localRouteSkylineManager, subRoutes.size());
                deleteSavedNode(nodeId);

                return subRoutes;
            }
        }
        return null;
    }

    public void deleteAllFiles() throws IOException {
        org.neo4j.io.fs.FileUtils.deleteRecursively(new File(SUBROUTE_DIRECTORY_PATH));
        //FileUtils.deleteDirectory(new File(SUBROUTE_DIRECTORY_PATH));
    }

}