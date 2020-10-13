package project;

import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.Map;

public class PerformanceReporter {
    private long startTime = 0, endTime = 0;
    private Runtime runtime;
    private long startMemory = 0, endMemory = 0;
    private boolean reportRouteSkyline = true;
    private boolean reportExecutionTime = true;
    private boolean reportMemoryUsage = true;


    public void printReport(Node startNode, Node destinationNode, List<String> propertyKeys, List<Label> routeSkylines, LocalRouteSkylineManager localRouteSkylineManager) {
        printHeader(startNode, destinationNode);
        reportExecutionTime();
        //reportMemoryUsage();
        //reportSubRouteSkylinesMemoryUsage(false, subRouteSkyline);
        reportRouteSkylines(routeSkylines, propertyKeys);
        printCacheManagerReport(localRouteSkylineManager);
        printFooter(routeSkylines);
    }

    private void printHeader(Node startNode, Node destinationNode) {
        System.out.println("---------- Node-" + startNode.getProperty("name") + " to Node-" + destinationNode.getProperty("name") + " ----------");
    }

    private void printFooter(List<Label> routeSkylines) {
        System.out.println("----- " + routeSkylines.size() + " route(s) has been found. -----\n\n");
    }

    private void reportRouteSkylines(List<Label> routeSkylines, List<String> propertyKeys) {
        if (reportRouteSkyline) {
            System.out.println("Paths:");
            routeSkylines.forEach(route -> {
                System.out.print("Path - ");
                for (int criteriaIndex = 0; criteriaIndex < propertyKeys.size(); criteriaIndex++) {
                    System.out.print("Criteria-" + (criteriaIndex + 1) + ": " + route.getCosts().get(criteriaIndex) + " ");
                }
                System.out.println();
            });
        }
    }

    /* Cache Manager */
    public void printCacheManagerReport(LocalRouteSkylineManager localRouteSkylineManager) {
        System.out.println(localRouteSkylineManager.cacheManager().toString());
    }


    /* Execution Time */
    public void startExecutionTime() {
        startTime = System.currentTimeMillis();
    }

    public void endExecutionTime() {
        endTime = System.currentTimeMillis();
    }

    private void reportExecutionTime() {
        if (reportExecutionTime) {
            //System.out.println("Execution time in milliseconds: " + (endTime - startTime));
            System.out.print((endTime-startTime) + ", ");
        }
    }

    /* Memory Usage */
    public void startMemoryUsage() {
        runtime = Runtime.getRuntime();
        runtime.gc();
        startMemory = runtime.totalMemory() - runtime.freeMemory();
    }

    public void endMemoryUsage() {
        endMemory = runtime.totalMemory() - runtime.freeMemory();
    }

    private void reportMemoryUsage() {
        System.out.println((endMemory - startMemory) + ", ");
    }


    private void reportSubRouteSkylinesMemoryUsage(boolean showEachSubRouteMemoryUsage, LocalRouteSkylineManager localRouteSkylineManager) {
        System.out.println("Total Memory Usage of all subroutes; " + localRouteSkylineManager.getTotalBytesOfPrimitives());
        if (showEachSubRouteMemoryUsage) {
            for (Map.Entry<Long, List<Label>> entry : localRouteSkylineManager.getSubRoutesOnMemory().entrySet()) {
                System.out.println("Node-" + entry.getKey() + " SubrouteSkyline(s) memory usage; " +
                        localRouteSkylineManager.getBytesOfPrimitives(entry.getKey()));
            }
        }
    }
}


