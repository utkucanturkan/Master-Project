package project;

import project.LowerBoundsCalculators.ParetoPrep;
import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;

public class MultiPreferencePathPlannerARSC {
    @Context
    public GraphDatabaseService db;

    @Procedure(value = "dbis.ARSC", name = "dbis.ARSC", mode = Mode.READ)
    @Description("Advanced Route Skyline Computation from specified start node to destination node regarding to the relationship property keys")
    public Stream<RouteSkyline> ARSC(@Name("start") Node start,
                                     @Name("destination") Node destination,
                                     @Name("relationshipPropertyKeys") List<String> relationshipPropertyKeys,
                                     @Name("resourceConstraints") Map<String, Double> resourceConstraints,
                                     @Name("labelConstraint") List<String> labelConstraints) throws IOException, ClassNotFoundException {
        long startNodeId = start.getId();
        long destinationNodeId = destination.getId();

        DiskManager diskManager = new DiskManager(0); //0.00001f
        LocalRouteSkylineManager localRouteSkylinesManager = new LocalRouteSkylineManager(diskManager);
        PerformanceReporter performanceReporter = new PerformanceReporter();

        //performanceReporter.startMemoryUsage();
        performanceReporter.startExecutionTime();

        PriorityQueue<Long> nodeQueue = new PriorityQueue<>(
                new Comparator<Long>() {
                    @Override
                    public int compare(Long lhsNodeId, Long rhsNodeId) {
                        List<Label> lhsSubRoutes = null;
                        List<Label> rhsSubRoutes = null;
                        try {
                            lhsSubRoutes = localRouteSkylinesManager.get(lhsNodeId);
                            rhsSubRoutes = localRouteSkylinesManager.get(rhsNodeId);
                        } catch (IOException | ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                        if (lhsSubRoutes != null && rhsSubRoutes != null) {
                            for (Label lhsNodeSubRoute : lhsSubRoutes) {
                                int higherPrefenceCount = 0;
                                for (Label rhsNodeSubRoute : rhsSubRoutes) {
                                    if (lhsNodeSubRoute.preferenceFunction() > rhsNodeSubRoute.preferenceFunction()) {
                                        higherPrefenceCount += 1;
                                    } else {
                                        break;
                                    }
                                }
                                if (higherPrefenceCount == rhsSubRoutes.size()) {
                                    return 1;
                                }
                            }
                        }
                        return 0;
                    }
                }
        );
        List<Label> routeSkylines = new LinkedList<>();

        // Pareto-prep Method to compute lower bounds
        ParetoPrep paretoPrep = new ParetoPrep(startNodeId, destinationNodeId, relationshipPropertyKeys, resourceConstraints, labelConstraints);
        Vector<Double> pLb = paretoPrep.execute(db);

        //MultiDijkstra md = new MultiDijkstra(db, destinationNodeId, propertyKeys);
        //Vector<Double> pLb = md.lb(startLabel);

        localRouteSkylinesManager.add(startNodeId, new Label(startNodeId, destinationNodeId, relationshipPropertyKeys, resourceConstraints, labelConstraints));
        nodeQueue.add(startNodeId);

        while (!nodeQueue.isEmpty()) {
            long activeNodeId = nodeQueue.peek();
            for (int subRouteSkylineIndex = 0; subRouteSkylineIndex < localRouteSkylinesManager.getSizeOfSubRoutes(activeNodeId); subRouteSkylineIndex++) {
                Label subRoute = localRouteSkylinesManager.getSubRouteByIndex(activeNodeId, subRouteSkylineIndex);
                // Pruning based on forward estimation
                // compute attribute vector p.lb[] -> lower bounding cost estimations for each path attribute
                boolean plb_isDominated = false;
                for (Label route : routeSkylines) {
                    if (route.doesDominate(pLb)) {
                        plb_isDominated = true;
                        localRouteSkylinesManager.removeSubRoute(activeNodeId, subRoute);
                        subRouteSkylineIndex--;
                        break;
                    }
                }
                if (!plb_isDominated) {                                             // if sub-route is not processed yet
                    for (Label subRoutePrime : subRoute.expandARSC(db, nodeQueue, localRouteSkylinesManager)) {           // expand actual path p by one hop (in each direction)
                        if (subRoutePrime.getLastNodeId() == destination.getId() && !subRoutePrime.isDominatedInRouteList(routeSkylines)) {                 // route completed
                            routeSkylines.add(subRoutePrime);                       // path must be a skyline route
                            for (int skylineRouteIndex = 0; skylineRouteIndex < routeSkylines.size(); skylineRouteIndex++) {
                                Label route = routeSkylines.get(skylineRouteIndex);
                                if (route.isDominatedBy(subRoutePrime)) {
                                    routeSkylines.remove(route);                    // update route skyline
                                    skylineRouteIndex--;
                                }
                            }
                        } else {
                            // route is not completed and path must be further expanded
                            // Pruning based on sub-route skyline criterion (Pruning Criterion II)
                            long subRoutePrimeLastNodeId = subRoutePrime.getLastNodeId();
                            if (!subRoutePrime.isDominatedInRouteList(localRouteSkylinesManager.get(subRoutePrimeLastNodeId)) && localRouteSkylinesManager.add(subRoutePrimeLastNodeId, subRoutePrime)) {
                                for (int vNextSubRouteSkylineIndex = 0; vNextSubRouteSkylineIndex < localRouteSkylinesManager.getSizeOfSubRoutes(subRoutePrimeLastNodeId); vNextSubRouteSkylineIndex++) {
                                    Label route = localRouteSkylinesManager.getSubRouteByIndex(subRoutePrimeLastNodeId, vNextSubRouteSkylineIndex);
                                    if (route.isDominatedBy(subRoutePrime)) {
                                        localRouteSkylinesManager.removeSubRoute(subRoutePrimeLastNodeId, route);
                                        vNextSubRouteSkylineIndex--;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            nodeQueue.remove(activeNodeId);
        }
        diskManager.deleteAllFiles();

        performanceReporter.endExecutionTime();
        //performanceReporter.endMemoryUsage();

        performanceReporter.printReport(start, destination, relationshipPropertyKeys, routeSkylines, localRouteSkylinesManager);
        return routeSkylines.stream().map(RouteSkyline::new);
    }

    public static class RouteSkyline {
        public String route;

        public RouteSkyline(Label p) {
            this.route = p.toString();
        }
    }
}