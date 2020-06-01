package project;

import org.neo4j.graphalgo.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

public class MultiPreferencePathPlanner {
    private static List<String> propertyKeys;
    private static Node startNode;
    private static Node destinationNode;
    private static Map<Long, Map<String, Map<Long, Double>>> distanceEstimations = new HashMap<>();
    private static SubRouteSkyline subRouteSkyline = new SubRouteSkyline();
    private static PriorityQueue<Node> nodeQueue;

    @Context
    public GraphDatabaseService db;
    @Context
    public Log log;

    // DEFINITION 2
    private double cost(Path path, String propertyKey) {
        double cost = 0d;
        if (path.relationships() != null) {
            for (Relationship e : path.relationships()) {
                cost += Double.parseDouble(e.getProperty(propertyKey).toString());
            }
        }
        return cost;
    }

    // DEFINITION 3
    private double preferenceFunction(Path path) {
        double result = 0d;
        for (String properyKey : propertyKeys) {
            result += cost(path, properyKey);
        }
        return result;
    }

    // DEFINITION 5
    private double networkDistanceEstimation(Node source, Node target, String propertyKey) {
        // Stores distances from node to another nodes in the graph (reference nodes)
        Map<Long, Double> distancesFromSource = getNetworkDistance(source, propertyKey);
        Map<Long, Double> distancesFromTarget = getNetworkDistance(target, propertyKey);
        Vector<Double> results = new Vector<>();
        for (Map.Entry<Long, Double> sourceEntry : distancesFromSource.entrySet()) {
            for (Map.Entry<Long, Double> targetEntry : distancesFromTarget.entrySet()) {
                // if distances are through same nodes ex; vs -> N, vt -> N
                if (sourceEntry.getKey() == targetEntry.getKey()) {
                    // subtraction from source to target distance and take absolute of it
                    // if sourceDistance or targetDistance equals 0, set the network estimation distance 0
                    results.add(Math.abs(sourceEntry.getValue() - targetEntry.getValue()));
                    break;
                }
            }
        }
        // get and return the maximum value
        return Collections.max(results);
    }

    private Map<Long, Double> getNetworkDistance(Node node, String propertyKey) {
        if (distanceEstimations.containsKey(node.getId())) {
            if (!distanceEstimations.get(node.getId()).containsKey(propertyKey)) {
                distanceEstimations.get(node.getId()).put(propertyKey, calculateNetworkDistance(node, propertyKey));
            }
        } else {
            Map<String, Map<Long, Double>> propertyDistance = new HashMap<>();
            propertyDistance.put(propertyKey, calculateNetworkDistance(node, propertyKey));
            distanceEstimations.put(node.getId(), propertyDistance);
        }
        return distanceEstimations.get(node.getId()).get(propertyKey);
    }

    private Map<Long, Double> calculateNetworkDistance(Node node, String propertyKey) {
        // Store cost of shortest path w.r.t nodes
        Map<Long, Double> networkDistances = new LinkedHashMap<>();
        // Apply dijkstra according to attribute/weightIndex
        PathFinder<WeightedPath> finder = GraphAlgoFactory.dijkstra(PathExpanders.forDirection(Direction.INCOMING), propertyKey);
        // db.getAllNodes might be not efficient
        for (Node graphNode : db.getAllNodes()) {
            WeightedPath p = finder.findSinglePath(graphNode, node);
            networkDistances.put(graphNode.getId(), (p == null) ? 0d : p.weight());
        }
        return networkDistances;
    }

    // DEFINITION 7
    private boolean isDominatedBy(Path lhsPath, Path rhsPath) {
        if (lhsPath == null || rhsPath == null) return false;
        if (lhsPath.relationships() == null || rhsPath.relationships() == null) return false;
        if (lhsPath.equals(rhsPath)) return false;
        int dominatedWeightCount = 0;
        for (String propertyKey : propertyKeys) {
            if (cost(lhsPath, propertyKey) > cost(rhsPath, propertyKey)) {
                dominatedWeightCount++;
            }
        }
        return dominatedWeightCount == propertyKeys.size();
    }

    private boolean isDominatedBy(Vector<Double> vector, Path path) {
        if (vector == null || path == null) return false;
        if (vector.size() == 0 || path.relationships() == null) return false;
        int dominatedWeightCount = 0, index = 0;
        for (String propertyKey : propertyKeys) {
            if (vector.get(index) > cost(path, propertyKey)) {
                dominatedWeightCount++;
            }
            index += 1;
        }
        return dominatedWeightCount == propertyKeys.size();
    }

    private Vector<Double> attr(Path path) {
        Vector<Double> attributeVectors = new Vector<>();
        for (String propertyKey : propertyKeys) {
            attributeVectors.add(cost(path, propertyKey));
        }
        return attributeVectors;
    }

    private Vector<Double> lb(Path path) {
        Vector<Double> lb = new Vector<>();
        int propertyIndex = 0;
        for (String propertyKey : propertyKeys) {
            lb.add(attr(path).get(propertyIndex) + networkDistanceEstimation(path.endNode(), destinationNode, propertyKey));
            propertyIndex++;
        }
        return lb;
    }

    private boolean checkRouteIsDominatedInRouteList(Path route, List<Path> routeList) {
        for (Path subRoute : routeList) {
            if (isDominatedBy(route, subRoute)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasCycle(Path path) {
        if (path.relationships() != null) {
            for (Relationship lhsRelationShip : path.relationships()) {
                for (Relationship rhsRelationShip : path.relationships()) {
                    if (!lhsRelationShip.equals(rhsRelationShip) &&
                            (lhsRelationShip.getEndNode().equals(rhsRelationShip.getEndNode()) || lhsRelationShip.getEndNode().equals(path.startNode()))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private List<Path> expandBRSC(Path path) {
        List<Path> expandedPaths = new LinkedList<>();
        for (Relationship r : (Iterable<Relationship>) PathExpanders.forDirection(Direction.OUTGOING).expand(path, BranchState.NO_STATE)) {
            Path p = new Path() {
                @Override
                public Node startNode() {
                    return path.startNode();
                }

                @Override
                public Node endNode() {
                    return r.getEndNode();
                }

                @Override
                public Relationship lastRelationship() {
                    return r;
                }

                @Override
                public Iterable<Relationship> relationships() {
                    List<Relationship> relationships = new LinkedList<>();
                    if (path.relationships() != null) {
                        for (Relationship relationship : path.relationships()) {
                            relationships.add(relationship);
                        }
                    }
                    relationships.add(r);
                    return relationships;
                }

                @Override
                public Iterable<Relationship> reverseRelationships() {
                    return path.reverseRelationships();
                }

                @Override
                public Iterable<Node> nodes() {
                    return path.nodes();
                }

                @Override
                public Iterable<Node> reverseNodes() {
                    return path.reverseNodes();
                }

                @Override
                public int length() {
                    return path.length() + 1;
                }

                @Override
                public Iterator<PropertyContainer> iterator() {
                    return path.iterator();
                }
            };
            expandedPaths.add(p);
        }
        return expandedPaths;
    }

    private List<Path> expandARSC(Path path) {
        List<Path> expandedPaths = new LinkedList<>();
        for (Relationship r : (Iterable<Relationship>) PathExpanders.forDirection(Direction.OUTGOING).expand(path, BranchState.NO_STATE)) {
            Path p = new Path() {
                @Override
                public Node startNode() {
                    return path.startNode();
                }

                @Override
                public Node endNode() {
                    return r.getEndNode();
                }

                @Override
                public Relationship lastRelationship() {
                    return r;
                }

                @Override
                public Iterable<Relationship> relationships() {
                    List<Relationship> relationships = new LinkedList<>();
                    if (path.relationships() != null) {
                        for (Relationship relationship : path.relationships()) {
                            relationships.add(relationship);
                        }
                    }
                    relationships.add(r);
                    return relationships;
                }

                @Override
                public Iterable<Relationship> reverseRelationships() {
                    return path.reverseRelationships();
                }

                @Override
                public Iterable<Node> nodes() {
                    return path.nodes();
                }

                @Override
                public Iterable<Node> reverseNodes() {
                    return path.reverseNodes();
                }

                @Override
                public int length() {
                    return path.length() + 1;
                }

                @Override
                public Iterator<PropertyContainer> iterator() {
                    return path.iterator();
                }
            };
            if (subRouteSkyline.contains(r.getEndNode())) {
                boolean is_SubRouteDominated = checkRouteIsDominatedInRouteList(p, subRouteSkyline.get(r.getEndNode()));
                if (!is_SubRouteDominated) {
                    boolean isSubRouteAdded = subRouteSkyline.add(r.getEndNode(), p);
                    if (isSubRouteAdded) {
                        for (int subRouteIndex = 0; subRouteIndex < subRouteSkyline.get(r.getEndNode()).size(); subRouteIndex++) {
                            Path oldSubRoute = subRouteSkyline.get(r.getEndNode()).get(subRouteIndex);
                            if (isDominatedBy(oldSubRoute, p)) {
                                subRouteSkyline.get(r.getEndNode()).remove(oldSubRoute);
                                subRouteIndex--;
                            }
                        }
                    }
                    expandedPaths.add(p);
                }
            } else {
                subRouteSkyline.add(r.getEndNode(), p);
                expandedPaths.add(p);
                if (r.getEndNode().getId() != destinationNode.getId()) {
                    nodeQueue.add(r.getEndNode());
                }
            }
        }
        return expandedPaths;
    }

    @Procedure(value = "dbis.BRSC", name = "dbis.BRSC")
    @Description("Basic Route Skyline Computation from specified start node to destination node regarding to the relationship property keys")
    public Stream<RouteSkyline> BRSC(@Name("start") Node start,
                                     @Name("destination") Node destination,
                                     @Name("relationshipPropertyKeys") List<String> relationshipPropertyKeys) {
        this.propertyKeys = relationshipPropertyKeys;
        this.startNode = start;
        this.destinationNode = destination;
        Queue<Path> candidateQueue = new PriorityQueue<>(new Comparator<Path>() {
            @Override
            public int compare(Path o1, Path o2) {
                if (preferenceFunction(o1) > preferenceFunction(o2)) return -1;
                else if (preferenceFunction(o1) < preferenceFunction(o2)) return 1;
                return 0;
            }
        });
        List<Path> skylineRoutes = new LinkedList<>();
        Path p0 = new Path() {
            @Override
            public Node startNode() {
                return start;
            }

            @Override
            public Node endNode() {
                return start;
            }

            @Override
            public Relationship lastRelationship() {
                return null;
            }

            @Override
            public Iterable<Relationship> relationships() {
                return null;
            }

            @Override
            public Iterable<Relationship> reverseRelationships() {
                return null;
            }

            @Override
            public Iterable<Node> nodes() {
                return null;
            }

            @Override
            public Iterable<Node> reverseNodes() {
                return null;
            }

            @Override
            public int length() {
                return 0;
            }

            @Override
            public Iterator<PropertyContainer> iterator() {
                return null;
            }
        };
        candidateQueue.add(p0);
        while (!candidateQueue.isEmpty()) {
            Path p = candidateQueue.peek();             // fetch next path(sub-route) from the queue
            if (p.endNode().getId() == destination.getId()) {      // route completed
                boolean p_isDominated = false;
                // Is p dominated by any skyline route?
                for (Path route : skylineRoutes) {
                    if (isDominatedBy(p, route)) {
                        p_isDominated = true;
                        break;
                    }
                }
                if (!p_isDominated) {                   // path p must be a skyline route
                    skylineRoutes.add(p);               // update route skyline
                    // remove all routes in Sroutes that are dominated by p
                    for (int skylineRoutesIndex = 0; skylineRoutesIndex < skylineRoutes.size(); skylineRoutesIndex++) {
                        Path route = skylineRoutes.get(skylineRoutesIndex);
                        if (isDominatedBy(route, p)) {
                            skylineRoutes.remove(route);
                            skylineRoutesIndex--;
                        }
                    }
                }
            } else {
                // route is not completed, thus p must be further expanded
                // Pruning based on forward estimation (Pruning Criterion I)
                // lower bounding cost estimations for each path attribute
                Vector<Double> pLb = lb(p);
                boolean plb_isDominated = false;
                // Does any skyline route dominate the lower bounding cost estimation vector?
                for (Path route : skylineRoutes) {
                    if (isDominatedBy(pLb, route)) {
                        plb_isDominated = true;
                        break;
                    }
                }
                if (!plb_isDominated) {
                    // expand actual path by one hop (in each direction)
                    List<Path> VecPath = expandBRSC(p);
                    // remove sub-routes in vecPath that includes cycles
                    for (int vecPathIndex = 0; vecPathIndex < VecPath.size(); vecPathIndex++) {
                        Path subRoute = VecPath.get(vecPathIndex);
                        if (hasCycle(subRoute)) {
                            VecPath.remove(subRoute);
                            vecPathIndex--;
                        }
                    }
                    // insert sub-routes in vecPath into Qcand
                    for (Path subRoute : VecPath) {
                        candidateQueue.add(subRoute);
                    }
                }
            }
            candidateQueue.remove(p);
        }
        System.out.println("BRSC Routes;");
        skylineRoutes.forEach(route -> {
            route.relationships().forEach(relationship -> {
                System.out.print(relationship.toString());
            });
            System.out.println();
        });
        return skylineRoutes.stream().map(RouteSkyline::new);
    }

    @Procedure(value = "dbis.ARSC", name = "dbis.ARSC")
    @Description("Advanced Route Skyline Computation from specified start node to destination node regarding to the relationship property keys")
    public Stream<RouteSkyline> ARSC(@Name("start") Node start,
                                     @Name("destination") Node destination,
                                     @Name("relationshipPropertyKeys") List<String> relationshipPropertyKeys) {
        propertyKeys = relationshipPropertyKeys;
        startNode = start;
        destinationNode = destination;
        nodeQueue = new PriorityQueue<>(
                new Comparator<Node>() {
                    @Override
                    public int compare(Node o1, Node o2) {
                        List<Path> o1SubRoutes = subRouteSkyline.get(o1);
                        List<Path> o2SubRoutes = subRouteSkyline.get(o2);
                        for (Path p1 : o1SubRoutes) {
                            double p1Prefence = preferenceFunction(p1);
                            int higherPrefenceCount = 0;
                            for (Path p2 : o2SubRoutes) {
                                double p2Prefence = preferenceFunction(p2);
                                if (p1Prefence > p2Prefence) {
                                    higherPrefenceCount += 1;
                                } else {
                                    break;
                                }
                            }
                            if (higherPrefenceCount == o2SubRoutes.size()) {
                                return 1;
                            }
                        }
                        return 0;
                    }
                }
        );
        List<Path> routeSkylines = new LinkedList<>();
        subRouteSkyline.add(startNode, new Path() {
            @Override
            public Node startNode() {
                return startNode;
            }

            @Override
            public Node endNode() {
                return startNode;
            }

            @Override
            public Relationship lastRelationship() {
                return null;
            }

            @Override
            public Iterable<Relationship> relationships() {
                return null;
            }

            @Override
            public Iterable<Relationship> reverseRelationships() {
                return null;
            }

            @Override
            public Iterable<Node> nodes() {
                return null;
            }

            @Override
            public Iterable<Node> reverseNodes() {
                return null;
            }

            @Override
            public int length() {
                return 0;
            }

            @Override
            public Iterator<PropertyContainer> iterator() {
                return null;
            }
        });
        nodeQueue.add(startNode);
        while (!nodeQueue.isEmpty()) {
            Node nI = nodeQueue.peek();
            for (int subRouteSkylineIndex = 0; subRouteSkylineIndex < subRouteSkyline.get(nI).size(); subRouteSkylineIndex++) {
                Path p = subRouteSkyline.get(nI).get(subRouteSkylineIndex);
                // Pruning based on forward estimation
                // compute attribute vector p.lb[] -> lower bounding cost estimations for each path attribute
                Vector<Double> pLb = lb(p);
                boolean plb_isDominated = false;
                for (Path route : routeSkylines) {
                    if (isDominatedBy(pLb, route)) {
                        plb_isDominated = true;
                        subRouteSkyline.get(nI).remove(p);
                        subRouteSkylineIndex--;
                        break;
                    }
                }
                if (!plb_isDominated) {                                                     // if sub-route is not processed yet
                    List<Path> vecPath = expandARSC(p);                                     // expand actual path p by one hop (in each direction)
                    for (Path pPrime : vecPath) {
                        if (pPrime.endNode().getId() == destination.getId()) {              // route completed
                            boolean pPrime_isDominated = checkRouteIsDominatedInRouteList(pPrime, routeSkylines);
                            if (!pPrime_isDominated) {
                                routeSkylines.add(pPrime);                                      // path must be a skyline route
                                for (int skylineRouteIndex = 0; skylineRouteIndex < routeSkylines.size(); skylineRouteIndex++) {
                                    Path route = routeSkylines.get(skylineRouteIndex);
                                    if (isDominatedBy(route, pPrime)) {
                                        routeSkylines.remove(route);                            // update route skyline
                                        skylineRouteIndex--;
                                    }
                                }
                            }
                        } else {
                            // route is not completed and path must be further expanded
                            // Pruning based on sub-route skyline criterion (Pruning Criterion II)
                            Node vNext = pPrime.endNode();                                      // access end node of the actual path
                            boolean pPrime_isDominated = checkRouteIsDominatedInRouteList(pPrime, subRouteSkyline.get(vNext));
                            if (!pPrime_isDominated) {
                                boolean isPathAdded = subRouteSkyline.add(vNext, pPrime);
                                if (isPathAdded) {
                                    for (int vNextSubRouteSkylineIndex = 0; vNextSubRouteSkylineIndex < subRouteSkyline.get(vNext).size(); vNextSubRouteSkylineIndex++) {
                                        Path route = subRouteSkyline.get(vNext).get(vNextSubRouteSkylineIndex);
                                        if (isDominatedBy(route, pPrime)) {
                                            subRouteSkyline.get(vNext).remove(route);
                                            vNextSubRouteSkylineIndex--;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            nodeQueue.remove(nI);
            subRouteSkyline.removeAll(nI);
        }
        // Reporting Routes
        System.out.println("ARSC Routes;");
        routeSkylines.forEach(route -> {
            route.relationships().forEach(relationship -> {
                System.out.print(relationship.toString());
            });
            System.out.println();
        });
        return routeSkylines.stream().map(RouteSkyline::new);
    }

    public static class RouteSkyline {
        public String route;

        public RouteSkyline(Path p) {
            this.route = p.toString();
        }
    }

    public static class SubRouteSkyline {
        private Map<Long, List<Path>> subRoutes;

        public SubRouteSkyline() {
            subRoutes = new HashMap<>();
        }

        private List<Path> get(Node node) {
            if (!subRoutes.containsKey(node.getId())) {
                LinkedList<Path> emptySubRoutes = new LinkedList<>();
                subRoutes.put(node.getId(), emptySubRoutes);
                return emptySubRoutes;
            }
            return subRoutes.get(node.getId());
        }

        private boolean add(Node node, Path path) {
            boolean isPathAlreadyASubroute = false;
            for (Path subRoute : get(node)) {
                if (subRoute.equals(path)) {
                    isPathAlreadyASubroute = true;
                    break;
                }
            }
            if (!isPathAlreadyASubroute) {
                get(node).add(path);
                return true;
            }
            return false;
        }

        private void removeAll(Node node) {
            get(node).clear();
        }

        private boolean contains(Node node) {
            return subRoutes.containsKey(node.getId());
        }
    }
}