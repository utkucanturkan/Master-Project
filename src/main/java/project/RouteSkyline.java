package project;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

public class RouteSkyline {
    private static Relationship relationship;
    private static List<String> propertyKeys;
    private static Node startNode;
    private static Node destinationNode;

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    // Injectable Resource
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    // DEFINITION 2
    private double cost(Path path, String propertyKey) {
        double cost = 0d;
        if (path.relationships() != null) {
            for (Relationship e : path.relationships()) {
                double weight = Double.valueOf(e.getProperty(propertyKey).toString());
                cost += weight;
            }
        }
        return cost;
    }

    // DEFINITION 3
    private double preferenceFunction(Path path) {
        double result = 0d;
        for (String properyKey : propertyKeys) {
            double cost = cost(path, properyKey);
            result += cost;
        }
        return result;
    }

    // DEFINITION 7
    private boolean isDominatedBy(Path lhsPath, Path rhsPath) {
        int dominatedWeightCount = 0;
        for (String propertyKey : propertyKeys) {
            if (cost(lhsPath, propertyKey) > cost(rhsPath, propertyKey)) {
                dominatedWeightCount++;
            }
        }
        return dominatedWeightCount == propertyKeys.size();
    }

    private boolean doesDominate(Path path, Vector<Double> vector) {
        int dominatedWeightCount = 0, index = 0;
        for (String propertyKey : propertyKeys) {
            if (vector.get(index) < cost(path, propertyKey)) {
                dominatedWeightCount++;
            }
            index++;
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
            lb.add(propertyIndex, attr(path).get(propertyIndex) + networkDistanceEstimation(startNode, destinationNode, propertyKey));
            propertyIndex++;
        }
        return lb;
    }

    // DEFINITION 5
    private double networkDistanceEstimation(Node source, Node target, String propertyKey) {
        // Stores distances from node to another nodes in the graph ? (reference nodes)
        Map<String, Double> distancesFromSource = networkDistance(source, propertyKey);
        Map<String, Double> distancesFromTarget = networkDistance(target, propertyKey);
        Vector<Double> results = new Vector<>();
        for (Map.Entry<String, Double> sourceEntry : distancesFromSource.entrySet()) {
            for (Map.Entry<String, Double> targetEntry : distancesFromTarget.entrySet()) {
                // if distances are through same nodes ex; vs -> N, vt -> N
                if (sourceEntry.getKey().toString().equals(targetEntry.getKey().toString())) {
                    // subtraction from source to target distance and take absolute of it
                    // if sourceDistance or targetDistance equals 0, set the network estimation distance 0
                    results.add((sourceEntry.getValue() == 0 || targetEntry.getValue() == 0)
                            ? Math.abs(sourceEntry.getValue() - targetEntry.getValue()) : 0);
                    break;
                }
            }
        }
        // get and return the maximum value
        return Collections.max(results);
    }

    private Map<String, Double> networkDistance(Node node, String propertyKey) {
        // Store cost of shortest path w.r.t nodes
        Map<String, Double> networkDistances = new LinkedHashMap<>();
        // Apply dijkstra according to attribute/weightIndex
        PathFinder<WeightedPath> finder = GraphAlgoFactory.dijkstra(PathExpanders.forDirection(Direction.OUTGOING), propertyKey);
        for (Node graphNode : db.getAllNodes()) {
            WeightedPath p = finder.findSinglePath(node, graphNode);
            networkDistances.put(graphNode.getProperty("name").toString(), (p == null || p.weight() == 0) ? 0d : p.weight());
        }
        return networkDistances;
    }

    private List<Path> expand(Path path) {
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

    private boolean hasCycle(Path path) {
        if (path.relationships() != null) {
            for (Relationship lhsRelationShip : path.relationships()) {
                for (Relationship rhsRelationShip : path.relationships()) {
                    if (!lhsRelationShip.equals(rhsRelationShip) &&
                            (lhsRelationShip.getEndNode().equals(rhsRelationShip.getEndNode()) || lhsRelationShip.getEndNode().equals(startNode))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean hasAllKeys(Relationship relationship, List<String> propertyKeys) {
        for (String propertyKey : propertyKeys) {
            if (!relationship.hasProperty(propertyKey)) {
                return false;
            }
        }
        return true;
    }

    // stores the state of each path that are processed or not
    private static Map<String, Boolean> processedPaths = new HashMap<>();

    private boolean isProcessed(Path path) {
        return processedPaths.get(path.toString());
    }

    private void setProcessed(Path path, boolean state) {
        processedPaths.put(path.toString(), state);
    }

    // stores the subroutes from start node to specified node
    private static Map<String, List<Path>> subRouteSkylines = new HashMap<>();

    private List<Path> getSubRouteSkylines(Node node) {
        List<Path> subRoutes = subRouteSkylines.get(node.toString());
        if (subRoutes != null) {
            return subRoutes;
        }
        subRoutes = new LinkedList<>();
        // TODO: correct maximum depth of the path
        PathFinder<Path> finder = GraphAlgoFactory.allSimplePaths(PathExpanders.forDirection(Direction.OUTGOING), Integer.MAX_VALUE);
        for (Path p : finder.findAllPaths(startNode, node)) {
            subRoutes.add(p);
            setProcessed(p, false);
            // TODO: control whether the path is dominated by any another path
        }
        setSubRouteSkylines(node, subRoutes);
        return subRoutes;
    }

    private void setSubRouteSkylines(Node node, List<Path> subRoutes) {
        subRouteSkylines.put(node.toString(), subRoutes);
    }

    @Procedure(value = "dbis.BRSC", name = "dbis.BRSC")
    @Description("Basic Route Skyline Computation from specified start node to destination node regarding to the relationship property keys")
    public Stream<SkylineRoute> BRSC(@Name("start") Node start,
                                     @Name("destination") Node destination,
                                     //@Name("relationship") Relationship relationship,
                                     @Name("relationshipPropertyKeys") List<String> relationshipPropertyKeys) {
        //if (hasAllKeys(relationship, relationshipPropertyKeys)) {
        //this.relationship = relationship;
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
            Path p = candidateQueue.poll();             // fetch next path(sub-route) from the queue
            if (p.endNode().equals(destination)) {      // route completed
                boolean p_isDominated = false;
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
                for (Path route : skylineRoutes) {
                    if (doesDominate(route, pLb)) {
                        plb_isDominated = true;
                        break;
                    }
                }
                if (!plb_isDominated) {
                    // expand actual path by one hop (in each direction)
                    List<Path> VecPath = expand(p);
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
        }
        return skylineRoutes.stream().map(SkylineRoute::new);
            /*
        } else {
            log.debug("The Relationship does not contain the specified all property keys.");
        }
        return null;
        */
    }

    @Procedure(value = "dbis.ARSC", name = "dbis.ARSC")
    @Description("Advanced Route Skyline Computation from specified start node to destination node regarding to the relationship property keys")
    public Stream<SkylineRoute> ARSC(@Name("start") Node start,
                                     @Name("destination") Node destination,
                                     //@Name("relationship") Relationship relationship,
                                     @Name("relationshipPropertyKeys") List<String> relationshipPropertyKeys) {
        Queue<Node> nodeQueue = new PriorityQueue<>(
                new Comparator<Node>() {
                    @Override
                    public int compare(Node o1, Node o2) {
                        if (getSubRouteSkylines(o1).size() > getSubRouteSkylines(o2).size())
                            return 1;
                        else if (getSubRouteSkylines(o1).size() < getSubRouteSkylines(o2).size())
                            return -1;
                        return 0;
                    }
                }
        );      // updatable priority queue of nodes, each node stores its own sub-route skyline in a list n.SubouteSkyline
        List<Path> skylineRoutes = new LinkedList<>();
        nodeQueue.add(startNode);
        while (!nodeQueue.isEmpty()) {
            Node nI = nodeQueue.poll();
            // TODO: subRouteSkyline - returns list of path that maintains a skyline of all already explored sub-routes ending at node n
            for (int subRouteSkylineIndex = 0; subRouteSkylineIndex < getSubRouteSkylines(nI).size(); subRouteSkylineIndex++) {
                Path p = getSubRouteSkylines(nI).get(subRouteSkylineIndex);
                // Pruning based on forward estimation
                // compute attribute vector p.lb[] -> lower bounding cost estimations for each path attribute
                Vector<Double> pLb = lb(p);
                boolean plb_isDominated = false;
                for (Path route : skylineRoutes) {
                    if (doesDominate(route, pLb)) {
                        plb_isDominated = true;
                        // TODO: store as a key(Node)-value(List<Path>) list subrouteSkylines for each node
                        getSubRouteSkylines(nI).remove(p);
                        subRouteSkylineIndex--;
                        break;
                    }
                }
                // TODO: Implement isProcessed() and setProcessed() methods
                // TODO: store as a key(Path)-value(boolean) list processed information for each path
                if (!plb_isDominated && !isProcessed(p)) {                 // if sub-route is not processed yet
                    List<Path> vecPath = expand(p);                         // expand actual path p by one hop (in each direction)
                    for (Path pPrime : vecPath) {
                        setProcessed(pPrime, true);                          // mark sub-route pPrime as processed
                        if (pPrime.endNode().equals(destinationNode)) {     // route completed
                            boolean pPrime_isDominated = false;
                            for (Path route : skylineRoutes) {
                                if (isDominatedBy(pPrime, route)) {
                                    pPrime_isDominated = true;
                                    break;
                                }
                            }
                            if (!pPrime_isDominated) {
                                skylineRoutes.add(pPrime);              // path must be a skyline route
                                for (int skylineRouteIndex = 0; skylineRouteIndex < skylineRoutes.size(); skylineRouteIndex++) {
                                    Path route = skylineRoutes.get(skylineRouteIndex);
                                    if (isDominatedBy(route, pPrime)) {
                                        skylineRoutes.remove(route);    // update route skyline
                                        skylineRouteIndex--;
                                    }
                                }
                            }
                        } else {
                            // route is not completed and path must be further expanded
                            // Pruning based on sub-route skyline criterion (Pruning Criterion II)
                            Node vNext = pPrime.endNode();  // access end node of the actual path
                            boolean pPrime_isDominated = false;
                            for (Path route : getSubRouteSkylines(vNext)) {
                                if (isDominatedBy(pPrime, route)) {
                                    pPrime_isDominated = true;
                                }
                            }
                            if (!pPrime_isDominated) {

                                // TODO: store as a list subrouteSkylines for each node
                                getSubRouteSkylines(vNext).add(pPrime);
                                for (int vNextSubRouteSkylineIndex = 0; vNextSubRouteSkylineIndex < getSubRouteSkylines(vNext).size(); vNextSubRouteSkylineIndex++) {
                                    Path route = getSubRouteSkylines(vNext).get(vNextSubRouteSkylineIndex);
                                    if (isDominatedBy(route, pPrime)) {

                                        // TODO: store as a list subrouteSkylines for each node
                                        getSubRouteSkylines(vNext).remove(route);
                                        vNextSubRouteSkylineIndex--;
                                    }
                                }
                            }
                            // update nodeQueue
                            // updating is automatically done by priority queue with overrided compare method
                        }
                    }
                }
            }
        }
        return skylineRoutes.stream().map(SkylineRoute::new);
    }


    public static class SkylineRoute {
        public String route;

        public SkylineRoute(Path p) {
            this.route = p.toString();
        }
    }
}