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

public class MultiPreferencePathPlannerARSC {
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
                distanceEstimations.get(node.getId()).put(propertyKey, calculateNetworkDistance(propertyKey));
            }
        } else {
            Map<String, Map<Long, Double>> propertyDistance = new HashMap<>();
            propertyDistance.put(propertyKey, calculateNetworkDistance(propertyKey));
            distanceEstimations.put(node.getId(), propertyDistance);
        }
        return distanceEstimations.get(node.getId()).get(propertyKey);
    }

    private Map<Long, Double> calculateNetworkDistance(String propertyKey) {
        // Store cost of shortest path w.r.t nodes
        Map<Long, Double> networkDistances = new LinkedHashMap<>();
        // Apply dijkstra according to attribute/weightIndex
        PathFinder<WeightedPath> finder = GraphAlgoFactory.dijkstra(PathExpanders.forDirection(Direction.INCOMING), propertyKey);
        for (Node graphNode : db.getAllNodes()) {
            WeightedPath p = finder.findSinglePath(destinationNode, graphNode);
            networkDistances.put(graphNode.getId(), (p == null) ? 0d : p.weight());
        }
        return networkDistances;
    }

    private Vector<Double> lb(Label label) {
        Vector<Double> lb = new Vector<>();
        int propertyIndex = 0;
        for (String propertyKey : propertyKeys) {
            lb.add(label.getCostByIndex(propertyIndex) + networkDistanceEstimation(label.lastNode(), destinationNode, propertyKey));
            propertyIndex++;
        }
        return lb;
    }

    @Procedure(value = "dbis.ARSC", name = "dbis.ARSC")
    @Description("Advanced Route Skyline Computation from specified start node to destination node regarding to the relationship property keys")
    public Stream<RouteSkyline> ARSC(@Name("start") Node start,
                                     @Name("destination") Node destination,
                                     @Name("relationshipPropertyKeys") List<String> relationshipPropertyKeys) {
        long startTime = System.currentTimeMillis();
        propertyKeys = relationshipPropertyKeys;
        startNode = start;
        destinationNode = destination;
        nodeQueue = new PriorityQueue<>(
                new Comparator<Node>() {
                    @Override
                    public int compare(Node o1, Node o2) {
                        List<Label> o1SubRoutes = subRouteSkyline.get(o1);
                        List<Label> o2SubRoutes = subRouteSkyline.get(o2);
                        for (Label p1 : o1SubRoutes) {
                            double p1Prefence = p1.preferenceFunction();
                            int higherPrefenceCount = 0;
                            for (Label p2 : o2SubRoutes) {
                                double p2Prefence = p2.preferenceFunction();
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
        Label startLabel = new Label(startNode);
        Vector<Double> pLb = lb(startLabel);
        List<Label> routeSkylines = new LinkedList<>();
        subRouteSkyline.add(startNode, startLabel);
        nodeQueue.add(startNode);
        while (!nodeQueue.isEmpty()) {
            Node nI = nodeQueue.peek();
            for (int subRouteSkylineIndex = 0; subRouteSkylineIndex < subRouteSkyline.get(nI).size(); subRouteSkylineIndex++) {
                Label p = subRouteSkyline.get(nI).get(subRouteSkylineIndex);
                // Pruning based on forward estimation
                // compute attribute vector p.lb[] -> lower bounding cost estimations for each path attribute

                boolean plb_isDominated = false;
                for (Label route : routeSkylines) {
                    if (route.doesDominate(pLb)) {
                        plb_isDominated = true;
                        subRouteSkyline.get(nI).remove(p);
                        subRouteSkylineIndex--;
                        break;
                    }
                }
                if (!plb_isDominated) {                                                         // if sub-route is not processed yet
                    List<Label> vecPath = p.expandARSC();                                    // expand actual path p by one hop (in each direction)
                    for (Label pPrime : vecPath) {
                        if (pPrime.lastNode().getId() == destination.getId()) {                 // route completed
                            boolean pPrime_isDominated = pPrime.checkRouteIsDominatedInRouteList(routeSkylines);
                            if (!pPrime_isDominated) {
                                routeSkylines.add(pPrime);                                      // path must be a skyline route
                                for (int skylineRouteIndex = 0; skylineRouteIndex < routeSkylines.size(); skylineRouteIndex++) {
                                    Label route = routeSkylines.get(skylineRouteIndex);
                                    if (route.isDominatedBy(pPrime)) {
                                        routeSkylines.remove(route);                            // update route skyline
                                        skylineRouteIndex--;
                                    }
                                }
                            }
                        } else {
                            // route is not completed and path must be further expanded
                            // Pruning based on sub-route skyline criterion (Pruning Criterion II)
                            Node vNext = pPrime.lastNode();                                      // access end node of the actual path
                            boolean pPrime_isDominated = pPrime.checkRouteIsDominatedInRouteList(subRouteSkyline.get(vNext));
                            if (!pPrime_isDominated) {
                                boolean isPathAdded = subRouteSkyline.add(vNext, pPrime);
                                if (isPathAdded) {
                                    for (int vNextSubRouteSkylineIndex = 0; vNextSubRouteSkylineIndex < subRouteSkyline.get(vNext).size(); vNextSubRouteSkylineIndex++) {
                                        Label route = subRouteSkyline.get(vNext).get(vNextSubRouteSkylineIndex);
                                        if (route.isDominatedBy(pPrime)) {
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
        long endTime = System.currentTimeMillis();
        long timeElapsed = (endTime - startTime) / 1000;
        System.out.println("Execution time in seconds: " + timeElapsed);
        reportRouteSkylines("ARSC", routeSkylines);
        return routeSkylines.stream().map(RouteSkyline::new);
    }

    private void reportRouteSkylines(String algorithmName, List<Label> routeSkylines) {
        System.out.println(algorithmName + " Routes;");
        routeSkylines.forEach(route -> {
            /*
            route.getNodes().forEach(relationship -> {
                System.out.print(relationship.getId() + " - ");
            });
            */
            System.out.print("Path - ");
            for (int criteriaIndex = 0; criteriaIndex < propertyKeys.size(); criteriaIndex++) {
                System.out.print("Criteria-" + (criteriaIndex + 1) + ": " + route.getCost().get(criteriaIndex) + " ");
            }
            System.out.println();
        });
    }

    public static class RouteSkyline {
        public String route;

        public RouteSkyline(Label p) {
            this.route = p.toString();
        }
    }

    public class ParetoPrep {
        private Map<Long, Map<String, Double>> lowerBounds = new HashMap<>();
        private Map<Long, Map<String, Relationship>> successorEdges = new HashMap<>();

        public double getLb(Node from, String propertyKey) {
            if (from.getId() == destinationNode.getId()) {
                return 0d;
            }

            if (!lowerBounds.containsKey(from.getId()) || !lowerBounds.get(from.getId()).containsKey(propertyKey)) {
                Map<String, Double> emptyLowerBoundsMapByPropertyKey = new HashMap<>();
                emptyLowerBoundsMapByPropertyKey.put(propertyKey, Double.POSITIVE_INFINITY);
                lowerBounds.put(from.getId(), emptyLowerBoundsMapByPropertyKey);
                return Double.POSITIVE_INFINITY;
            }

            return lowerBounds.get(from.getId()).get(propertyKey);
        }

        public void setLb(Node from, String propertyKey, Double value) {
            if (lowerBounds.containsKey(from.getId())) {
                lowerBounds.get(from.getId()).put(propertyKey, value);
            } else {
                lowerBounds.put(from.getId(), new HashMap<String, Double>() {{
                    put(propertyKey, value);
                }});
            }
        }

        public void setSuccessorEdges(Node from, String propertyKey, Relationship edge) {
            if (successorEdges.containsKey(from.getId())) {
                successorEdges.get(from.getId()).put(propertyKey, edge);
            } else {
                successorEdges.put(from.getId(), new HashMap<String, Relationship>() {{
                    put(propertyKey, edge);
                }});
            }
        }

        public Relationship getSuccessorEdges(Node from, String propertyKey) {
            if (!successorEdges.containsKey(from.getId()) || !successorEdges.get(from.getId()).containsKey(propertyKey)) {
                return null;
            }
            return successorEdges.get(from).get(propertyKey);
        }

        public List<Label> paretoPrep(Label startLabel) {
            // Initialization
            List<Label> S = new LinkedList<>();
            Queue<Node> open = new PriorityQueue<>(new Comparator<Node>() {
                @Override
                public int compare(Node o1, Node o2) {
                    double costSumNode1 = 0, costSumNode2 = 0;
                    for (String propertyKey : propertyKeys) {
                        costSumNode1 += getLb(o1, propertyKey);
                        costSumNode2 += getLb(o2, propertyKey);
                    }
                    if (costSumNode1 < costSumNode2) return -1;
                    else if (costSumNode1 > costSumNode2) return 1;
                    else return 0;
                }
            });
            open.add(destinationNode);
            while (!open.isEmpty()) {

                // Node Selection
                // select n with minimal lower bound sum from open and remove from set
                Node n = open.peek();
                open.remove(n);

                // Global Selection
                boolean doesAKnownPathDominate = false;
                for (Label aKnownPath : S) {
                    Vector<Double> globalLowerBoundVector = new Vector<>();
                    int index = 0;
                    for (String propertyKey: propertyKeys) {
                        double sum = getLb(n, propertyKey) + getLb(startNode, propertyKey);
                        globalLowerBoundVector.add(index, sum);
                        index++;
                    }
                    if (aKnownPath.doesDominate(globalLowerBoundVector)) {
                        doesAKnownPathDominate = true;
                        break;
                    }
                }

                if (!doesAKnownPathDominate) {
                    // Node Expansion
                    Vector<String> modifiedComponents = new Vector<>();
                    for (Relationship edge : n.getRelationships(Direction.INCOMING)) {
                        Node m = edge.getStartNode();
                        boolean isNodeAddedForAProperty = false;
                        for (String propertyKey : propertyKeys) {
                            double lbPlusCost = getLb(n, propertyKey) + Double.parseDouble(edge.getProperty(propertyKey).toString());
                            if (lbPlusCost < getLb(m, propertyKey)) {
                                setLb(m, propertyKey, lbPlusCost);
                                if (m.getId() == startNode.getId()) {
                                    modifiedComponents.add(propertyKey);
                                }
                                setSuccessorEdges(m, propertyKey, edge);
                                if (!isNodeAddedForAProperty) {
                                    if (m.getId() != startNode.getId()) {
                                        open.add(m);
                                        isNodeAddedForAProperty = true;
                                    }
                                }
                            }
                        }
                    }

                    // Path Construction
                    if (!modifiedComponents.isEmpty()) {
                        for (String modifiedComponent : modifiedComponents) {
                            Label p = constructPath(modifiedComponent);
                            S.add(p);
                            for (int index = 0; index < S.size(); index++) {
                                Label label = S.get(index);
                                if (label.isDominatedBy(p)) {
                                    S.remove(index);
                                    index--;
                                }
                            }
                        }
                    }
                }
            }
            return S;
        }

        private Label constructPath(String modifiedComponent) {
            Node m = startNode;
            Label p = new Label();
            while (m.getId() != destinationNode.getId()) {
                Relationship m_n = getSuccessorEdges(m, modifiedComponent);
                if (m_n == null) break;
                p = p.expand(m_n);
                m = m_n.getEndNode();
            }
            return p;
        }
    }

    public static class Label {
        private Vector<Double> cost = new Vector<>();
        private Node lastNode;


        public Label() {
            createInitialCostVector();
        }

        public Label(Node startNode) {
            createInitialCostVector();
            lastNode = startNode;
        }

        public double getCostByIndex(int index) {
            if (index < 0) return cost.get(0);
            return cost.get(index);
        }

        public double preferenceFunction() {
            double result = 0d;
            for (Double cost : this.getCost()) {
                result += cost;
            }
            return result;
        }

        private void createInitialCostVector() {
            for (int index = 0; index < propertyKeys.size(); index++) {
                cost.add(index, 0d);
            }
        }

        public Vector<Double> getCost() {
            return this.cost;
        }

        public Node lastNode() {
            return lastNode;
        }

        private List<Label> expandARSC() {
            List<Label> expandedPaths = new LinkedList<>();
            for (Relationship relationship : lastNode().getRelationships(Direction.OUTGOING)) {
                Label expandedPath = expand(relationship);
                if (subRouteSkyline.contains(relationship.getEndNode())) {
                    boolean is_SubRouteDominated = expandedPath.checkRouteIsDominatedInRouteList(subRouteSkyline.get(relationship.getEndNode()));
                    if (!is_SubRouteDominated) {
                        boolean isSubRouteAdded = subRouteSkyline.add(relationship.getEndNode(), expandedPath);
                        if (isSubRouteAdded) {
                            for (int subRouteIndex = 0; subRouteIndex < subRouteSkyline.get(relationship.getEndNode()).size(); subRouteIndex++) {
                                Label oldLabel = subRouteSkyline.get(relationship.getEndNode()).get(subRouteIndex);
                                if (oldLabel.isDominatedBy(expandedPath)) {
                                    subRouteSkyline.get(relationship.getEndNode()).remove(oldLabel);
                                    subRouteIndex--;
                                }
                            }
                        }
                        expandedPaths.add(expandedPath);
                    }
                } else {
                    subRouteSkyline.add(relationship.getEndNode(), expandedPath);
                    expandedPaths.add(expandedPath);
                    if (relationship.getEndNode().getId() != destinationNode.getId()) {
                        nodeQueue.add(relationship.getEndNode());
                    }
                }
            }
            return expandedPaths;
        }

        public Label expand(Relationship relationship) {
            Label expandedLabel = new Label();
            expandedLabel.lastNode = relationship.getEndNode();
            int index = 0;
            for (String propertyKey : propertyKeys) {
                expandedLabel.getCost().set(index, this.cost.get(index) +
                        Double.parseDouble(relationship.getProperty(propertyKey).toString()));
                index++;
            }
            return expandedLabel;
        }

        // DEFINITION 7
        private boolean isDominatedBy(Label rhsLabel) {
            if (rhsLabel == null) return false;
            if (getCost().isEmpty() || rhsLabel.getCost().isEmpty()) return false;
            if (equals(rhsLabel)) return false;
            int dominatedWeightCount = 0;
            for (int index = 0; index < propertyKeys.size(); index++) {
                if (getCost().get(index) > rhsLabel.getCost().get(index)) {
                    dominatedWeightCount++;
                }
            }
            return dominatedWeightCount == propertyKeys.size();
        }

        private boolean doesDominate(Vector<Double> vector) {
            if (vector == null) return false;
            if (vector.isEmpty() || getCost().isEmpty()) return false;
            int dominatedWeightCount = 0;
            for (int index = 0; index < propertyKeys.size(); index++) {
                if (vector.get(index) > getCost().get(index)) {
                    dominatedWeightCount++;
                }
            }
            return dominatedWeightCount == propertyKeys.size();
        }

        private boolean checkRouteIsDominatedInRouteList(List<Label> labels) {
            for (Label subLabel : labels) {
                if (isDominatedBy(subLabel)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Label label = (Label) o;

            if (!getCost().equals(label.getCost())) return false;
            return lastNode.equals(label.lastNode);
        }

        @Override
        public int hashCode() {
            int result = getCost().hashCode();
            result = 31 * result + lastNode.hashCode();
            return result;
        }
    }

    public static class SubRouteSkyline {

        private Map<Long, List<Label>> subRoutes;

        public SubRouteSkyline() {
            subRoutes = new HashMap<>();
        }

        private List<Label> get(Node node) {
            if (!subRoutes.containsKey(node.getId())) {
                List<Label> newLabels = new LinkedList<>();
                subRoutes.put(node.getId(), newLabels);
                return newLabels;
            }
            return subRoutes.get(node.getId());
        }

        private boolean add(Node node, Label label) {
            boolean isPathAlreadyASubroute = false;
            for (Label subLabel : get(node)) {
                if (label.equals(subLabel)) {
                    isPathAlreadyASubroute = true;
                    break;
                }
            }
            if (!isPathAlreadyASubroute) {
                get(node).add(label);
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