package project;


import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpanders;

import java.util.*;

public class MultiDijkstra {
    private Map<Long, Map<String, Map<Long, Double>>> distanceEstimations = new HashMap<>();
    private GraphDatabaseService db;
    private long destinationNodeId;
    private List<String> propertyKeys;

    public MultiDijkstra(GraphDatabaseService dbs, long destinationNodeId, List<String> propertyKeys) {
        db = dbs;
        this.destinationNodeId = destinationNodeId;
        this.propertyKeys = propertyKeys;
    }

    // DEFINITION 5
    private double networkDistanceEstimation(long intermediateNodeId, String propertyKey) {
        // Stores distances from node to another nodes in the graph (reference nodes)
        Map<Long, Double> distancesFromSource = getNetworkDistance(intermediateNodeId, propertyKey);
        Map<Long, Double> distancesFromTarget = getNetworkDistance(destinationNodeId, propertyKey);
        Vector<Double> results = new Vector<>();
        for (Map.Entry<Long, Double> sourceEntry : distancesFromSource.entrySet()) {
            for (Map.Entry<Long, Double> targetEntry : distancesFromTarget.entrySet()) {
                // if distances are through same nodes ex; vs -> N, vt -> N
                if (sourceEntry.getKey().equals(targetEntry.getKey())) {
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

    private Map<Long, Double> getNetworkDistance(long nodeId, String propertyKey) {
        if (distanceEstimations.containsKey(nodeId)) {
            if (!distanceEstimations.get(nodeId).containsKey(propertyKey)) {
                distanceEstimations.get(nodeId).put(propertyKey, calculateNetworkDistance(nodeId, propertyKey));
            }
        } else {
            Map<String, Map<Long, Double>> propertyDistance = new HashMap<>();
            propertyDistance.put(propertyKey, calculateNetworkDistance(nodeId, propertyKey));
            distanceEstimations.put(nodeId, propertyDistance);
        }
        return distanceEstimations.get(nodeId).get(propertyKey);
    }

    private Map<Long, Double> calculateNetworkDistance(long targetId, String propertyKey) {
        // Store cost of shortest path w.r.t nodes
        Map<Long, Double> networkDistances = new LinkedHashMap<>();
        // Apply dijkstra according to attribute/weightIndex
        Node target = db.getNodeById(targetId);
        PathFinder<WeightedPath> finder = GraphAlgoFactory.dijkstra(PathExpanders.forDirection(Direction.INCOMING), propertyKey);
        for (Node graphNode : db.getAllNodes()) {
            WeightedPath p = finder.findSinglePath(graphNode, target);
            networkDistances.put(graphNode.getId(), (p == null) ? 0d : p.weight());
        }
        return networkDistances;
    }

    public Vector<Double> lb(Label intermediateLabel) {
        Vector<Double> lb = new Vector<>();
        int propertyIndex = 0;
        for (String propertyKey : propertyKeys) {
            lb.add(intermediateLabel.getCostByIndex(propertyIndex) + networkDistanceEstimation(intermediateLabel.getLastNodeId(), propertyKey));
            propertyIndex++;
        }
        return lb;
    }
}


