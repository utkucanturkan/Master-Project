package project.LowerBoundsCalculators;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import project.Label;

import java.util.*;

public class ParetoPrep {
    private long startNodeId, destinationNodeId;
    private List<String> propertyKeys;
    private Map<String, Double> resourceConstraints;
    private List<String> labelConstraints;
    private Map<Long, Map<String, Double>> lowerBounds ;
    private Map<Long, Map<String, Relationship>> successorEdges;

    public ParetoPrep(long startNodeId, long destinationId, List<String> propertyKeys, Map<String, Double> resourceConstraints, List<String> labelConstraints) {
        this.startNodeId = startNodeId;
        this.destinationNodeId = destinationId;
        this.propertyKeys = propertyKeys;
        this.resourceConstraints = resourceConstraints;
        this.labelConstraints = labelConstraints;
        lowerBounds = new HashMap<>();
        successorEdges = new HashMap<>();
    }

    private double getLb(long fromNodeId, String propertyKey) {
        if (fromNodeId == destinationNodeId) {
            return 0d;
        }

        if (!lowerBounds.containsKey(fromNodeId)) {
            lowerBounds.put(fromNodeId, new HashMap<String, Double>() {{
                put(propertyKey, Double.POSITIVE_INFINITY);
            }});
        } else if (!lowerBounds.get(fromNodeId).containsKey(propertyKey)) {
            lowerBounds.get(fromNodeId).put(propertyKey, Double.POSITIVE_INFINITY);
        }

        return lowerBounds.get(fromNodeId).get(propertyKey);
    }

    private void setLb(long fromNodeId, String propertyKey, Double value) {
        if (lowerBounds.containsKey(fromNodeId)) {
            if (lowerBounds.get(fromNodeId) == null) {
                lowerBounds.put(fromNodeId, new HashMap<>());
            }
            lowerBounds.get(fromNodeId).put(propertyKey, value);
        } else {
            lowerBounds.put(fromNodeId, new HashMap<String, Double>() {{
                put(propertyKey, value);
            }});
        }
    }

    private void setSuccessorEdges(long fromNodeId, String propertyKey, Relationship edge) {
        if (successorEdges.containsKey(fromNodeId)) {
            if (successorEdges.get(fromNodeId) == null) {
                successorEdges.put(fromNodeId, new HashMap<>());
            }
            successorEdges.get(fromNodeId).put(propertyKey, edge);
        } else {
            successorEdges.put(fromNodeId, new HashMap<String, Relationship>() {{
                put(propertyKey, edge);
            }});
        }
    }

    private Relationship getSuccessorEdges(long fromNodeId, String propertyKey) {
        if (!successorEdges.containsKey(fromNodeId) || successorEdges.get(fromNodeId) == null) {
            return null;
        }
        return successorEdges.get(fromNodeId).get(propertyKey);
    }

    public Vector<Double> execute(GraphDatabaseService db) {
        // Initialization
        Set<Label> S = new LinkedHashSet<>();
        Queue<Long> open = new PriorityQueue<>(new Comparator<Long>() {
            @Override
            public int compare(Long lhsNodeId, Long rhsNodeId) {
                double costSumNode1 = 0d, costSumNode2 = 0d;
                for (String propertyKey : propertyKeys) {
                    costSumNode1 += getLb(lhsNodeId, propertyKey);
                    costSumNode2 += getLb(rhsNodeId, propertyKey);
                }
                return Double.compare(costSumNode1, costSumNode2);
            }
        });
        open.add(destinationNodeId);
        while (!open.isEmpty()) {
            // Node Selection
            // select n with minimal lower bound sum from open and remove from set
            long currentNodeId = open.peek();
            // Global Selection
            boolean doesAKnownPathDominate = false;
            for (Label aKnownPath : S) {
                Vector<Double> globalLowerBoundVector = new Vector<>();
                int index = 0;
                for (String propertyKey : propertyKeys) {
                    double sum = getLb(currentNodeId, propertyKey) + 0d; //getLb(startNode, propertyKey);
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
                Node node = db.getNodeById(currentNodeId);
                for (Relationship edge : node.getRelationships(Direction.INCOMING)) {
                    long mId = edge.getStartNode().getId();
                    boolean isNodeAddedForAProperty = false;
                    for (String propertyKey : propertyKeys) {
                        double lbPlusCost = getLb(currentNodeId, propertyKey) + Double.parseDouble(edge.getProperty(propertyKey).toString());
                        if (lbPlusCost < getLb(mId, propertyKey)) {
                            setLb(mId, propertyKey, lbPlusCost);
                            if (mId == startNodeId) {
                                modifiedComponents.add(propertyKey);
                            }
                            setSuccessorEdges(mId, propertyKey, edge);
                            if (!isNodeAddedForAProperty) {
                                if (mId != startNodeId) {
                                    open.add(mId);
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
                            Label label = (Label) S.toArray()[index];
                            if (label.isDominatedBy(p)) {
                                S.remove(label);
                                index--;
                            }
                        }
                    }
                }
            }
            open.remove(currentNodeId);
        }

        Vector<Double> lowerBoundVector = new Vector<>();
        for (int lowerBoundVectorIndex = 0; lowerBoundVectorIndex < propertyKeys.size(); lowerBoundVectorIndex++) {
            double lowerBound = Double.MAX_VALUE;
            for (Label l : S) {
                lowerBound = Math.min(lowerBound, l.getCostByIndex(lowerBoundVectorIndex));
            }
            lowerBoundVector.add(lowerBoundVectorIndex, lowerBound);
        }
        return lowerBoundVector;
    }

    private Label constructPath(String modifiedComponent) {
        long mId = startNodeId;
        Label p = new Label(startNodeId, destinationNodeId, propertyKeys, resourceConstraints, labelConstraints);
        while (mId != destinationNodeId) {
            Relationship m_n = getSuccessorEdges(mId, modifiedComponent);
            if (m_n == null) break;
            p = p.expand(m_n);
            mId = m_n.getEndNode().getId();
        }
        return p;
    }
}
