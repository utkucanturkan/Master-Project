package project;

import org.neo4j.graphdb.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class Label implements Serializable {
    private static List<String> propertyKeys;
    private static Map<String, Double> resourceConstraints;
    private static List<String> labelConstraints;

    private long destinationNodeId;
    private long lastNodeId;
    public Vector<Double> cost;
    private Map<String, Double> resourceConstraintValues;

    public Label(long nodeId, long destinationNodeId, List<String> propertyKeys, Map<String, Double> resourceConstraints, List<String> labelConstraints) {
        Label.propertyKeys = propertyKeys;
        Label.resourceConstraints = resourceConstraints;
        Label.labelConstraints = labelConstraints;
        this.destinationNodeId = destinationNodeId;
        this.lastNodeId = nodeId;
        createInitialCostValues(propertyKeys);
        createInitialResourceConstraintValues(resourceConstraints);
    }

    public static double getBytesOfPrimitives() {
        final double DOUBLE_SIZE = 8;
        return propertyKeys.size() * DOUBLE_SIZE;
    }

    public double getCostByIndex(int index) {
        return cost.get((index < 0) ? 0 : Math.min(index, cost.size() - 1));
    }

    public double preferenceFunction() {
        double result = 0d;
        for (Double cost : getCosts()) {
            result += cost;
        }
        return result;
    }

    private void createInitialCostValues(List<String> propertyKeys) {
        if (propertyKeys == null) {
            return;
        }
        cost = new Vector<>();
        for (int index = 0; index < propertyKeys.size(); index++) {
            cost.add(index, 0d);
        }
    }

    private void createInitialResourceConstraintValues(Map<String, Double> constraintValues) {
        if (constraintValues == null) {
            return;
        }
        resourceConstraintValues = new HashMap<>();
        for (Map.Entry<String, Double> entry : constraintValues.entrySet()) {
            resourceConstraintValues.put(entry.getKey(), 0.);
        }
    }

    public Vector<Double> getCosts() {
        return this.cost;
    }

    public long getLastNodeId() {
        return this.lastNodeId;
    }

    public List<Label> expandARSC(GraphDatabaseService db, Queue<Long> nodeQueue, LocalRouteSkylineManager localRouteSkylineManager) throws IOException, ClassNotFoundException {
        List<Label> expandedPaths = new LinkedList<>();
        Node lastNode = db.getNodeById(lastNodeId);
        List<Relationship> validRelationships = new LinkedList<>();
        lastNode.getRelationships(Direction.OUTGOING).forEach(validRelationships::add);
        if (labelConstraints != null) {
            validRelationships = getValidRelationshipsByLabelConstraints(validRelationships);
        }
        for (Relationship relationship : validRelationships) {
            Label expandedPath = expand(relationship);
            if (expandedPath.isValidByResourceConstraints()) {
                long relationshipEndNodeId = relationship.getEndNodeId();
                if (localRouteSkylineManager.hasSubRoutes(relationshipEndNodeId)) {
                    boolean is_SubRouteDominated = expandedPath.isDominatedInRouteList(localRouteSkylineManager.get(relationshipEndNodeId));
                    if (!is_SubRouteDominated) {
                        if (localRouteSkylineManager.add(relationshipEndNodeId, expandedPath)) {
                            for (int subRouteIndex = 0; subRouteIndex < localRouteSkylineManager.getSizeOfSubRoutes(relationshipEndNodeId); subRouteIndex++) {
                                Label oldLabel = localRouteSkylineManager.getSubRouteByIndex(relationshipEndNodeId, subRouteIndex);
                                if (oldLabel.isDominatedBy(expandedPath)) {
                                    localRouteSkylineManager.removeSubRoute(relationshipEndNodeId, oldLabel);
                                    subRouteIndex--;
                                }
                            }
                        }
                        expandedPaths.add(expandedPath);
                    }
                } else {
                    localRouteSkylineManager.add(relationshipEndNodeId, expandedPath);
                    expandedPaths.add(expandedPath);
                    if (relationshipEndNodeId != destinationNodeId) {
                        nodeQueue.add(relationshipEndNodeId);
                    }
                }
            }
        }
        return expandedPaths;
    }

    public Label expand(Relationship relationship) {
        Label expandedLabel = new Label(relationship.getEndNode().getId(), destinationNodeId, propertyKeys, resourceConstraints, labelConstraints);
        int index = 0;
        for (String propertyKey : propertyKeys) {
            expandedLabel.getCosts().set(index, getCostByIndex(index) +
                    Double.parseDouble(relationship.getProperty(propertyKey).toString()));
            index++;
        }

        if (resourceConstraints != null && !resourceConstraints.isEmpty()) {
            for (Map.Entry<String, Double> resourceConstraint : resourceConstraints.entrySet()) {
                String resourceConstraintKey = resourceConstraint.getKey();
                double relationshipCost = relationship.hasProperty(resourceConstraintKey) ? Double.parseDouble(relationship.getProperty(resourceConstraintKey).toString()) : Double.MAX_VALUE;
                double updatedResourceConstraintValue = resourceConstraintValues.get(resourceConstraintKey)
                        + relationshipCost;
                expandedLabel.resourceConstraintValues.put(resourceConstraintKey, updatedResourceConstraintValue);
            }
        }

        return expandedLabel;
    }

    // RESOURCE CONSTRAINTS
    private boolean isValidByResourceConstraints() {
        if (resourceConstraints != null) {
            for (Map.Entry<String, Double> resourceConstraint : resourceConstraints.entrySet()) {
                if (resourceConstraintValues.get(resourceConstraint.getKey()) > resourceConstraint.getValue()) {
                    return false;
                }
            }
        }
        return true;
    }

    // LABEL CONSTRAINTS
    private boolean isValidRelationShipByLabelConstraint(Relationship relationship) {
        if (labelConstraints != null) {
            return labelConstraints.stream().anyMatch(x -> x.toLowerCase().equals(relationship.getType().name().toLowerCase()));
        }
        return true;
    }

    private boolean isDominatedBy(Relationship lhs, Relationship rhs) {
        int dominatedPropertyCount = 0;
        for (String propertyKey: propertyKeys) {
            double rhsPropertyCost = rhs.hasProperty(propertyKey) ? Double.parseDouble(rhs.getProperty(propertyKey).toString()) : Double.MAX_VALUE;
            double lhsPropertyCost = lhs.hasProperty(propertyKey) ? Double.parseDouble(lhs.getProperty(propertyKey).toString()) : Double.MAX_VALUE;
            if (lhsPropertyCost >= rhsPropertyCost) {
                dominatedPropertyCount++;
            }
        }
        return dominatedPropertyCount == propertyKeys.size();
    }

    private List<Relationship> getValidRelationshipsByLabelConstraints(List<Relationship> relationships) {
        List<Relationship> validRelationships = new LinkedList<>();

        List<Relationship> tempRelationships = new LinkedList<>();
        relationships.stream().filter(this::isValidRelationShipByLabelConstraint).forEach(tempRelationships::add);

        for (Relationship lhsRelationship: tempRelationships) {
            boolean isLhsRelationshipDominated = false;
            for (Relationship rhsRelationship: tempRelationships) {
                if (!lhsRelationship.equals(rhsRelationship) && lhsRelationship.getEndNodeId() == rhsRelationship.getEndNodeId()) {
                    isLhsRelationshipDominated = isDominatedBy(lhsRelationship, rhsRelationship);
                    if (isLhsRelationshipDominated) {
                        break;
                    }
                }
            }
            if (!isLhsRelationshipDominated) {
                validRelationships.add(lhsRelationship);
            }
        }

        return validRelationships;
    }

    // DEFINITION 7
    public boolean isDominatedBy(Label rhsLabel) {
        if (rhsLabel == null || getCosts().isEmpty() || rhsLabel.getCosts().isEmpty() || equals(rhsLabel))
            return false;
        int dominatedWeightCount = 0;
        for (int index = 0; index < propertyKeys.size(); index++) {
            if (getCostByIndex(index) >= rhsLabel.getCostByIndex(index)) {
                dominatedWeightCount++;
            }
        }
        return dominatedWeightCount == propertyKeys.size();
    }

    public boolean doesDominate(Vector<Double> vector) {
        if (vector == null || vector.isEmpty() || getCosts().isEmpty()) return false;
        int dominatedWeightCount = 0;
        for (int index = 0; index < propertyKeys.size(); index++) {
            if (vector.get(index) >= getCostByIndex(index)) {
                dominatedWeightCount++;
            }
        }
        return dominatedWeightCount == propertyKeys.size();
    }

    public boolean isDominatedInRouteList(List<Label> subRoutes) {
        for (Label subRoute : subRoutes) {
            if (isDominatedBy(subRoute)) {
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

        if (lastNodeId != label.lastNodeId) return false;
        return Objects.equals(cost, label.cost);
    }

    @Override
    public int hashCode() {
        int result = cost != null ? cost.hashCode() : 0;
        result = 31 * result + (int) (lastNodeId ^ (lastNodeId >>> 32));
        return result;
    }
}
