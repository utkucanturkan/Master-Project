package project;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Procedure;

import java.util.*;

public class BRSC {

    // TODO: Implement preference function
    private double preferenceFunction(Path path) {
        return 0d;
    }

    // TODO: Implement isDominatedBy function
    private boolean isDominatedBy(Path lhsPath, Path rhs) {
        return true;
    }

    // TODO: Implement doesDominate function
    private boolean doesDominate(Path path, Vector<Double> vector) {
        return true;
    }

    // TODO: Compute attribute vector p.lb[]
    private Vector<Double> lb(Path path, Node start, Node destination) {
        return null;
    }

    // TODO: Implement expand function
    private List<Path> expand(Path path) {
        return null;
    }

    // TODO: Implement hasCycle function
    private boolean hasCycle(Path path) {
        return false;
    }

    @Procedure(value = "dbis.BRSC")
    @Description("Basic Route Skyline Computation")
    public void BRSC(Node start, Node destination) {
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
                return null;
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
                Vector<Double> pLb = lb(p, start, destination);
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
        // TODO: Implement reportSkylineRoutes(skylineRoutes) function
    }
}
