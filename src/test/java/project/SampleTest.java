package project;

import org.neo4j.driver.v1.*;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class SampleTest {

    private final static short TOTAL_NODE_COUNT = 3353; //3353;
    private static short ITERATION_COUNT = 1;

    private static int[] testNodePairsArray = {1371, 1579, 698, 2207, 2136, 3190, 2867, 1123, 3239, 776, 633, 2190, 3329, 1682, 205, 1657, 530, 2787, 2566, 2722, 2512, 616, 1771, 605, 1473, 1138, 1046, 1875, 628, 2527, 1610, 3009, 2932, 1122, 2550, 2079, 2543, 1876, 1791, 1560, 450, 3230, 3237, 176, 2770, 2030, 1898, 460, 2174, 801, 2364, 1194, 2576, 2784, 2751, 632, 2675, 2466, 158, 651, 509, 1952, 1824, 3265, 748, 2908, 2825, 3159, 2369, 372, 2476, 2149, 1391, 1432, 2278, 1537, 1266, 2226, 1445, 2760, 878, 168, 37, 2138, 2230, 686, 290, 3332, 2318, 1338, 300, 709, 2691, 2977, 2664, 3337, 939, 1177, 3313, 351, 84, 1730, 2251, 491, 1481, 663, 1669, 3203, 209, 1937, 2745, 475, 2508, 1170, 1132, 2618, 234, 1963, 968, 3197, 1772, 2899, 96, 127, 675, 2041, 1927, 2491, 1040, 3069, 1338, 2519, 3176, 3217, 882, 1825, 2458, 1349, 2138, 971, 2607, 379, 804, 523, 1269, 2044, 2475, 2079, 775, 1925, 793, 2987, 438, 1496, 2225, 1228, 1871, 1174, 3039, 2959, 56, 2005, 2374, 1148, 1767, 2670, 3079, 710, 3060, 2075, 1759, 2639, 3200, 685, 1372, 2841, 1268, 3289, 53, 1438, 700, 1575, 210, 2879, 3262, 3216, 604, 266, 577, 2574, 755, 3280, 2078, 1386, 481, 23, 3095, 3121, 151, 2541};

    private static Void createRome99Graph(Transaction tx) {
        String seedQuery = "" +
                "create(n0:Node{name:'n0'})," +
                "(n1:Node{name:'n1'})," +
                "(n2:Node{name:'n2'})," +
                "(n3:Node{name:'n3'})," +
                "(n4:Node{name:'n4'})," +
                "(n5:Node{name:'n5'})," +
                "(n0)-[:GOES_TO{length:7, cost:7}]->(n1), " +
                "(n1)-[:GOES_TO{length:7, cost:7}]->(n0)," +
                "(n0)-[:GOES_TO{length:3, cost:6}]->(n3)," +
                "(n3)-[:GOES_TO{length:3, cost:6}]->(n0)," +
                "(n0)-[:GOES_TO{length:4, cost:4}]->(n2)," +
                "(n2)-[:GOES_TO{length:4, cost:4}]->(n0)," +
                "(n3)-[:GOES_TO{length:2, cost:2}]->(n1)," +
                "(n1)-[:GOES_TO{length:2, cost:2}]->(n3)," +
                "(n3)-[:GOES_TO{length:3, cost:3}]->(n2)," +
                "(n2)-[:GOES_TO{length:3, cost:3}]->(n3)," +
                "(n3)-[:GOES_TO{length:3, cost:6}]->(n5)," +
                "(n5)-[:GOES_TO{length:3, cost:6}]->(n3)," +
                "(n3)-[:GOES_TO{length:5, cost:4}]->(n4)," +
                "(n4)-[:GOES_TO{length:5, cost:4}]->(n3)," +
                "(n2)-[:GOES_TO{length:5, cost:5}]->(n4)," +
                "(n4)-[:GOES_TO{length:5, cost:5}]->(n2)," +
                "(n4)-[:GOES_TO{length:1, cost:1}]->(n5)," +
                "(n5)-[:GOES_TO{length:1, cost:1}]->(n4)," +
                "(n5)-[:GOES_TO{length:7, cost:7}]->(n1)," +
                "(n1)-[:GOES_TO{length:7, cost:7}]->(n5);";


        String theodorosRome99QueryString = "LOAD CSV FROM 'file:////Users/thodoriscjn/Desktop/Coding/utkucan-tuerkan/src/test/java/project/rome99.csv' AS line MERGE (start:Node{name:line[0]}) MERGE (destination:Node{name:line[1]}) MERGE (start)-[:GOES_TO{length:line[2], cost:50}]->(destination)";
        String utkucanRome99QueryString = "LOAD CSV FROM 'file:///D:/MasterProjects/MasterProjectNeo4jImplementation/src/test/java/project/rome99.csv' AS line MERGE (start:Node{name:line[0]}) MERGE (destination:Node{name:line[1]}) MERGE (start)-[:GOES_TO{length:line[2], cost:50}]->(destination)";

        tx.run(utkucanRome99QueryString);
        return null;
    }

    private static Void runARSC(Transaction tx, int startNode, int endNode) {
        tx.run("MATCH (startNode:Node{name:'" + startNode + "'}), (destinationNode:Node{name:'" + endNode + "'}) " +
                "CALL dbis.ARSC(startNode, destinationNode, ['length', 'cost']) " +
                "YIELD route " +
                "RETURN route");
        return null;
    }

    public static void main(String[] args) {
        final Config driverConfig = Config.build().withoutEncryption().toConfig();
        ServerControls embeddedDatabaseServer = TestServerBuilders
                .newInProcessBuilder()
                .withProcedure(MultiPreferencePathPlannerARSC.class)
                .newServer();

        Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);

        Session session = driver.session();

        System.out.println("Session started. Loading graph.");
        session.writeTransaction(new TransactionWork<Void>() {
            @Override
            public Void execute(Transaction transaction) {
                return createRome99Graph(transaction);
            }
        });
        System.out.println("Done loading graph.");


        for (int indexOfPair = 0; indexOfPair < testNodePairsArray.length; indexOfPair += 2) {
            int startNode = testNodePairsArray[indexOfPair];
            int endNode = testNodePairsArray[indexOfPair + 1];
            session.readTransaction(new TransactionWork<Void>() {
                @Override
                public Void execute(Transaction transaction) {
                    runARSC(transaction, startNode, endNode);
                    return null;
                }
            });
        }

        /*
        Random random = new Random();
        for (int i = 0; i < ITERATION_COUNT; i++) {
            int randomStartNodeName = random.nextInt(TOTAL_NODE_COUNT) + 1;
            int randomEndNodeName = random.nextInt(TOTAL_NODE_COUNT) + 1;
            if (randomStartNodeName != randomEndNodeName) {
                session.readTransaction(new TransactionWork<Void>() {
                    @Override
                    public Void execute(Transaction transaction) {
                        runARSC(transaction, randomStartNodeName, randomEndNodeName);
                        return null;
                    }
                });
            } else {
                ITERATION_COUNT--;
            }
        }*/
        session.close();
        System.out.println("Completed");
    }
}
