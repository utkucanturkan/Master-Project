package project;

import org.neo4j.driver.v1.*;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;

import java.util.Random;

public class SampleTest {
    public static void main(String[] args) {
        final Config driverConfig = Config.build().withoutEncryption().toConfig();
        ServerControls embeddedDatabaseServer = TestServerBuilders
                .newInProcessBuilder()
                .withProcedure(MultiPreferencePathPlannerARSC.class)
                .newServer();
        String rome99Query = "LOAD CSV FROM 'file:///D:/MasterProjects/MasterProjectNeo4jImplementation/src/test/java/project/rome99.csv' AS line MERGE (start:Node{name:line[0]}) MERGE (destination:Node{name:line[1]}) MERGE (start)-[:GOES_TO{length:line[2], cost:50}]->(destination)";
        /*
        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig); Session session = driver.session()) {
            session.run(rome99Query);
            session.close();
            Random random = new Random();
            for (int i = 0; i < 3; i++) {
                int randomStartNodeName = random.nextInt(3353) + 1;
                int randomEndNodeName = random.nextInt(3353) + 1;
                if (randomStartNodeName != randomEndNodeName) {
                    session.run("MATCH (startNode:Node{name:'"+randomStartNodeName+"'}), (destinationNode:Node{name:'"+randomEndNodeName+"'}) " +
                            "CALL dbis.ARSC(startNode, destinationNode, ['length', 'cost']) YIELD route RETURN route;");
                }

            }
        }
         */
        Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
        try {
            Session session = driver.session();
            session.run(rome99Query);
            session.close();
        }
        catch(Exception e) {
        }

        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            int randomStartNodeName = random.nextInt(3353) + 1;
            int randomEndNodeName = random.nextInt(3353) + 1;
            if (randomStartNodeName != randomEndNodeName) {
                try {
                    Session session = driver.session();
                    session.run("MATCH (startNode:Node{name:'"+randomStartNodeName+"'}), (destinationNode:Node{name:'"+randomEndNodeName+"'}) " +
                            "CALL dbis.ARSC(startNode, destinationNode, ['length', 'cost']) YIELD route RETURN route;");
                    session.close();
                }
                catch(Exception e) {
                }

            }

        }
    }
}
