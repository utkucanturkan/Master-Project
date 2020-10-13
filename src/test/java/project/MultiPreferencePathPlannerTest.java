package project;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MultiPreferencePathPlannerTest {
    private static final Config driverConfig = Config.build().withoutEncryption().toConfig();
    private ServerControls embeddedDatabaseServer;

    private String seedQuery = "create(n0:Node{name:'n0'})," +
            "(n1:Node{name:'n1'})," +
            "(n2:Node{name:'n2'})," +
            "(n3:Node{name:'n3'})," +
            "(n4:Node{name:'n4'})," +
            "(n5:Node{name:'n5'})," +
            "(n0)-[:STREET{length:7, cost:7}]->(n1)," +
            "(n0)-[:HIGHWAY{length:2, cost:8}]->(n1)," +
            "(n1)-[:STREET{length:7, cost:7}]->(n0)," +
            "(n1)-[:HIGHWAY{length:2, cost:8}]->(n0)," +
            "(n0)-[:STREET{length:3, cost:6}]->(n3)," +
            "(n3)-[:STREET{length:3, cost:6}]->(n0)," +
            "(n0)-[:STREET{length:4, cost:4}]->(n2)," +
            "(n2)-[:STREET{length:4, cost:4}]->(n0)," +
            "(n3)-[:STREET{length:2, cost:2}]->(n1)," +
            "(n3)-[:HIGHWAY{length:6, cost:8}]->(n1)," +
            "(n1)-[:STREET{length:2, cost:2}]->(n3)," +
            "(n1)-[:HIGHWAY{length:6, cost:8}]->(n3)," +
            "(n3)-[:STREET{length:3, cost:3}]->(n2)," +
            "(n2)-[:STREET{length:3, cost:3}]->(n3)," +
            "(n3)-[:STREET{length:3, cost:6}]->(n5)," +
            "(n5)-[:STREET{length:3, cost:6}]->(n3)," +
            "(n3)-[:STREET{length:5, cost:4}]->(n4)," +
            "(n3)-[:HIGHWAY{length:3, cost:2}]->(n4)," +
            "(n4)-[:STREET{length:5, cost:4}]->(n3)," +
            "(n4)-[:HIGHWAY{length:3, cost:2}]->(n3)," +
            "(n2)-[:STREET{length:5, cost:5}]->(n4)," +
            "(n4)-[:STREET{length:5, cost:5}]->(n2)," +
            "(n4)-[:STREET{length:1, cost:1}]->(n5)," +
            "(n4)-[:HIGHWAY{length:1, cost:6}]->(n5)," +
            "(n5)-[:STREET{length:1, cost:1}]->(n4)," +
            "(n4)-[:HIGHWAY{length:1, cost:6}]->(n5)," +
            "(n5)-[:STREET{length:7, cost:7}]->(n1)," +
            "(n5)-[:HIGHWAY{length:7, cost:4}]->(n1)," +
            "(n1)-[:STREET{length:7, cost:7}]->(n5)," +
            "(n1)-[:HIGHWAY{length:7, cost:4}]->(n5);";

    private String seedQuery2 = "create(s:Node{name:'s'}),(a:Node{name:'a'}),(b:Node{name:'b'}),(c:Node{name:'c'}),(d:Node{name:'d'}),(t:Node{name:'t'}),(s)-[:STREET{length:2, cost:2}]->(a),(s)-[:STREET{length:3, cost:6}]->(c),(s)-[:STREET{length:3, cost:5}]->(b),(a)-[:STREET{length:2, cost:2}]->(c),(b)-[:STREET{length:4, cost:5}]->(d),(c)-[:STREET{length:3, cost:4}]->(d),(c)-[:STREET{length:5, cost:8}]->(t),(d)-[:STREET{length:4, cost:7}]->(t);";

    private String rome99Query = "LOAD CSV FROM 'file:///D:/MasterProjects/MasterProjectNeo4jImplementation/src/test/java/project/rome99.csv' AS line MERGE (start:Node{name:line[0]}) MERGE (destination:Node{name:line[1]}) MERGE (start)-[:STREET{length:line[2], cost:50}]->(destination)";
    //round(rand()*50)

    private boolean isDataSeeded = false;

    @BeforeAll
    void initializeNeo4j() {
        this.embeddedDatabaseServer = TestServerBuilders
                .newInProcessBuilder()
                .withProcedure(MultiPreferencePathPlannerBRSC.class)
                .withProcedure(MultiPreferencePathPlannerARSC.class)
                .newServer();
    }

    private void seed(Session session) {
        if (!isDataSeeded) {
            session.run(seedQuery);
            isDataSeeded = true;
        }
    }

    @Test
    public void findRouteSkylinesByBRSC() {
        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig); Session session = driver.session()) {
            seed(session);
            StatementResult result = session.run(
                    "MATCH (startNode:Node{name:'s'}), (destinationNode:Node{name:'t'}) " +
                            "CALL dbis.BRSC(startNode, destinationNode, ['length', 'cost']) YIELD route RETURN route;"
            );
            assertThat(result.stream().count()).isGreaterThan(0);
        }
    }

    @Test
    public void findRouteSkylinesByARSC() {
        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig); Session session = driver.session()) {
            seed(session);
            StatementResult result = session.run("MATCH (startNode:Node{name:'n0'}), (destinationNode:Node{name:'n5'}) " +
                    "CALL dbis.ARSC(" +
                    "startNode, destinationNode, " +        // start and destination Node
                    "['length', 'cost'], " +                // considered properties of edges
                    "{cost: 10.0}," +                       // resource constraints
                    "['HIGHWAY', 'STREET']" +               // label constraints
                    ") YIELD route RETURN route;");
            assertThat(result.stream().count()).isGreaterThan(0);
        }
    }
}
