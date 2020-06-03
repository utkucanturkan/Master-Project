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

    private String seedQuery = "create(n0:Node{name:'n0'}),(n1:Node{name:'n1'}),(n2:Node{name:'n2'}),(n3:Node{name:'n3'}),(n4:Node{name:'n4'}),(n5:Node{name:'n5'}),(n0)-[:GOES_TO{length:7, cost:7}]->(n1), \n" +
            "(n1)-[:GOES_TO{length:7, cost:7}]->(n0),(n0)-[:GOES_TO{length:3, cost:6}]->(n3),(n3)-[:GOES_TO{length:3, cost:6}]->(n0),(n0)-[:GOES_TO{length:4, cost:4}]->(n2),(n2)-[:GOES_TO{length:4, cost:4}]->(n0),(n3)-[:GOES_TO{length:2, cost:2}]->(n1),(n1)-[:GOES_TO{length:2, cost:2}]->(n3),(n3)-[:GOES_TO{length:3, cost:3}]->(n2),(n2)-[:GOES_TO{length:3, cost:3}]->(n3),(n3)-[:GOES_TO{length:3, cost:6}]->(n5),(n5)-[:GOES_TO{length:3, cost:6}]->(n3),(n3)-[:GOES_TO{length:5, cost:4}]->(n4),(n4)-[:GOES_TO{length:5, cost:4}]->(n3),(n2)-[:GOES_TO{length:5, cost:5}]->(n4),(n4)-[:GOES_TO{length:5, cost:5}]->(n2),(n4)-[:GOES_TO{length:1, cost:1}]->(n5),(n5)-[:GOES_TO{length:1, cost:1}]->(n4),(n5)-[:GOES_TO{length:7, cost:7}]->(n1),(n1)-[:GOES_TO{length:7, cost:7}]->(n5);";

    private String seedQuery2 = "create(s:Node{name:'s'}),(a:Node{name:'a'}),(b:Node{name:'b'}),(c:Node{name:'c'}),(d:Node{name:'d'}),(t:Node{name:'t'}),(s)-[:GOES_TO{length:2, cost:2}]->(a),(s)-[:GOES_TO{length:3, cost:6}]->(c),(s)-[:GOES_TO{length:3, cost:5}]->(b),(a)-[:GOES_TO{length:2, cost:2}]->(c),(b)-[:GOES_TO{length:4, cost:5}]->(d),(c)-[:GOES_TO{length:3, cost:4}]->(d),(c)-[:GOES_TO{length:5, cost:8}]->(t),(d)-[:GOES_TO{length:4, cost:7}]->(t);";

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
                    "MATCH (startNode:Node{name:'n0'}), (destinationNode:Node{name:'n5'}) " +
                    "CALL dbis.BRSC(startNode, destinationNode, ['length', 'cost']) YIELD route RETURN route;"
            );
            assertThat(result.stream().count()).isEqualTo(3);
        }
    }

    @Test
    public void findRouteSkylinesByARSC() {
        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig); Session session = driver.session()) {
            seed(session);
            StatementResult result = session.run(
                    "MATCH (startNode:Node{name:'n0'}), (destinationNode:Node{name:'n5'}) " +
                    "CALL dbis.ARSC(startNode, destinationNode, ['length', 'cost']) YIELD route RETURN route;");
            assertThat(result.stream().count()).isEqualTo(3);
        }
    }
}
