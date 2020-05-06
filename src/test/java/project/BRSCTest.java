package project;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BRSCTest {
    private static final Config driverConfig = Config.build().withoutEncryption().toConfig();
    private ServerControls embeddedDatabaseServer;

    private String seedQuery = "create(n0:Node{name:'n0'}),(n1:Node{name:'n1'}),(n2:Node{name:'n2'}),(n3:Node{name:'n3'}),(n4:Node{name:'n4'}),(n5:Node{name:'n5'}),(n0)-[:GOES_TO{length:7, cost:7}]->(n1), \n" +
            "(n1)-[:GOES_TO{length:7, cost:7}]->(n0),(n0)-[:GOES_TO{length:3, cost:6}]->(n3),(n3)-[:GOES_TO{length:3, cost:6}]->(n0),(n0)-[:GOES_TO{length:4, cost:4}]->(n2),(n2)-[:GOES_TO{length:4, cost:4}]->(n0),(n3)-[:GOES_TO{length:2, cost:2}]->(n1),(n1)-[:GOES_TO{length:2, cost:2}]->(n3),(n3)-[:GOES_TO{length:3, cost:3}]->(n2),(n2)-[:GOES_TO{length:3, cost:3}]->(n3),(n3)-[:GOES_TO{length:3, cost:6}]->(n5),(n5)-[:GOES_TO{length:3, cost:6}]->(n3),(n3)-[:GOES_TO{length:5, cost:4}]->(n4),(n4)-[:GOES_TO{length:5, cost:4}]->(n3),(n2)-[:GOES_TO{length:5, cost:5}]->(n4),(n4)-[:GOES_TO{length:5, cost:5}]->(n2),(n4)-[:GOES_TO{length:1, cost:1}]->(n5),(n5)-[:GOES_TO{length:1, cost:1}]->(n4),(n5)-[:GOES_TO{length:7, cost:7}]->(n1),(n1)-[:GOES_TO{length:7, cost:7}]->(n5);";

    @BeforeAll
    void initializeNeo4j() {
        this.embeddedDatabaseServer = TestServerBuilders
                .newInProcessBuilder()
                .withProcedure(BRSC.class)
                .newServer();
    }

    @Test
    public void findRouteSkylinesByBRSC() {
        /*
        try (Driver driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig); Session session = driver.session()) {
            //session.run(seedQuery);

            StatementResult result = session.run("MATCH (startNode:Node{name:'n0'}), (destinationNode:Node{name:'n5'}) " +
                    "CALL dbis.BRSC(startNode, destinationNode, ['length', 'cost']) as paths");

            assertThat(result.stream().count()).isEqualTo(3);
        }
        */
    }
}
