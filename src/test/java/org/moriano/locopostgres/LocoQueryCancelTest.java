package org.moriano.locopostgres;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.moriano.locopostgres.container.PostgresAuthMethod;
import org.moriano.locopostgres.container.PostgresTestContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * A class to test query cancellations
 */
@Testcontainers
public class LocoQueryCancelTest {

    private static final Logger log = LogManager.getLogger(LocoQueryCancelTest.class);

    private static final String USER = "someUser";
    private static final String PASSWORD = "somePassword";
    private static final String DB_NAME = "someDB";

    Properties props = new Properties();

    /**
     * The connection used by the LocoPostgres driver
     */
    private Connection locoConnection = null;

    /**
     * The connectin used by the official postgres driver
     */
    private Connection postgresConnection = null;


    /**
     * The container used by LocoPostgres
     */
    @Container
    private static final PostgresTestContainer locoContainer =
            new PostgresTestContainer("postgres:17.2", DB_NAME, USER, PASSWORD, PostgresAuthMethod.MD5);

    /**
     * The container used by regular driver
     */
    @Container
    private static final PostgresTestContainer postgresContainer =
            new PostgresTestContainer("postgres:17.2", DB_NAME, USER, PASSWORD, PostgresAuthMethod.PASSWORD);

    @BeforeEach
    public void setup() throws Exception {
        Class.forName("org.moriano.locopostgres.LocoDriver");

        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        props.setProperty("database", DB_NAME);
        props.setProperty("ssl", "false");
        props.setProperty("sslmode", "disable");

        DriverManager.registerDriver(new LocoDriver());


        postgresConnection = DriverManager.getConnection(postgresContainer.getJdbcUrl(), props);
        String locoUrl = locoContainer.getJdbcUrl().replace("jdbc:postgresql", "jdbc:loco:postgresql");
        DriverManager.registerDriver(new LocoDriver());
        locoConnection = DriverManager.getConnection(locoUrl, props);


    }

    /**
     * Tests whether we can cancel a query that is in execution
     */
    @Test
    public void cancelRunningQuery() throws Exception {
        Map<String, Connection> connections = Map.of("Postgres", postgresConnection,
                "Loco", locoConnection);
        for (Map.Entry<String, Connection> entry : connections.entrySet()) {
            log.info("Testing " + entry.getKey());
            Connection connection = entry.getValue();

            Statement statement = connection.createStatement();
            Thread cancelStatementThread = new Thread( () -> {
                try {
                    log.info("Will wait a bit before we try to cancel");
                    Thread.sleep(1000);
                    log.info("Cancel!");
                    statement.cancel();
                } catch (Exception e) {
                    log.error("Ouch! could not cancel statement", e);
                }
            });
            try {
                cancelStatementThread.start();
                statement.execute("SELECT pg_sleep(50)");
            } catch (SQLException e) {
                log.info("We got an exception while cancelling, this is the behaviour of the postgres driver");
            }

            /*
            Lets confirm that the query is indeed cancelled by running another query
             */
            ResultSet resultSet = statement.executeQuery("SELECT 42 as result");
            resultSet.next();
            int result = resultSet.getInt("result");
            assertEquals(result, 42);
        }
    }

    /**
     * Tests the case where we try to cancel when no query is being executed
     */
    @Test
    public void cancelNoQuery() {

    }


}
