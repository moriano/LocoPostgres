package org.moriano.locopostgres;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.moriano.locopostgres.container.PostgresAuthMethod;
import org.moriano.locopostgres.container.PostgresTestContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.*;

/**
 * The unit test for LocoStatement.
 *
 * As with other tests we will run two PostgreSQL containers and will connect to one using the official Postgres
 * driver and the other one with LocoPostgres. We assume that the official Postgres driver is correct and use it
 * as a model of what LocoPostgres should return
 */
@Testcontainers
public class LocoStatementTest {

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

    private Statement locoStatement = null;
    private Statement postgresStatement = null;

    /**
     * The container used by LocoPostgres driver clear password auth
     */
    @Container
    private static final PostgresTestContainer locoContainer =
            new PostgresTestContainer("postgres:17.2", DB_NAME, USER, PASSWORD, PostgresAuthMethod.PASSWORD);

    /**
     * The container used by the postgres official driver
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

        locoStatement = locoConnection.createStatement();
        postgresStatement = postgresConnection.createStatement();
    }

    @AfterEach
    public void cleanUp() throws Exception {
        locoStatement.close();
        postgresStatement.close();
    }

    @Test
    public void clearBatch() throws Exception {
        locoStatement.addBatch("SELECT 1");
        postgresStatement.addBatch("SELECT 1");

        locoStatement.addBatch("SELECT 2");
        postgresStatement.addBatch("SELECT 2");

        locoStatement.clearBatch();
        postgresStatement.clearBatch();
    }

    @Test
    public void clearBatch_nothingInTheBatch() throws Exception {
        locoStatement.clearBatch();
        postgresStatement.clearBatch();
    }

    @Test
    public void executeBatch() throws Exception {
        locoStatement.addBatch("SELECT 1");
        postgresStatement.addBatch("SELECT 1");

        locoStatement.addBatch("SELECT 2");
        postgresStatement.addBatch("SELECT 2");

        int[] locoResult = locoStatement.executeBatch();
        int[] postgresResult = postgresStatement.executeBatch();
        assertArrayEquals(locoResult, postgresResult);
    }

    @Test
    public void executeBatch_nothingInTheBatch() throws Exception {
        int[] locoResult = locoStatement.executeBatch();
        int[] postgresResult = postgresStatement.executeBatch();
        assertArrayEquals(locoResult, postgresResult);
    }


}
