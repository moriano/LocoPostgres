package org.moriano.locopostgres;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.moriano.locopostgres.container.PostgresAuthMethod;
import org.moriano.locopostgres.container.PostgresTestContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;


@Testcontainers
public class LocoPreparedStatementTest {

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
    }

    @Test
    public void testPreparedStatementNoParameters() throws Exception {
        PreparedStatement preparedStatement = this.locoConnection.prepareStatement("SELECT 1 as result;");
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        int result = resultSet.getInt("result");
        assertTrue(result == 1);
    }

    @Test
    public void testPreparedStatementInvalidQuery() {

    }

    @Test
    public void testPreparedStatementSingleParameter() {

    }

    @Test
    public void testPreparedStatementMultipleParameters() {

    }
}