package org.moriano.locopostgres;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.moriano.locopostgres.container.PostgresAuthMethod;
import org.moriano.locopostgres.container.PostgresTestContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static junit.framework.TestCase.assertEquals;

/**
 * A class to test connections.
 */
@Testcontainers
public class LocoConnectionTest {
    private static final Logger log = LogManager.getLogger(LocoConnectionTest.class);

    private static final String USER = "someUser";
    private static final String PASSWORD = "somePassword";
    private static final String DB_NAME = "someDB";

    Properties props = new Properties();

    /**
     * The container used by LocoPostgres driver md5 auth
     */
    @Container
    private static final PostgresTestContainer postgresMD5 =
            new PostgresTestContainer("postgres:17.2", DB_NAME, USER, PASSWORD, PostgresAuthMethod.MD5);

    /**
     * The container used by LocoPostgres driver clear password auth
     */
    @Container
    private static final PostgresTestContainer postgresClearText =
            new PostgresTestContainer("postgres:17.2", DB_NAME, USER, PASSWORD, PostgresAuthMethod.PASSWORD);

    /**
     * The container used by LocoPostgres driver scram sha 256 auth
     */
    @Container
    private static final PostgresTestContainer postgresScramSHA256 =
            new PostgresTestContainer("postgres:17.2", DB_NAME, USER, PASSWORD, PostgresAuthMethod.SCRAM_SHA_256);




    @BeforeEach
    public void setup() throws Exception {
        Class.forName("org.moriano.locopostgres.LocoDriver");

        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        props.setProperty("database", DB_NAME);
        props.setProperty("ssl", "false");
        props.setProperty("sslmode", "disable");

        DriverManager.registerDriver(new LocoDriver());


    }


    /**
     * Tests that authentication using md5 works
     */
    @Test
    public void athenticationMD5() throws SQLException  {
        this.basicAuthenticationTest(postgresMD5);
    }

    /**
     * Tests that authentication using clear text passwords works
     */
    @Test
    public void authenticationClearText() throws SQLException {
        this.basicAuthenticationTest(postgresClearText);
    }

    //@Test
    public void authenticationScramSHA256() throws SQLException {
        this.basicAuthenticationTest(postgresScramSHA256);
    }

    private void basicAuthenticationTest(PostgresTestContainer container) throws SQLException {
        String locoUrl = container.getJdbcUrl().replace("jdbc:postgresql", "jdbc:loco:postgresql");
        Connection locoConnection  = DriverManager.getConnection(locoUrl, props);
        ResultSet resultSet = locoConnection.createStatement().executeQuery("SELECT 'hello' as world");
        resultSet.next();
        String result = resultSet.getString(1);
        assertEquals(result, "hello");
        locoConnection.close();
    }
}
