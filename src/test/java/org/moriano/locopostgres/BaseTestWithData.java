package org.moriano.locopostgres;

import org.moriano.locopostgres.container.PostgresAuthMethod;
import org.moriano.locopostgres.container.PostgresTestContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

/**
 * A simple abstract class that provides containers with data in the table
 */
@Testcontainers
public abstract class BaseTestWithData {
    private static final Logger log = LoggerFactory.getLogger(BaseTestWithData.class);


    /**
     * The connection to be used with the official postgres driver
     */
    private Connection postgresConnection;

    /**
     * The connection to be used with LocoDriver
     */
    private Connection locoConnection;


    private static final String USER = "someUser";
    private static final String PASSWORD = "somePassword";
    private static final String DB_NAME = "someDB";


    @Container
    private static final PostgresTestContainer locoContainer =
            new PostgresTestContainer("postgres:17.2", DB_NAME, USER, PASSWORD, PostgresAuthMethod.PASSWORD);

    /**
     * The container used by the postgres official driver
     */
    @Container
    private static final PostgresTestContainer postgresContainer =
            new PostgresTestContainer("postgres:17.2", DB_NAME, USER, PASSWORD, PostgresAuthMethod.PASSWORD);

    static {
        try {
            Class.forName("org.moriano.locopostgres.LocoDriver");
            DriverManager.registerDriver(new LocoDriver());
        } catch (Exception e) {
            log.error("Ouch! could not register loco driver");
        }
    }

    public BaseTestWithData()  {
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        props.setProperty("database", DB_NAME);
        props.setProperty("ssl", "false");
        props.setProperty("sslmode", "disable");

        try {
            postgresConnection = DriverManager.getConnection(postgresContainer.getJdbcUrl(), props);
            String locoUrl = locoContainer.getJdbcUrl().replace("jdbc:postgresql", "jdbc:loco:postgresql");
            DriverManager.registerDriver(new LocoDriver());


            locoConnection = DriverManager.getConnection(locoUrl, props);
            this.loadDDL(locoConnection, postgresConnection);
            this.loadDML(locoConnection, postgresConnection);
        } catch(Exception e) {
            log.error("Ouch! could not initialize base constructor!", e);
        }
    }

    private void loadDML(Connection locoConnection, Connection postgresConnection) {
        try {
            String dmls = Files.readString(Paths.get(getClass().getClassLoader().getResource("dml.sql").toURI()));
            Statement locoStatement = locoConnection.createStatement();
            Statement postgresStatement = postgresConnection.createStatement();
            for (String dml : dmls.split(";")) {
                locoStatement.addBatch(dml);
                postgresStatement.addBatch(dml);
            }
            int[] postgresUpdates = postgresStatement.executeBatch();

            int[] locoUpdates = locoStatement.executeBatch();
            TestUtils.closeQuietly(locoStatement);
            TestUtils.closeQuietly(postgresStatement);
        } catch (Exception e) {
            log.error("Cannot load the dml!", e);
        }
    }

    private void loadDDL(Connection locoConnection, Connection postgresConnection) {

        try {
            String ddls = Files.readString(Paths.get(getClass().getClassLoader().getResource("ddl.sql").toURI()));
            Statement locoStatement = locoConnection.createStatement();
            Statement postgresStatement = postgresConnection.createStatement();
            for (String ddl : ddls.split(";")) {
                locoStatement.addBatch(ddl);
                postgresStatement.addBatch(ddl);
            }
            locoStatement.executeBatch();
            postgresStatement.executeBatch();
            TestUtils.closeQuietly(locoStatement);
            TestUtils.closeQuietly(postgresStatement);
        } catch (Exception e) {
            log.error("Cannot load the ddl!", e);
        }
    }

    public Connection getPostgresConnection() {
        return postgresConnection;
    }

    public Connection getLocoConnection() {
        return locoConnection;
    }

    public String getUser() {
        return USER;
    }

    public String getPassword() {
        return PASSWORD;
    }

    public String getDBName() {
        return DB_NAME;
    }
}
