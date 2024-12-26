package org.moriano.locopostgres;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.moriano.locopostgres.container.PostgresAuthMethod;
import org.moriano.locopostgres.container.PostgresTestContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Tests for the resultset. Tables used are
 * <p>
 * test=> \d orders;
 * Table "public.orders"
 * Column      |         Type          | Collation | Nullable |                  Default
 * -----------------+-----------------------+-----------+----------+--------------------------------------------
 * o_orderkey      | integer               |           | not null | nextval('orders_o_orderkey_seq'::regclass)
 * o_custkey       | bigint                |           | not null |
 * o_orderstatus   | character(1)          |           |          |
 * o_totalprice    | numeric               |           |          |
 * o_orderdate     | date                  |           |          |
 * o_orderpriority | character(15)         |           |          |
 * o_clerk         | character(15)         |           |          |
 * o_shippriority  | integer               |           |          |
 * o_comment       | character varying(79) |           |          |
 * Indexes:
 * "orders_pkey" PRIMARY KEY, btree (o_orderkey)
 */
@Testcontainers
public class LocoResultSetTest {
    private static final Logger log = LogManager.getLogger(LocoResultSetTest.class);
    private final ResultSetComparator resultSetComparator = new ResultSetComparator();

    private static final String USER = "someUser";
    private static final String PASSWORD = "somePassword";
    private static final String DB_NAME = "someDB";

    /**
     * The container used for the official PostgreSQL driver
     */
    @Container
    private static final PostgresTestContainer postgresForOfficial =
            new PostgresTestContainer("postgres:15.3", DB_NAME, USER, PASSWORD, PostgresAuthMethod.MD5);



    /**
     * The container used by LocoPostgres driver
     */
    @Container
    private static final PostgresTestContainer postgresForLoco =
            new PostgresTestContainer("postgres:15.3", DB_NAME, USER, PASSWORD, PostgresAuthMethod.MD5);


    Connection postgresConnection;
    Connection locoConnection;
    Properties props = new Properties();
    //String url = "jdbc:postgresql://localhost/test";
    String ordersSQL = "SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, " +
            "o_clerk, o_shippriority, o_comment FROM orders ORDER BY o_orderkey LIMIT 3";



    @BeforeEach
    public void setup() throws Exception {
        Class.forName("org.postgresql.Driver");
        Class.forName("org.moriano.locopostgres.LocoDriver");

                /*
        CREATE TABLE orders
(
    o_orderkey       BIGINT not null,
    o_custkey        BIGINT not null,
    o_orderstatus    CHAR(1) not null,
    o_totalprice     DOUBLE PRECISION not null,
    o_orderdate      DATE not null,
    o_orderpriority  CHAR(15) not null,
    o_clerk          CHAR(15) not null,
    o_shippriority   INTEGER not null,
    o_comment        VARCHAR(79) not null
);
         */

        String createTableSQL = "        CREATE TABLE orders\n" +
                "(\n" +
                "    o_orderkey       BIGINT not null,\n" +
                "    o_custkey        BIGINT not null,\n" +
                "    o_orderstatus    CHAR(1) not null,\n" +
                "    o_totalprice     DOUBLE PRECISION not null,\n" +
                "    o_orderdate      DATE not null,\n" +
                "    o_orderpriority  CHAR(15) not null,  \n" +
                "    o_clerk          CHAR(15) not null, \n" +
                "    o_shippriority   INTEGER not null,\n" +
                "    o_comment        VARCHAR(79) not null\n" +
                ");";

        String insertsql = "INSERT INTO orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) VALUES\n" +
                "(1, 45, 'O', 7500.50, '2023-12-15', '1-URGENT', 'Clerk#000000001', 0, 'Expedited shipping requested.'),\n" +
                "(2, 32, 'F', 3200.75, '2023-11-20', '3-MEDIUM', 'Clerk#000000002', 0, 'Customer requested additional packaging.'),\n" +
                "(3, 87, 'P', 1250.00, '2024-01-10', '2-HIGH', 'Clerk#000000003', 0, 'Awaiting payment confirmation.'),\n" +
                "(4, 21, 'O', 5800.99, '2023-12-05', '4-LOW', 'Clerk#000000004', 0, 'Delay expected due to weather.'),\n" +
                "(5, 67, 'F', 9200.10, '2023-10-25', '1-URGENT', 'Clerk#000000005', 0, 'Priority handling.'),\n" +
                "(6, 12, 'O', 4300.20, '2023-11-18', '2-HIGH', 'Clerk#000000006', 0, 'Awaiting confirmation of stock.'),\n" +
                "(7, 74, 'F', 2700.00, '2024-01-22', '3-MEDIUM', 'Clerk#000000007', 0, 'Delivered successfully.'),\n" +
                "(8, 53, 'P', 6700.85, '2024-01-08', '4-LOW', 'Clerk#000000008', 0, 'Pending customer response.'),\n" +
                "(9, 39, 'O', 8900.00, '2023-12-12', '1-URGENT', 'Clerk#000000009', 0, 'Shipment delayed due to holidays.'),\n" +
                "(10, 25, 'F', 1500.45, '2023-11-30', '2-HIGH', 'Clerk#000000010', 0, 'Normal processing.'),\n" +
                "(11, 98, 'P', 7800.60, '2023-10-14', '3-MEDIUM', 'Clerk#000000011', 0, 'Requires manager approval.'),\n" +
                "(12, 33, 'O', 2200.10, '2023-12-01', '1-URGENT', 'Clerk#000000012', 0, 'Rush order for holiday sale.'),\n" +
                "(13, 64, 'F', 4900.99, '2023-11-15', '4-LOW', 'Clerk#000000013', 0, 'No issues reported.'),\n" +
                "(14, 88, 'O', 3500.80, '2023-12-19', '2-HIGH', 'Clerk#000000014', 0, 'Special gift wrapping included.'),\n" +
                "(15, 41, 'P', 6600.00, '2024-01-03', '1-URGENT', 'Clerk#000000015', 0, 'Customer requested weekend delivery.'),\n" +
                "(16, 73, 'F', 1200.99, '2023-10-20', '3-MEDIUM', 'Clerk#000000016', 0, 'Standard order.'),\n" +
                "(17, 29, 'O', 8100.55, '2023-12-08', '4-LOW', 'Clerk#000000017', 0, 'Waiting for inventory update.'),\n" +
                "(18, 56, 'F', 3700.00, '2024-01-18', '2-HIGH', 'Clerk#000000018', 0, 'Processed for international shipment.'),\n" +
                "(19, 11, 'P', 9900.00, '2024-01-06', '1-URGENT', 'Clerk#000000019', 0, 'Customer confirmed express delivery.'),\n" +
                "(20, 75, 'O', 5400.20, '2023-12-14', '3-MEDIUM', 'Clerk#000000020', 0, 'Pending review by finance department.');\n";

        Class.forName("org.postgresql.Driver");
        Class.forName("org.moriano.locopostgres.LocoDriver");
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        props.setProperty("database", DB_NAME);
        props.setProperty("ssl", "false");
        props.setProperty("sslmode", "disable");


        postgresConnection = DriverManager.getConnection(postgresForOfficial.getJdbcUrl(), props);
        String locoUrl = postgresForLoco.getJdbcUrl().replace("jdbc:postgresql", "jdbc:loco:postgresql");
        DriverManager.registerDriver(new LocoDriver());
        locoConnection = DriverManager.getConnection(locoUrl, props);


        locoConnection.createStatement().executeQuery(createTableSQL);
        locoConnection.createStatement().executeQuery(insertsql);

        postgresConnection.createStatement().execute(createTableSQL);
        postgresConnection.createStatement().execute(insertsql);



    }

    @AfterEach
    public void cleanUp() throws Exception {
        postgresConnection.createStatement().execute("DROP TABLE orders");
        locoConnection.createStatement().execute("DROP TABLE orders");
        postgresConnection.close();
        locoConnection.close();
    }

    //@Test
    public void throwAway() throws Exception {
        PreparedStatement preparedStatement = this.locoConnection.prepareStatement("SELECT * FROM ORDERS where o_orderkey = ?");
        preparedStatement.setInt(1, 1);
        ResultSet rs = preparedStatement.executeQuery();
        rs.next();
        rs.getInt(1);
        int a = 1;
    }

    @Test
    public void getStringViaName2() throws Exception {
        ResultSet postgresResultSet = this.buildResultSet(postgresConnection, "Select 'hello world' as value");
        ResultSet locoResultSet = this.buildResultSet(locoConnection, "Select 'hello world' as value");
        MyFunction getString = (ResultSet rs) -> rs.getString(1);
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getString);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getStringViaName() throws Exception {
        ResultSet postgresResultSet = this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getString = (ResultSet rs) -> rs.getString("o_comment");
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getString);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getStringViaPosition() throws Exception {
        ResultSet postgresResultSet = this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getString = (ResultSet rs) -> rs.getString(9);
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getString);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getIntViaName() throws Exception {
        ResultSet postgresResultSet = this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getInt = (ResultSet rs) -> rs.getInt("o_orderkey");
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getInt);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getIntViaPosition() throws Exception {
        ResultSet postgresResultSet = this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getInt = (ResultSet rs) -> rs.getInt(1);
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getInt);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getShortViaName() throws Exception {
        ResultSet postgresResultSet = this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getShort = (ResultSet rs) -> rs.getShort("o_orderkey");
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getShort);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getShortViaPosition() throws Exception {
        ResultSet postgresResultSet = this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getShort = (ResultSet rs) -> rs.getShort(1);
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getShort);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getLongViaName() throws Exception {
        ResultSet postgresResultSet = this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getLong = (ResultSet rs) -> rs.getLong("o_orderkey");
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getLong);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getLongViaPosition() throws Exception {
        ResultSet postgresResultSet = this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getLong = (ResultSet rs) -> rs.getLong(2);
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getLong);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getFloatViaName() throws Exception {
        ResultSet postgresResultSet = this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getFloat = (ResultSet rs) -> rs.getFloat("o_totalprice");
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getFloat);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getFloatViaPosition() throws Exception {
        ResultSet postgresResultSet = this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getFloat = (ResultSet rs) -> rs.getFloat(4);
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getFloat);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }


    @Test
    public void getBigDecimalViaName() throws Exception {
        ResultSet postgresResultSet = this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getBigDecimal = (ResultSet rs) -> rs.getBigDecimal("o_orderkey");
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getBigDecimal);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getBigDecimalViaPosition() throws Exception {
        ResultSet postgresResultSet = this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getBigDecimal = (ResultSet rs) -> rs.getBigDecimal(1);
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getBigDecimal);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getBytes() throws Exception {
        ResultSet postgresResultSet = this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getBytes = (ResultSet rs) -> rs.getBytes(1);
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getBytes);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }


    private ResultSet buildResultSet(Connection connection, String sql) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        return resultSet;
    }
}
