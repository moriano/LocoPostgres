package org.moriano.locopostgres;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Tests for the resultset. Tables used are
 *
 * test=> \d orders;
 *                                             Table "public.orders"
 *      Column      |         Type          | Collation | Nullable |                  Default
 * -----------------+-----------------------+-----------+----------+--------------------------------------------
 *  o_orderkey      | integer               |           | not null | nextval('orders_o_orderkey_seq'::regclass)
 *  o_custkey       | bigint                |           | not null |
 *  o_orderstatus   | character(1)          |           |          |
 *  o_totalprice    | numeric               |           |          |
 *  o_orderdate     | date                  |           |          |
 *  o_orderpriority | character(15)         |           |          |
 *  o_clerk         | character(15)         |           |          |
 *  o_shippriority  | integer               |           |          |
 *  o_comment       | character varying(79) |           |          |
 * Indexes:
 *     "orders_pkey" PRIMARY KEY, btree (o_orderkey)
 */
public class LocoResultSetTest {
    private static final Logger log = LogManager.getLogger(LocoResultSetTest.class);
    private final ResultSetComparator resultSetComparator = new ResultSetComparator();

    Connection postgresConnection;
    Connection locoConnection;
    Properties props = new Properties();
    String url = "jdbc:postgresql://localhost/test";
    String ordersSQL = "SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, " +
            "o_clerk, o_shippriority, o_comment FROM orders ORDER BY o_orderkey LIMIT 100";

    @BeforeEach
    public void setup() throws Exception {
        Class.forName("org.postgresql.Driver");
        Class.forName("org.moriano.locopostgres.LocoDriver");
        props.setProperty("user", "admin");
        props.setProperty("password", "admin");
        props.setProperty("database", "test");
        props.setProperty("ssl", "false");
        props.setProperty("sslmode", "disable");


        postgresConnection = DriverManager.getConnection(url, props);

        DriverManager.registerDriver(new LocoDriver());
        locoConnection = DriverManager.getConnection("jdbc:loco:postgresql://localhost/test", props);
    }

    @AfterEach
    public void cleanUp() throws Exception {
        postgresConnection.close();
        locoConnection.close();
    }

    @Test
    public void throwAway() throws Exception {
        PreparedStatement preparedStatement = this.locoConnection.prepareStatement("SELECT * FROM ORDERS where o_orderkey = ?");
        preparedStatement.setInt(1, 1);
        ResultSet rs = preparedStatement.executeQuery();
        rs.next();
        rs.getInt(1);
        int a = 1;
    }

    @Test
    public void getStringViaName() throws Exception {
        ResultSet postgresResultSet =this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getString = (ResultSet rs) ->  rs.getString("o_comment");
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getString);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getStringViaPosition() throws Exception {
        ResultSet postgresResultSet =this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getString = (ResultSet rs) ->  rs.getString(9);
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getString);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getIntViaName() throws Exception {
        ResultSet postgresResultSet =this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getInt = (ResultSet rs) ->  rs.getInt("o_orderkey");
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getInt);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getIntViaPosition() throws Exception {
        ResultSet postgresResultSet =this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getInt = (ResultSet rs) ->  rs.getInt(1);
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getInt);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getShortViaName() throws Exception {
        ResultSet postgresResultSet =this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getShort = (ResultSet rs) ->  rs.getShort("o_orderkey");
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getShort);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getShortViaPosition() throws Exception {
        ResultSet postgresResultSet =this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getShort = (ResultSet rs) ->  rs.getShort(1);
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getShort);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getLongViaName() throws Exception {
        ResultSet postgresResultSet =this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getLong = (ResultSet rs) ->  rs.getLong("o_custkey");
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getLong);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getLongViaPosition() throws Exception {
        ResultSet postgresResultSet =this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getLong = (ResultSet rs) ->  rs.getLong(2);
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getLong);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getFloatViaName() throws Exception {
        ResultSet postgresResultSet =this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getFloat = (ResultSet rs) ->  rs.getFloat("o_totalprice");
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getFloat);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getFloatViaPosition() throws Exception {
        ResultSet postgresResultSet =this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getFloat = (ResultSet rs) ->  rs.getFloat(4);
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getFloat);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }


    @Test
    public void getBigDecimalViaName() throws Exception {
        ResultSet postgresResultSet =this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getBigDecimal = (ResultSet rs) ->  rs.getBigDecimal("o_orderkey");
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getBigDecimal);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getBigDecimalViaPosition() throws Exception {
        ResultSet postgresResultSet =this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getBigDecimal = (ResultSet rs) ->  rs.getBigDecimal(1);
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getBigDecimal);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }

    @Test
    public void getBytes() throws Exception {
        ResultSet postgresResultSet =this.buildResultSet(postgresConnection, this.ordersSQL);
        ResultSet locoResultSet = this.buildResultSet(locoConnection, this.ordersSQL);
        MyFunction getBytes = (ResultSet rs) ->  rs.getBytes(1);
        boolean sameResults = this.resultSetComparator.compareResultSets(postgresResultSet, locoResultSet, getBytes);
        assertTrue(sameResults, "Results did not match, check logs for details");
    }


    private ResultSet buildResultSet(Connection connection, String sql) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        return resultSet;
    }
}
