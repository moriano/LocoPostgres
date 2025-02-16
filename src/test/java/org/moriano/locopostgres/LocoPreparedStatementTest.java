package org.moriano.locopostgres;

import org.junit.jupiter.api.Test;
import org.moriano.locopostgres.model.SampleData;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Testcontainers
public class LocoPreparedStatementTest extends BaseTestWithData {

    private final ResultSetComparator resultSetComparator = new ResultSetComparator();

    @Test
    public void testPreparedStatementNoParameters() throws Exception {
        String sql = "SELECT * FROM sample_data";

        PreparedStatement postgresStatement = this.getPostgresConnection().prepareStatement(sql);
        ResultSet postgresResultSet = postgresStatement.executeQuery();

        List<SampleData> postgresResults = SampleData.fromResultSet(postgresResultSet);

        PreparedStatement locoStatement = this.getLocoConnection().prepareStatement(sql);
        ResultSet locoResultSet = locoStatement.executeQuery();

        List<SampleData> locoResults = SampleData.fromResultSet(locoResultSet);

        assertTrue(locoResults.size() == postgresResults.size());
        for(int i = 0; i<locoResults.size(); i++) {
            SampleData expected = postgresResults.get(i);
            SampleData actual = locoResults.get(i);
            assertEquals(expected, actual, "Results do not match\nExpected "+expected +"\nActual" + actual);
        }
    }



//    @Test
//    public void testPreparedStatementInvalidQuery() {
//
//    }
//
//    @Test
//    public void testPreparedStatementSingleParameter() {
//
//    }
//
//    @Test
//    public void testPreparedStatementMultipleParameters() {
//
//    }
}