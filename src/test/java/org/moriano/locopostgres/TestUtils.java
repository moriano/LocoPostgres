package org.moriano.locopostgres;

import java.sql.SQLException;
import java.sql.Statement;

public class TestUtils {

    public static void closeQuietly(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                statement = null;
            }
        }
    }
}
