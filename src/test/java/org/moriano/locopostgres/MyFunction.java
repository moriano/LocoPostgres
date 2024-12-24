package org.moriano.locopostgres;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface MyFunction {
    Object apply(ResultSet resultSet) throws SQLException;
}
