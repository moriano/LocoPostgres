package org.moriano.locopostgres;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Properties;

public class App {

    private static final Logger log = LogManager.getLogger(App.class);
    public static void main(String[] args) throws Exception {
        log.info("here we go!");
        String url = "jdbc:loco:postgresql://localhost/test";
        Properties props = new Properties();
        props.setProperty("user", "admin");
        props.setProperty("password", "admin");
        props.setProperty("database", "test");
        props.setProperty("ssl", "true");

        //DriverManager.registerDriver(new LocoDriver());

        Connection connection = DriverManager.getConnection(url, props);
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM ORDERS where o_orderkey = ?");
        preparedStatement.setInt(1, 1);

        //preparedStatement.setInt(1, 10);
        ResultSet resultSet = preparedStatement.executeQuery();
        //Statement statement = connection.createStatement();
        //ResultSet resultSet = statement.executeQuery("SELECT * FROM ORDERS where o_orderkey = 1;");
        while (resultSet.next()) {
            log.info("orderKey " + resultSet.getInt("o_orderkey") + "-" + resultSet.getInt(1));
            log.info("o_comment " + resultSet.getString("o_comment") + "-" + resultSet.getString(9));
            log.info("was null " + resultSet.wasNull());
        }


    }
}
