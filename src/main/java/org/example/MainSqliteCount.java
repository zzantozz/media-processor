package org.example;

import java.sql.*;

public class MainSqliteCount {
    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:test.db")) {
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);
            ResultSet rs = statement.executeQuery("select count(*) as c from file_events");
            while (rs.next()) {
                System.out.println("count: " + rs.getInt("c"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
