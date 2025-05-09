package org.endeavourhealth.informationmanager.common;

import java.sql.*;
import java.util.Properties;

public class FeedDAL {
  private final Connection connection;

  public FeedDAL() throws SQLException {
    Properties props = new Properties();

    props.setProperty("user", System.getenv("JDBC_USERNAME"));
    props.setProperty("password", System.getenv("JDBC_PASSWORD"));


    connection = DriverManager.getConnection(System.getenv("JDBC_URL"), props);
  }

  public String getVersion(String name) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement("SELECT version FROM feed_version WHERE feed = ?")) {
      stmt.setString(1, name);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next())
          return rs.getString("version");
      }
    }
    return null;
  }
}
