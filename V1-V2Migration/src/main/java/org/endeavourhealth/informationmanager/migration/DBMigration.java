package org.endeavourhealth.informationmanager.migration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBMigration {
  public static void main(String[] args) {
    System.out.println("IM v1 to v2 Database migration tool");
    if (args.length != 2) {
      System.err.println("Usage:\n DBMigration <v1 connection string> <v2 connection string>");
      System.exit(1);
    }

    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      System.err.println("Could not load driver");
      e.printStackTrace();
    }

    System.out.print("Connecting to IMv1...");
    try (Connection imv1 = DriverManager.getConnection(args[0])) {
      System.out.println("Done");
      System.out.print("Connecting to IMv2...");
      try (Connection imv2 = DriverManager.getConnection(args[1])) {
        System.out.println("Done");

        new MigrationEngine(imv1, imv2)
          .execute();

      } catch (SQLException e) {
        System.err.println("Could not connect to IMV2");
        e.printStackTrace();
      }
    } catch (SQLException e) {
      System.err.println("Could not connect to IMV1");
      e.printStackTrace();
    }
  }
}
