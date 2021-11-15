package org.endeavourhealth.informationmanager.jdbc;

import org.endeavourhealth.informationmanager.TTFilerException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class ConnectionManagerJDBC {
    private ConnectionManagerJDBC() {}

    public static synchronized Connection get() throws TTFilerException {
        Map<String, String> envVars = System.getenv();

        String url = envVars.get("CONFIG_JDBC_URL");
        String user = envVars.get("CONFIG_JDBC_USERNAME");
        String pass = envVars.get("CONFIG_JDBC_PASSWORD");
        String driver = envVars.get("CONFIG_JDBC_CLASS");

        if (url == null || url.isEmpty()
            || user == null || user.isEmpty()
            || pass == null || pass.isEmpty())
            throw new IllegalStateException("You need to set the CONFIG_JDBC_ environment variables!");

        if (driver != null && !driver.isEmpty()) {
            loadDriver(driver);
        }

        Properties props = new Properties();

        props.setProperty("user", user);
        props.setProperty("password", pass);

        try {
            return DriverManager.getConnection(url, props);
        } catch (SQLException e) {
            throw new TTFilerException("Could not connect to database", e);
        }
    }

    private static void loadDriver(String driver) throws TTFilerException {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new TTFilerException("Failed to load driver", e);
        }
    }
}
