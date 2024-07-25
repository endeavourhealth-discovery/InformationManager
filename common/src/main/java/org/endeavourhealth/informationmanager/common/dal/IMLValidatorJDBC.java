package org.endeavourhealth.informationmanager.common.dal;

import org.endeavourhealth.imapi.model.tripletree.TTIriRef;

import java.sql.*;
import java.util.Map;
import java.util.Properties;

public class IMLValidatorJDBC implements IMLValidatorDAL {
  private Connection conn;
  private PreparedStatement isAQuery;
  private PreparedStatement getAllSubClasses;

  public IMLValidatorJDBC() throws SQLException, ClassNotFoundException {
    conn = getConnection();
    isAQuery = conn.prepareStatement("SELECT ancestor from tct join entity p on tct.ancestor=p.dbid\n" +
      "join entity c on tct.descendant= c.dbid\n" +
      "where c.iri=? and p.iri=?");
    getAllSubClasses = conn.prepareStatement("SELECT c.* from tct\n" +
      "join entity p on tct.ancestor=p.dbid\n" +
      "join entity c on tct.descendant=c.dbid\n" +
      "where p.iri=?");
  }

  @Override
  public boolean isA(TTIriRef descendant, TTIriRef ancestor) throws SQLException {
    DALHelper.setString(isAQuery, 1, descendant.getIri());
    DALHelper.setString(isAQuery, 2, ancestor.getIri());
    ResultSet rs = isAQuery.executeQuery();
    return rs.next();
  }


  private Connection getConnection() throws ClassNotFoundException, SQLException {
    Map<String, String> envVars = System.getenv();

    String url = envVars.get("CONFIG_JDBC_URL");
    String user = envVars.get("CONFIG_JDBC_USERNAME");
    String pass = envVars.get("CONFIG_JDBC_PASSWORD");
    String driver = envVars.get("CONFIG_JDBC_CLASS");

    if (url == null || url.isEmpty()
      || user == null || user.isEmpty()
      || pass == null || pass.isEmpty())
      throw new IllegalStateException("You need to set the CONFIG_JDBC_ environment variables!");

    if (driver != null && !driver.isEmpty())
      Class.forName(driver);

    Properties props = new Properties();

    props.setProperty("user", user);
    props.setProperty("password", pass);

    return DriverManager.getConnection(url, props);
  }

}
