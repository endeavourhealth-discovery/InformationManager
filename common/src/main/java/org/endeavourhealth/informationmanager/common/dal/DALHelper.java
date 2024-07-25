package org.endeavourhealth.informationmanager.common.dal;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static java.sql.Types.*;

public class DALHelper {

  public static int getGeneratedKey(PreparedStatement stmt) {
    try (ResultSet rs = stmt.getGeneratedKeys()) {
      rs.next();
      return rs.getInt(1);
    } catch (SQLException e) {
      throw new DALException("Error fetching generated key", e);
    }
  }

  public static String getNullableString(ResultSet rs, String field) throws SQLException {
    String result = rs.getString(field);
    return rs.wasNull() ? null : result;
  }


  public static long getGeneratedLongKey(PreparedStatement stmt) {
    try (ResultSet rs = stmt.getGeneratedKeys()) {
      rs.next();
      return rs.getLong(1);
    } catch (SQLException e) {
      throw new DALException("Error fetching generated long key", e);
    }
  }

  // SET methods
  public static void setLong(PreparedStatement stmt, int i, Long value) {
    try {
      if (value == null)
        stmt.setNull(i, BIGINT);
      else
        stmt.setLong(i, value);
    } catch (SQLException e) {
      throw new DALException("Error setting LONG value", e);
    }
  }

  public static void setFloat(PreparedStatement stmt, int i, Float value) {
    try {
      if (value == null)
        stmt.setNull(i, FLOAT);
      else
        stmt.setFloat(i, value);
    } catch (SQLException e) {
      throw new DALException("Error setting FLOAT value", e);
    }
  }

  public static void setInt(PreparedStatement stmt, int i, Integer value) {
    try {
      if (value == null)
        stmt.setNull(i, INTEGER);
      else
        stmt.setInt(i, value);
    } catch (SQLException e) {
      throw new DALException("Error setting INT value", e);
    }
  }

  public static void setBool(PreparedStatement stmt, int i, Boolean value) {
    try {
      if (value == null)
        stmt.setNull(i, BOOLEAN);
      else
        stmt.setBoolean(i, value);
    } catch (SQLException e) {
      throw new DALException("Error setting BOOL value", e);
    }
  }

  public static void setByte(PreparedStatement stmt, int i, Byte value) {
    try {
      if (value == null)
        stmt.setNull(i, TINYINT);
      else
        stmt.setByte(i, value);
    } catch (SQLException e) {
      throw new DALException("Error setting BYTE value", e);
    }
  }

  public static void setShort(PreparedStatement stmt, int i, Short value) {
    try {
      if (value == null)
        stmt.setNull(i, TINYINT);
      else
        stmt.setShort(i, value);
    } catch (SQLException e) {
      throw new DALException("Error setting SHORT value", e);
    }
  }

  public static void setString(PreparedStatement stmt, int i, String value) {
    try {
      if (value == null)
        stmt.setNull(i, VARCHAR);
      else
        stmt.setString(i, value);
    } catch (SQLException e) {
      throw new DALException("Error setting STRING value", e);
    }
  }

  public static void setTimestamp(PreparedStatement stmt, int i, Date value) {
    try {
      if (value == null)
        stmt.setNull(i, TIMESTAMP);
      else
        stmt.setTimestamp(i, new Timestamp(value.getTime()));
    } catch (SQLException e) {
      throw new DALException("Error setting TIMESTAMP value", e);
    }
  }

  public static void setTimestamp(PreparedStatement stmt, int i, String value) {
    try {
      if (value == null)
        stmt.setNull(i, TIMESTAMP);
      else {
        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd:hh:mm:ss");
        stmt.setTimestamp(i, new Timestamp(sd.parse(value).getTime()));
      }
    } catch (SQLException | ParseException e) {
      throw new DALException("Error setting TIMESTAMP value", e);
    }
  }

  public static void setTimestamp(PreparedStatement stmt, int i, Timestamp value) {
    try {
      if (value == null)
        stmt.setNull(i, TIMESTAMP);
      else
        stmt.setTimestamp(i, value);
    } catch (SQLException e) {
      throw new DALException("Error setting TIMESTAMP value", e);
    }
  }

  public static void setVersion(PreparedStatement stmt, int i, String value) {
    try {
      if (value == null)
        stmt.setNull(i, VARCHAR);
      else
        stmt.setString(i, value);
    } catch (SQLException e) {
      throw new DALException("Error setting VERSION value", e);
    }
  }

  public static String inListParams(int size) {
    List<String> q = Collections.nCopies(size, "?");
    return String.join(",", q);
  }

  public static int setLongArray(PreparedStatement stmt, int i, List<Long> values) {
    try {
      for (Long value : values) {
        stmt.setLong(i++, value);
      }
      return i;
    } catch (SQLException e) {
      throw new DALException("Error setting LONG array", e);
    }
  }


  // GET methods
  public static Integer getInt(ResultSet rs, String field) throws SQLException {
    int result = rs.getInt(field);
    return rs.wasNull() ? null : result;
  }

  public static String getString(ResultSet rs, String field) throws SQLException {
    String result = rs.getString(field);
    return rs.wasNull() ? null : result;
  }

  public static Date getDate(ResultSet rs, String field) throws SQLException {
    Date result = rs.getDate(field);
    return rs.wasNull() ? null : result;
  }

  public static Integer getCalculatedRows(Connection conn) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement("SELECT FOUND_ROWS()");
         ResultSet rs = stmt.executeQuery()) {
      if (rs.next())
        return rs.getInt(1);
      else
        return null;
    }
  }
}
