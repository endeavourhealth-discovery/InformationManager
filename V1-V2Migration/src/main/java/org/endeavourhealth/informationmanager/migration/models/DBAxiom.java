package org.endeavourhealth.informationmanager.migration.models;

import java.util.ArrayList;
import java.util.List;

public class DBAxiom {
  public static final byte SUBCLASS = 0;
  public static final byte EQUIVALENT = 1;
  public static final byte DATAPROPERTYRANGE = 6;
  public static final byte INVERSEPROPERTYOF = 10;

  private int dbid;
  private byte type;
  private List<DBExpression> expressions = new ArrayList<DBExpression>();

  public int getDbid() {
    return dbid;
  }

  public DBAxiom setDbid(int dbid) {
    this.dbid = dbid;
    return this;
  }

  public byte getType() {
    return type;
  }

  public DBAxiom setType(byte type) {
    this.type = type;
    return this;
  }

  public List<DBExpression> getExpressions() {
    return expressions;
  }

  public DBAxiom setExpressions(List<DBExpression> expressions) {
    this.expressions = expressions;
    return this;
  }
}
