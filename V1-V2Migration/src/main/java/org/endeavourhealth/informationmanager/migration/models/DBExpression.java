package org.endeavourhealth.informationmanager.migration.models;

public class DBExpression {
  public static final byte CLASS = 0;

  private byte type;
  private int target_entity;

  public DBExpression(byte type, int target_entity) {
    this.type = type;
    this.target_entity = target_entity;
  }

  public byte getType() {
    return type;
  }

  public DBExpression setType(byte type) {
    this.type = type;
    return this;
  }

  public int getTarget_entity() {
    return target_entity;
  }

  public DBExpression setTarget_entity(int target_entity) {
    this.target_entity = target_entity;
    return this;
  }
}
