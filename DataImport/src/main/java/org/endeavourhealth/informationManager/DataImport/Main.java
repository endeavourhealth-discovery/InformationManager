package org.endeavourhealth.informationManager.DataImport;

public class Main {
  public static void main(String[] args) throws Exception {
    Transformer transformer = new Transformer();

    transformer.transform("Z:\\SyntheticEmisData\\v8.0 schema test data\\bulk_95047_Admin_Location_20231017043213_F95EE3AF-0B9D-40EB-8B28-8E858EF0091F.csv", null);
  }
}