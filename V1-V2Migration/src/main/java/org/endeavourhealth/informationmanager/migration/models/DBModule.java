package org.endeavourhealth.informationmanager.migration.models;

import java.util.Arrays;
import java.util.List;

public class DBModule {
  public static DBModule SEMANTIC = new DBModule(1, "http://www.EndeavourHealth.org/InformationModel/Module/SemanticModel");
  public static DBModule SNOMED = new DBModule(2, "http://www.EndeavourHealth.org/InformationModel/Module/Snomed");
  public static DBModule READ2 = new DBModule(3, "http://www.EndeavourHealth.org/InformationModel/Module/Legacy/READ2");
  public static DBModule CTV3 = new DBModule(4, "http://www.EndeavourHealth.org/InformationModel/Module/Legacy/CTV3");
  public static DBModule ICD10 = new DBModule(5, "http://www.EndeavourHealth.org/InformationModel/Module/Legacy/ICD10");
  public static DBModule OPCS4 = new DBModule(6, "http://www.EndeavourHealth.org/InformationModel/Module/Legacy/OPCS4");
  public static DBModule EMIS = new DBModule(7, "http://www.EndeavourHealth.org/InformationModel/Module/Legacy/EMIS");
  public static DBModule TPP = new DBModule(8, "http://www.EndeavourHealth.org/InformationModel/Module/Legacy/TPP");
  public static DBModule BARTS = new DBModule(9, "http://www.EndeavourHealth.org/InformationModel/Module/Legacy/BARTS");
  public static DBModule LEGACY = new DBModule(10, "http://www.EndeavourHealth.org/InformationModel/Module/Legacy");
  public static DBModule FHIR = new DBModule(10, "http://www.EndeavourHealth.org/InformationModel/Module/Legacy/FHIR");

  public static List<DBModule> modules = Arrays.asList(
    SEMANTIC, SNOMED, READ2, CTV3, ICD10, OPCS4, EMIS, TPP, BARTS, LEGACY, FHIR
  );

  public int dbid;
  public String iri;

  public DBModule(int dbid, String iri) {
    this.dbid = dbid;
    this.iri = iri;
  }
}
