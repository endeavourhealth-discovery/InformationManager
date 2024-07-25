package org.endeavourhealth.informationmanager.migration.models;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapData {
  public static Map<String, MapData> documentScheme = Stream.of(
      new AbstractMap.SimpleEntry<>("InformationModel/dm/core", new MapData(DBNamespace.CORE, DBModule.SEMANTIC, "%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/core#CM_DiscoveryCode", new MapData(DBNamespace.CORE, DBModule.SEMANTIC, "%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/core#DS_DATE_PREC", new MapData(DBNamespace.CORE, DBModule.SEMANTIC, "%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/core#LE_TYPE", new MapData(DBNamespace.CORE, DBModule.SEMANTIC, "LE_%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/core#SNOMED", new MapData(DBNamespace.CORE, DBModule.SNOMED, "SN_%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/HealthData/Medication", new MapData(DBNamespace.CORE, DBModule.SEMANTIC, "%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/Snomed", new MapData(DBNamespace.CORE, DBModule.SNOMED, "SN_%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/Snomed#SNOMED", new MapData(DBNamespace.SNOMED, DBModule.SNOMED, "%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/READ2", new MapData(DBNamespace.CORE, DBModule.READ2, "R2_%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/READ2#READ2", new MapData(DBNamespace.READ2, DBModule.READ2, "%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/CTV3", new MapData(DBNamespace.CORE, DBModule.CTV3, "CTV3_%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/CTV3#CTV3", new MapData(DBNamespace.CTV3, DBModule.CTV3, "%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/ICD10", new MapData(DBNamespace.CORE, DBModule.ICD10, "ICD10_%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/ICD10#ICD10", new MapData(DBNamespace.ICD10, DBModule.ICD10, "%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/OPCS4", new MapData(DBNamespace.CORE, DBModule.OPCS4, "OPCS4_%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/OPCS4#OPCS4", new MapData(DBNamespace.OPCS4, DBModule.OPCS4, "%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/O4-proxy", new MapData(DBNamespace.CORE, DBModule.OPCS4, "OPCS4_%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/BartsCerner", new MapData(DBNamespace.CORE, DBModule.BARTS, "BC_%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/BartsCerner#BartsCerner", new MapData(DBNamespace.BARTS, DBModule.BARTS, "%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/Discovery", new MapData(DBNamespace.CORE, DBModule.SEMANTIC, "%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/FHIR", new MapData(DBNamespace.CORE, DBModule.FHIR, "FHIR_%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/FHIR#FHIR_AG", new MapData(DBNamespace.FHIRVS, DBModule.FHIR, "administrative-gender/%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/FHIR#FHIR_AS", new MapData(DBNamespace.FHIRVS, DBModule.FHIR, "appointmentstatus/%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/FHIR#FHIR_AU", new MapData(DBNamespace.FHIRVS, DBModule.FHIR, "address-use/%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/FHIR#FHIR_CPS", new MapData(DBNamespace.FHIRVS, DBModule.FHIR, "contact-point-system/%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/FHIR#FHIR_CPU", new MapData(DBNamespace.FHIRVS, DBModule.FHIR, "contact-point-use/%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/FHIR#FHIR_PRS", new MapData(DBNamespace.FHIRVS, DBModule.FHIR, "request-status/%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/FHIR#FHIR_RFP", new MapData(DBNamespace.FHIRVS, DBModule.FHIR, "request-priority/%s")),

      new AbstractMap.SimpleEntry<>("InformationModel/dm/FHIR#FHIR_CEP", new MapData(DBNamespace.FHIRESD, DBModule.FHIR, "primarycare-problem-episodicity/%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/FHIR#FHIR_EC", new MapData(DBNamespace.FHIRESD, DBModule.FHIR, "primarycare-ethnic-category-extension/%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/FHIR#FHIR_MSAT", new MapData(DBNamespace.FHIRESD, DBModule.FHIR, "primarycare-medication-authorisation-type-extension/%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/FHIR#FHIR_RFT", new MapData(DBNamespace.FHIRESD, DBModule.FHIR, "primarycare-referral-request-type/%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/FHIR#FHIR_RS", new MapData(DBNamespace.FHIRESD, DBModule.FHIR, "primarycare-patient-registration-status/%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/FHIR#FHIR_RT", new MapData(DBNamespace.FHIRESD, DBModule.FHIR, "primarycare-patient-registration-type-extension/%s")),

      new AbstractMap.SimpleEntry<>("InformationModel/dm/EmisLocal", new MapData(DBNamespace.CORE, DBModule.EMIS, "EMLOC_%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/EmisLocal#EMIS_LOCAL", new MapData(DBNamespace.EMIS, DBModule.EMIS, "%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/TermMaps#LE_TYPE", new MapData(DBNamespace.CORE, DBModule.SEMANTIC, "LE_%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/TppLocal", new MapData(DBNamespace.CORE, DBModule.TPP, "TPP_%s")),
      new AbstractMap.SimpleEntry<>("InformationModel/dm/TppLocal#TPP_LOCAL", new MapData(DBNamespace.TPP, DBModule.TPP, "%s"))
    )
    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

  public static MapData getMap(String docPath, String scheme) {
    String key = docPath;
    if (scheme != null && !scheme.isEmpty())
      key += ("#" + scheme);

    MapData result = MapData.documentScheme.get(key);

    if (result == null)
      throw new IllegalStateException("Unmapped document/scheme combination [" + key + "]");

    return result;
  }

  public DBNamespace namespace;
  public DBModule module;
  public String format;

  public MapData(DBNamespace namespace, DBModule module, String format) {
    this.namespace = namespace;
    this.module = module;
    this.format = format;
  }
}
