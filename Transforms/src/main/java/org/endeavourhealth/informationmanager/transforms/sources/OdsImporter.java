package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.imq.QueryException;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.ODS;
import org.endeavourhealth.imapi.vocabulary.ORG;

import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;
import static org.endeavourhealth.imapi.model.tripletree.TTLiteral.literal;

public class OdsImporter implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(OdsImporter.class);
  private static final String CODING_SYSTEM = ".*\\\\TRUD\\\\ODS\\\\.*\\\\Code_System_Details.csv";
  private static final String ORGANISATION_DETAILS = ".*\\\\TRUD\\\\ODS\\\\.*\\\\Organisation_Details.csv";
  private static final String ORGANISATION_RELATIONSHIPS = ".*\\\\TRUD\\\\ODS\\\\.*\\\\Relationship_Details.csv";
  private static final String ORGANISATION_ROLES = ".*\\\\TRUD\\\\ODS\\\\.*\\\\Role_Details.csv";

  private List<String> fieldIndex;
  private String[] fieldData;
  private final Map<String, TTEntity> entityIndex = new HashMap<>();

  public void validateFiles(String inFolder) {
    ImportUtils.validateFiles(inFolder, new String[]{CODING_SYSTEM, ORGANISATION_DETAILS, ORGANISATION_RELATIONSHIPS, ORGANISATION_ROLES});
  }

  /**
   * Imports the TRUD Organisation data
   *
   * @param config import config
   * @throws Exception invalid document
   */
  @Override
  public void importData(TTImportConfig config) throws ImportException {
    try (TTManager manager = new TTManager();
         TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
      TTDocument doc = manager.createDocument(GRAPH.ODS);
      doc.setCrud(iri(IM.UPDATE_ALL));
      doc.addEntity(manager.createGraph(GRAPH.ODS, "ODS  code scheme and graph", "Official ODS code scheme and graph"));

      importCodingSystem(config, doc);
      filer.fileDocument(doc);

      doc = manager.createDocument(GRAPH.ODS);
      doc.setCrud(iri(IM.UPDATE_ALL));
      doc.addEntity(manager.createGraph(GRAPH.ODS, "ODS  code scheme and graph", "Official ODS code scheme and graph"));

      importOrganisationData(config, doc);
      importOrganisationRelationships(config);
      importOrganisationRoles(config);
      filer.fileDocument(doc);
    } catch (Exception e) {
      throw new ImportException(e.getMessage(),e);
    }
  }

  private void importCodingSystem(TTImportConfig config, TTDocument doc) throws IOException {
    TTArray recordClassSet;
    TTArray relationshipSet;
    LOG.info("Importing coding systems");

    Path file = ImportUtils.findFileForId(config.getFolder(), CODING_SYSTEM);

    relationshipSet = new TTArray();
    recordClassSet = new TTArray();
    TTIriRef londonExtension = iri(IM.NAMESPACE + "903031000252104");

    // Add/create base types
    doc
      .addEntity(new TTEntity(ODS.ORGANISATION_ROLE_TYPE)
        .setName("Organisation role")
        .addType(iri(IM.CONCEPT))
        .setDescription("The business role the organisation performs")
        .setStatus(iri(IM.ACTIVE))
        .set(iri(RDFS.SUBCLASS_OF), londonExtension)
      )
      .addEntity(new TTEntity(ODS.ORGANISATION_RELATIONSHIP)
        .setName("Organisation relationship")
        .addType(iri(IM.CONCEPT))
        .setDescription("The type of the relationship with another organisation")
        .setStatus(iri(IM.ACTIVE))
        .set(iri(RDFS.SUBCLASS_OF), londonExtension)
      ).addEntity(new TTEntity(ODS.ORGANISATION_RECORD_CLASS)
        .setName("Organisation record class")
        .addType(iri(IM.CONCEPT))
        .setDescription("The business role the organisation performs")
        .setStatus(iri(IM.ACTIVE))
        .set(iri(RDFS.SUBCLASS_OF), londonExtension)
      )

      // Add/create value sets
      .addEntity(new TTEntity(IM.NAMESPACE + "SET_OrganisationRole")
        .setName("Value set - Organisation role")
        .addType(iri(IM.VALUESET))
        .setDescription("Value set for Organisation (data model) / role")
        .setStatus(iri(IM.ACTIVE))
        .set(iri(IM.DEFINITION), literal("{\"match\":[{\"name\":\"Organisation role\",\"instanceOf\":[{\"@id\":\"" + ODS.ORGANISATION_ROLE_TYPE + "\",\"descendantsOf\":true}]}]}"))
        .set(iri(IM.IS_CONTAINED_IN), iri(IM.NAMESPACE + "VSET_DataModel"))
        .set(iri(IM.HAS_MEMBER), relationshipSet)
      ).addEntity(new TTEntity(IM.NAMESPACE + "SET_OrganisationRelationshipType")
        .setName("Value set - Organisation relationship")
        .addType(iri(IM.VALUESET))
        .setDescription("Value set for Organisation (data model) / relationship")
        .setStatus(iri(IM.ACTIVE))
        .set(iri(IM.DEFINITION), literal("{\"match\":[{\"name\":\"Organisation role\",\"instanceOf\":[{\"@id\":\"" + ODS.ORGANISATION_RELATIONSHIP + "\",\"descendantsOf\":true}]}]}"))
        .set(iri(IM.IS_CONTAINED_IN), iri(IM.NAMESPACE + "VSET_DataModel"))
        .set(iri(IM.HAS_MEMBER), relationshipSet)
      ).addEntity(new TTEntity(IM.NAMESPACE + "SET_OrganisationRecordClass")
        .setName("Value set - Organisation record class")
        .addType(iri(IM.VALUESET))
        .setDescription("Value set for Organisation (data model) / record class")
        .setStatus(iri(IM.ACTIVE))
        .set(iri(IM.DEFINITION), literal("{\"match\":[{\"name\":\"Organisation role\",\"instanceOf\":[{\"@id\":\"" + ODS.ORGANISATION_RECORD_CLASS + "\",\"descendantsOf\":true}]}]}"))
        .set(iri(IM.IS_CONTAINED_IN), iri(IM.NAMESPACE + "VSET_DataModel"))
        .set(iri(IM.HAS_MEMBER), recordClassSet)
      )

    ;

    LOG.info("Processing coding systems in {}", file.getFileName());
    int i = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      readLine(reader);
      processHeaders();

      while (readLine(reader)) {
        processCodingSystemLine(doc);
        i++;
        if (i % 25000 == 0)
          LOG.info("Processed {} lines", i);

      }
    }
  }

  private void processCodingSystemLine(TTDocument doc) {
    String codeSystem = fieldByName("CodeSystemName");
    if (null == codeSystem)
      return;

    String prefix = null;
    String suffix = null;

    switch (codeSystem) {
      case "OrganisationRecordClass":
        prefix = ODS.ORGANISATION_RECORD_CLASS;
        suffix = "(Organisation record class)";
        break;
      case "OrganisationRole":
        prefix = ODS.ORGANISATION_ROLE_TYPE;
        suffix = "(Organisation role)";
        break;
      case "OrganisationRelationship":
        prefix = ODS.ORGANISATION_RELATIONSHIP;
        suffix = "(Organisation relationship)";
        break;
      default:
        LOG.warn("Unknown ODS code system [{}]", codeSystem);
        break;
    }

    if (null != prefix) {
      TTEntity concept = new TTEntity(prefix + "_" + fieldByName("Code"))
        .addType(iri(IM.CONCEPT))
        .setName(fieldByName("DisplayName") + " " + suffix)
        .setScheme(iri(GRAPH.ODS))
        .setCode(fieldByName("Id"))
        .set(iri(RDFS.SUBCLASS_OF), new TTArray().add(iri(prefix)))
        .setStatus(iri(IM.ACTIVE));
      doc.addEntity(concept);
    }
  }

  private void importOrganisationData(TTImportConfig config, TTDocument doc) throws IOException {
    LOG.info("Importing Organisation data");

    Path file = ImportUtils.findFileForId(config.getFolder(), ORGANISATION_DETAILS);

    LOG.info("Processing organisations in {}", file.getFileName());
    int i = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      readLine(reader);
      processHeaders();

      while (readLine(reader)) {
        processOrganisationLine(doc);
        i++;
        if (i % 25000 == 0)
          LOG.info("Processed {} lines", i);

      }
    }
  }

  private void processOrganisationLine(TTDocument doc) {
    String odsCode = fieldByName("OrganisationId");
    String addIri = ORG.LOCATION_NAMESPACE + "ODS_" + odsCode;

    TTEntity org = new TTEntity(ORG.ORGANISATION_NAMESPACE + odsCode)
      .addType(TTIriRef.iri(IM.NAMESPACE + "Organisation"))
      .setName(fieldByName("Name"))
      .setStatus("Active".equals(fieldByName("Status")) ? iri(IM.ACTIVE) : iri(IM.INACTIVE))
      .set(iri(ORG.ODS_CODE), literal(odsCode))
      .set(iri(IM.ADDRESS), iri(addIri))
      .set(iri(ORG.ORGANISATION_RECORD_CLASS), iri(ODS.ORGANISATION_RECORD_CLASS + "_" + fieldByName("OrganisationRecordClass").substring(2)));

    addEntity(org, doc);

    TTEntity add = new TTEntity(addIri)
      .addType(iri(IM.ADDRESS_CLASS))
      .set(iri(IM.ADDRESS_LINE_1), literal(fieldByName("AddrLn1")))
      .set(iri(IM.ADDRESS_LINE_2), literal(fieldByName("AddrLn2")))
      .set(iri(IM.ADDRESS_LINE_3), literal(fieldByName("AddrLn3")))
      .set(iri(IM.LOCALITY), literal(fieldByName("Town")))
      .set(iri(IM.REGION), literal(fieldByName("County")))
      .set(iri(IM.POST_CODE), literal(fieldByName("PostCode")))
      .set(iri(IM.COUNTRY), literal(fieldByName("Country")));

    String uprn = fieldByName("UPRN");
    if (uprn != null && !uprn.isEmpty())
      add.set(iri(IM.UPRN), literal(fieldByName("UPRN")));

    addEntity(add, doc);
  }

  private void importOrganisationRelationships(TTImportConfig config) throws IOException {
    LOG.info("Importing Organisation relationship data");

    Path file = ImportUtils.findFileForId(config.getFolder(), ORGANISATION_RELATIONSHIPS);

    LOG.info("Processing relationships in {}", file.getFileName());
    int i = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      readLine(reader);
      processHeaders();

      while (readLine(reader)) {
        processRelationshipLine();
        i++;
        if (i % 25000 == 0)
          LOG.info("Processed {} lines", i);

      }
    }
  }

  private void processRelationshipLine() {
    String odsCode = fieldByName("OrganisationId");

    TTEntity org = entityIndex.get(ORG.ORGANISATION_NAMESPACE + odsCode);
    if (null == org)
      LOG.error("Unknown organisation [{}]", odsCode);
    else {
      TTArray rels = org.get(iri(ORG.RELATED_ORGANISATION));
      if (null == rels) {
        rels = new TTArray();
        org.set(iri(ORG.RELATED_ORGANISATION), rels);
      }

      TTNode rel = new TTNode()
        .set(iri(IM.CONCEPT), iri(ODS.ORGANISATION_RELATIONSHIP + "_" + fieldByName("RelationshipId").substring(2)))
        .set(iri(IM.EFFECTIVE_DATE), literal(fieldByName("OperationalStartDate")))
        .set(iri(IM.END_DATE), literal(fieldByName("OperationalEndDate")))
        .set(iri(IM.HAS_STATUS), "Active".equals(fieldByName("Status")) ? iri(IM.ACTIVE) : iri(IM.INACTIVE))
        .set(iri(ORG.TARGET), iri(ORG.ORGANISATION_NAMESPACE + fieldByName("TargetOrganisationId")));

      rels.add(rel);
    }
  }

  private void importOrganisationRoles(TTImportConfig config) throws IOException {
    LOG.info("Importing Organisation role data");

    Path file = ImportUtils.findFileForId(config.getFolder(), ORGANISATION_ROLES);

    LOG.info("Processing roles in {}", file.getFileName());
    int i = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      readLine(reader);
      processHeaders();

      while (readLine(reader)) {
        processRoleLine();
        i++;
        if (i % 25000 == 0)
          LOG.info("Processed {} lines", i);

      }
    }
  }

  private void processRoleLine() {
    String odsCode = fieldByName("OrganisationId");
    String roleId = fieldByName("RoleId");

    TTEntity org = entityIndex.get(ORG.ORGANISATION_NAMESPACE + odsCode);
    if (null == org)
      LOG.error("Unknown organisation [{}]", odsCode);
    else {
      TTArray roles = org.get(iri(ORG.ROLE));
      if (null == roles) {
        roles = new TTArray();
        org.set(iri(ORG.ROLE), roles);
      }

      TTNode role = new TTNode()
        .set(iri(IM.CONCEPT), iri(ODS.ORGANISATION_ROLE_TYPE + "_" + roleId.substring(2)))
        .set(iri(IM.EFFECTIVE_DATE), literal(fieldByName("OperationalStartDate")))
        .set(iri(IM.END_DATE), literal(fieldByName("OperationalEndDate")))
        .set(iri(IM.HAS_STATUS), "Active".equals(fieldByName("Status")) ? iri(IM.ACTIVE) : iri(IM.INACTIVE));

      roles.add(role);
    }
  }

  private boolean readLine(BufferedReader reader) throws IOException {
    String line = reader.readLine();

    if (line == null || line.isEmpty())
      return false;

    line = line.substring(1, line.length() - 1);
    this.fieldData = line.split("\",\"");

    return true;
  }

  private void processHeaders() {
    this.fieldIndex = Arrays.asList(this.fieldData);
  }

  private String fieldByName(String name) {
    int i = this.fieldIndex.indexOf(name);

    if (i >= this.fieldData.length)
      return null;

    return this.fieldData[i];
  }

  private void addEntity(TTEntity entity, TTDocument doc) {
    doc.addEntity(entity);
    entityIndex.put(entity.getIri(), entity);
  }

  @Override
  public void close() throws Exception {
  }
}
