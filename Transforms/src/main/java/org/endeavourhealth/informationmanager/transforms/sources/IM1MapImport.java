package org.endeavourhealth.informationmanager.transforms.sources;

import org.apache.commons.text.CaseUtils;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.logic.exporters.ImportMaps;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.ZipUtils;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.DataFormatException;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class IM1MapImport implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(IM1MapImport.class);
  private static final String[] im1Codes = {".*\\\\IMv1\\\\concepts.zip"};
  private static final String[] oldIris = {".*\\\\IMv1\\\\oldiris.txt"};
  private static final String[] context = {".*\\\\IMv1\\\\ContextMaps.txt"};
  private static final String[] fhirMaps = {".*\\\\FHIR\\\\FHIR_Core_Maps.txt"};

  private static final Map<String, String> organisationMap = Map.of(
    "CM_Org_Barts", "RQX42",
    "CM_Org_Imperial", "8HL46",
    "CM_Org_CWH", "5LA19",
    "CM_Org_THH", "RQM93",
    "CM_Org_LNWH", "RAX0A",
    "CM_Org_Kings", "NV178",
    "CM_Org_BHRUT", "RF4",
    "CM_Org_CQC", "8HN02"
  );
  private static final Map<String, TTEntity> oldIriEntity = new HashMap<>();
  private final ImportMaps importMaps = new ImportMaps();
  private final Map<String, Integer> used = new HashMap<>();
  private final Map<Integer, Integer> usedDbid = new HashMap<>();
  private final Set<Integer> numericConcepts = new HashSet<>();
  private final Map<String, String> oldIriTerm = new HashMap<>();
  private final Map<String, String> oldIriSnomed = new HashMap<>();
  private final Map<String, Integer> idToDbid = new HashMap<>();
  private final Map<String, TTEntity> iriToConcept = new HashMap<>();
  private final Map<String, String> remaps = new HashMap<>();
  private final Map<String, String> fhirToCore = new HashMap<>();
  private final Map<String, String> im1SchemeToIriTerm = new HashMap<>();
  private TTDocument document;
  private TTDocument statsDocument;
  private Map<String, Set<String>> entities;
  private FileWriter writer;
  private Map<String, String> codeToIri = new HashMap<>();

  private static String getFhirIriTerm(String term) {
    String[] words = term.split(" ");
    StringBuilder iriTerm = new StringBuilder();
    for (int i = 1; i < words.length; i++) {
      iriTerm.append(words[i].toLowerCase());
      if (i < words.length - 1) {
        iriTerm.append("-");
      }
    }
    return iriTerm.toString();
  }

  private static String getPhrase(String iri) {
    iri = iri.substring(0, 1).toUpperCase() + iri.substring(1);
    StringBuilder term = new StringBuilder();
    term.append(iri.charAt(0));
    for (int i = 1; i < iri.length(); i++) {
      if (Character.isUpperCase(iri.charAt(i))) {
        term.append(" ");
      }
      term.append(iri.charAt(i));
    }
    return term.toString();
  }

  @Override
  public void importData(TTImportConfig config) throws ImportException {
    try {
      String inFolder = config.getFolder();
      createFHIRMaps(inFolder);
      EMISImport.populateRemaps(remaps);
      codeToIri = importMaps.getCodeToIri(GRAPH.DISCOVERY);

      importOld(inFolder);


      LOG.info("Retrieving all entities and matches...");
      entities = importMaps.getAllPlusMatches(GRAPH.DISCOVERY);

      try (TTManager manager = new TTManager()) {
        document = manager.createDocument();
        document.addEntity(manager.createScheme(SCHEME.IM1, "IM1 code scheme and graph",
          "The IM1 code scheme and graph"));
        document.addEntity(manager.createScheme(IM.SYSTEM_NAMESPACE, "Computer system scheme and graph",
          "Computer system scheme and graph"));
        newSchemes();
        statsDocument = manager.createDocument();

        importv1Codes(inFolder);
        importContext(inFolder);
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
          filer.fileDocument(document);
        }

        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
          filer.fileDocument(statsDocument);
        }
      }
    } catch (Exception e) {
      throw new ImportException(e.getMessage(), e);
    }
  }

  @Override
  public void validateFiles(String inFolder) {
    ImportUtils.validateFiles(inFolder, im1Codes, oldIris, context, fhirMaps);
  }

  @Override
  public void close() throws Exception {
    used.clear();
    usedDbid.clear();
    numericConcepts.clear();
    oldIriTerm.clear();
    oldIriSnomed.clear();
    idToDbid.clear();
    iriToConcept.clear();
    importMaps.close();
    codeToIri.clear();
  }

  public String getNameSpace(String s) {
    if (s.length() > 10)
      return s.substring(s.length() - 10, s.length() - 3);
    else
      return "";
  }

  private void createFHIRMaps(String inFolder) throws IOException {
    LOG.info("Importing FHIR maps");
    Path file = ImportUtils.findFileForId(inFolder, fhirMaps[0]);
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      String line = reader.readLine();
      while (line != null && !line.isEmpty()) {
        String[] fields = line.split("\t");
        fhirToCore.put(fields[0], fields[1]);
        line = reader.readLine();
      }
    }
  }

  private void importv1Codes(String inFolder) throws Exception {
    LOG.info("Importing IMv1");
    Path zip = ImportUtils.findFileForId(inFolder, im1Codes[0]);
    File file = ZipUtils.unzipFile(zip.getFileName().toString(), zip.getParent().toString());

    int count = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      writer = new FileWriter(file.getParentFile().toString() + "/UnmappedConcepts.txt");
      writer.write("oldIri" + "\t" + "term" + "\t" + "im1Scheme" + "\t" + "code" + "\t" + "description" + "\t" + "used" + "\n");
      reader.readLine();
      String line = reader.readLine();
      while (line != null && !line.isEmpty()) {
        count = processV1Line(line, reader, count);
        line = reader.readLine();
      }
      writer.close();
    }
    LOG.info("Process ended with {}", count);
  }

  private int processV1Line(String line, BufferedReader reader, int count) throws Exception {
    if (line.toLowerCase().contains("accurx")) {
      LOG.trace("Accurx [{}]", line);
    }
    String[] fields = line.split("\t");
    while (fields.length < 6) {
      line = line + reader.readLine();
      fields = line.split("\t");
    }
    Integer dbid = Integer.parseInt(fields[0]);
    String oldIri = fields[1];

    if (oldIri.equals("CM_DidNotAttendEncounter"))
      LOG.info("");
    if (usedDbid.get(dbid) != null)
      used.put(oldIri, usedDbid.get(dbid));
    idToDbid.put(oldIri, dbid);
    String term = fields[2];

    String description = fields[3];
    String im1Scheme = fields[4];
    String code = fields[5];
    String draft = fields[7];

    if (!code.contains(",")) {

      String scheme = getScheme(code, im1Scheme, draft, oldIri);

      if (!scheme.equals("X")) {
        code = processSchemeCode(code, scheme, im1Scheme, term, oldIri, description);
      }

      if (term.contains("Clinical document"))
        LOG.trace("Clinical document [{}|{}]", code, term);
    }
    count++;
    if (count % 100000 == 0) {
      LOG.info("Imported {} im1 concepts", count);
    }
    return count;
  }

  private String processSchemeCode(String code, String scheme, String im1Scheme, String term, String oldIri, String description) throws Exception {
    String lname = code;

    switch (scheme) {
      case SCHEME.TPP -> processTPPCode(code, scheme, im1Scheme, term, oldIri, description, lname);
      case SCHEME.EMIS -> processEMISCode(code, scheme, im1Scheme, term, oldIri, description, lname);
      case SCHEME.ENCOUNTERS -> processEncounterCode(code, scheme, im1Scheme, term, oldIri, description, lname);
      case SCHEME.ICD10 -> processICD10Code(code, scheme, im1Scheme, term, oldIri, description, lname);
      case SCHEME.OPCS4 -> processOPCS4Code(code, scheme, im1Scheme, term, oldIri, description, lname);
      case GRAPH.DISCOVERY -> processDiscoveryCode(code, scheme, im1Scheme, term, oldIri, description, lname);
      case SNOMED.NAMESPACE -> processSnomedCode(code, scheme, im1Scheme, term, oldIri, description, lname);
      case SCHEME.BARTS_CERNER -> processBartsCernerCode(code, scheme, im1Scheme, term, oldIri, description, lname);
      case (FHIR.GRAPH_FHIR) -> addFhir(oldIri, term, im1Scheme, code);
      default -> checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
    }
    return code;
  }

  private void processTPPCode(String code, String scheme, String im1Scheme, String term, String oldIri, String description, String lname) throws IOException {
    lname = lname.replace(".", "_");
    if (entities.containsKey(scheme + lname)) {
      checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
    } else {
      TTIriRef core = importMaps.getReferenceFromCoreTerm(term, GRAPH.DISCOVERY);
      if (core != null) {
        addNewEntity(scheme + lname, core.getIri(), term, code, im1Scheme, oldIri, description, iri(IM.CONCEPT));
      } else {
        checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
      }

    }
  }

  private void processEMISCode(String code, String scheme, String im1Scheme, String term, String oldIri, String description, String lname) throws IOException {
    if (".....".equals(code))
      return;

    String emisCode = code;
    if (emisCode.endsWith(".") && !emisCode.contains("|"))
      emisCode = emisCode.replaceAll("\\.", "");

    String emisConcept = codeToIri.get(SCHEME.EMIS + emisCode);

    if (emisConcept == null && term.contains("SHHAPT")) {
      emisConcept = codeToIri.get(SCHEME.EMIS + emisCode + "-SHHAPT");
    }

    if (emisConcept == null) {
      if (emisCode.contains("|"))
        emisConcept = codeToIri.get(SCHEME.EMIS + (emisCode.substring(emisCode.indexOf("|") + 1)));
      else
        emisConcept = codeToIri.get(SCHEME.EMIS + emisCode + "-1");
    }

    if (emisConcept == null) {
      if (codeToIri.containsKey(SCHEME.VISION + code))
        LOG.trace("IM1 - Invalid EMIS concept - exists as VISION/READ2, ignoring (scheme/code|term) [{}/{}|{}]", im1Scheme, code, term);
      else if (codeToIri.containsKey(SCHEME.TPP + code))
        LOG.trace("IM1 - Invalid EMIS concept - exists as TPP/CTV3, ignoring (scheme/code|term) [{}/{}|{}]", im1Scheme, code, term);
      else {
        if (code.length() < 6) {
          createUnassigned(SCHEME.VISION, lname, im1Scheme, term, code, oldIri, "");
        } else {
          if (code.matches("[0-9]+") && code.length() > 5) {
            System.out.println("Snomed as Read?");
          }
          LOG.warn("IM1 - Invalid EMIS concept (scheme/code|term) [{}/{}|{}]", im1Scheme, code, term);
        }
      }
    } else
      addIM1id(emisConcept, oldIri);
  }

  private void processEncounterCode(String code, String scheme, String im1Scheme, String term, String oldIri, String description, String lname) throws IOException {
    lname = "LE_" + lname;
    if (entities.containsKey(scheme + lname))
      checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
    else {
      TTIriRef core = importMaps.getReferenceFromCoreTerm(term, GRAPH.DISCOVERY);
      if (core != null) {
        addIM1id(core.getIri(), oldIri);
      } else {
        checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
      }

    }
  }

  private void processICD10Code(String code, String scheme, String im1Scheme, String term, String oldIri, String description, String lname) throws IOException {
    String icd10 = lname.replace(".", "");
    if (entities.containsKey(scheme + icd10)) {
      checkEntity(scheme, icd10, im1Scheme, term, code, oldIri, description);
    } else if (lname.endsWith("X")) {
      String realName = lname.split("\\.")[0];
      if (entities.containsKey(scheme + realName)) {
        addIM1id(scheme + realName, oldIri);
      }
    }
  }

  private void processOPCS4Code(String code, String scheme, String im1Scheme, String term, String oldIri, String description, String lname) throws IOException {
    lname = lname.replace(".", "");
    checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
  }

  private void processDiscoveryCode(String code, String scheme, String im1Scheme, String term, String oldIri, String description, String lname) throws Exception {
    lname = lname.replace("\"", "%22");
    if (!checkOld(scheme, lname, term, code, oldIri, description, im1Scheme)) {
      String original = lname;
      if (original.contains("_")) {
        lname = lname.substring(lname.indexOf("_") + 1);
        if (original.startsWith("DM_"))
          lname = CaseUtils.toCamelCase(lname, false);
      }
      if (entities.containsKey(scheme + lname)) {
        checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
      } else {
        TTIriRef core = importMaps.getReferenceFromCoreTerm(term, GRAPH.DISCOVERY);
        if (core != null) {
          addIM1id(core.getIri(), oldIri);
        } else {
          checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
        }
      }
    }
  }

  private void processSnomedCode(String code, String scheme, String im1Scheme, String term, String oldIri, String description, String lname) throws IOException {
    //Crap snomed code
    if (!code.matches("[0-9]+"))
      return;


    String visionNamespace = "1000027";
    if (getNameSpace(code).equals(visionNamespace)) {
      scheme = SCHEME.VISION;
      addNewEntity(scheme + code, null, term, code, im1Scheme, oldIri, description, iri(IM.CONCEPT));
    }
    checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
  }

  private void processBartsCernerCode(String code, String scheme, String im1Scheme, String term, String oldIri, String description, String lname) throws IOException {
    if (entities.containsKey(scheme + lname)) {
      checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
    } else {
      TTIriRef core = importMaps.getReferenceFromCoreTerm(term, GRAPH.DISCOVERY);
      if (core != null) {
        addNewEntity(scheme + lname, core.getIri(), term, code, scheme, oldIri, description, iri(IM.CONCEPT));
      } else {
        checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
      }

    }
  }

  private String getScheme(String code, String im1Scheme, String draft, String oldIri) throws IOException {
    String scheme = null;

    switch (im1Scheme) {
      case "SNOMED":
        scheme = SNOMED.NAMESPACE;
        break;
      case "READ2":
        scheme = SCHEME.EMIS;
        break;
      case "DS_DATE_PREC":
        scheme = "X";
        break;
      case "DM+D":
        scheme = "X";
        break;
      case "CTV3":
        scheme = SCHEME.TPP;
        break;
      case "ICD10":
        scheme = SCHEME.ICD10;
        break;
      case "OPCS4":
        scheme = SCHEME.OPCS4;
        break;
      case "BartsCerner":
        scheme = SCHEME.BARTS_CERNER;
        break;
      case "FHIR_RFP":
      case "FHIR_RFT":
      case "FHIR_AG":
      case "FHIR_EC":
      case "FHIR_RT":
      case "FHIR_RS":
      case "FHIR_AS":
      case "FHIR_MSAT":
      case "FHIR_PRS":
      case "FHIR_AU":
      case "FHIR_CPS":
      case "FHIR_CPU":
      case "FHIR_CEP":
      case "FHIR_FPR":
      case "FHIR_LANG":
        if ("0".equals(draft))
          scheme = FHIR.GRAPH_FHIR;
        else
          scheme = "X";
        break;
      case "EMIS_LOCAL":
        scheme = SCHEME.EMIS;
        break;
      case "TPP_LOCAL":
        scheme = SCHEME.TPP;
        break;
      case "LE_TYPE":
        scheme = SCHEME.ENCOUNTERS;
        break;
      case "VISION_LOCAL":
        scheme = SCHEME.VISION;
        break;
      case "CM_DiscoveryCode":
        if (oldIri.startsWith("FHIR_")) {
          scheme = FHIR.GRAPH_FHIR;
        } else if (oldIri.startsWith("CM_Sys_")) {
          scheme = IM.SYSTEM_NAMESPACE;
        } else {
          scheme = GRAPH.DISCOVERY;
          if (code.startsWith("LPV_Imp_Crn"))
            scheme = "X";
        }
        break;
      case "ImperialCerner":
        scheme = "X";
        break;
      case "NULL":
        if (oldIri.startsWith("FHIR_")) {
          scheme = FHIR.GRAPH_FHIR;
        } else {
          scheme = "X";
        }
        break;
      default:
        if (code.startsWith("DM_"))
          LOG.info("data model property");
        LOG.error("Scheme [{}], Code [{}], Iri [{}]", im1Scheme, code, oldIri);
        scheme = "X";
    }
    if (scheme == null)
      throw new IOException();

    if (code.startsWith("_") && scheme.equals("X")) {
      scheme = SCHEME.ENCOUNTERS;
    }

    return scheme;
  }

  private void addFhir(String oldIri, String term, String im1Scheme, String code) throws IOException {
    String scheme = FHIR.GRAPH_FHIR + "ValueSet/";
    LOG.info("Writing Fhir ValueSet");
    TTEntity entity = new TTEntity();
    TTEntity concept = new TTEntity();

    if ("NULL".equals(code)) {
      // Is parent pseudo-scheme
      setParentConceptAndValueSet(oldIri, term, scheme, entity, concept);
    } else {
      if ("CM_DiscoveryCode".equals(im1Scheme)) {
        setParentConceptAndValueSet(oldIri, term, scheme, entity, concept);
      } else {
        String iriTerm = im1SchemeToIriTerm.get(im1Scheme);
        if (iriTerm == null) {
          LOG.error("Unknown IM1 scheme [{}]", im1Scheme);
          throw new IOException();
        }
        entity.setIri(FHIR.GRAPH_FHIR + iriTerm + "/" + (code.toLowerCase().replace(" ", "-")))
          .setCode(code)
          .addType(iri(IM.CONCEPT))
          .set(iri(IM.IS_A), FHIR.GRAPH_FHIR + iriTerm);
        if (fhirToCore.get(oldIri) != null) {
          entity.addObject(TTIriRef.iri(IM.MATCHED_TO), TTIriRef.iri(fhirToCore.get(oldIri)));
        }

        TTEntity parent = iriToConcept.get(FHIR.GRAPH_FHIR + iriTerm);
        if (parent != null) {
          TTArray arr = parent.get(iri(IM.HAS_CHILDREN));
          if (arr == null) {
            arr = new TTArray();
            parent.set(iri(IM.HAS_CHILDREN), arr);
          }
          arr.add(TTIriRef.iri(entity.getIri()));
        } else {
          LOG.error("Parent undefined");
        }
        TTEntity valueSet = iriToConcept.get(FHIR.GRAPH_FHIR + "ValueSet/" + iriTerm);
        valueSet.addObject(iri(IM.HAS_MEMBER), iri(entity.getIri()));
      }
    }
    if (concept.getIri() != null) {
      concept.setName(term).setScheme(iri(FHIR.GRAPH_FHIR)).set(iri(IM.IM_1_ID), TTLiteral.literal(oldIri));
      document.addEntity(concept);
            /*
            entity.set(iri(IM.DEFINITION), TTLiteral.literal(
                new Query().match(m -> m
                    .setName(term)
                    .setInstanceOf(new Node().setIri(concept.getIri())
                        .setDescendantsOrSelfOf(true))
                )));

             */
    }
    entity.setName(term)
      .setScheme(iri(FHIR.GRAPH_FHIR))
      .set(iri(IM.IM_1_ID), TTLiteral.literal(oldIri));

    if (!"NULL".equals(im1Scheme)) {
      entity.set(iri(IM.IM_1_SCHEME), TTLiteral.literal(im1Scheme));
    }
    document.addEntity(entity);
  }

  private void setParentConceptAndValueSet(String oldIri, String term, String scheme, TTEntity entity, TTEntity concept) {
    im1SchemeToIriTerm.put(oldIri, getFhirIriTerm(term));
    entity.setIri(scheme + im1SchemeToIriTerm.get(oldIri))
      .addType(iri(IM.VALUESET));
    entity.set(iri(IM.IS_CONTAINED_IN), iri(FHIR.VALUESET_FOLDER));
    concept.setIri(FHIR.GRAPH_FHIR + im1SchemeToIriTerm.get(oldIri)).addType(iri(IM.CONCEPT));
    iriToConcept.put(concept.getIri(), concept);
    iriToConcept.put(entity.getIri(), entity);
  }

  private boolean checkOld(String scheme, String lname, String term, String code, String oldIri, String description, String im1Scheme) throws Exception {
    if (oldIriTerm.containsKey(lname)) {
      if (oldIriSnomed.get(lname) != null) {
        String newCode = oldIriSnomed.get(lname);
        if (entities.containsKey(IM.NAMESPACE + newCode)) {
          checkEntity(IM.NAMESPACE, newCode, im1Scheme, term, code,
            oldIri, description);
          return true;
        } else {
          throw new Exception("mssing snomed " + lname + " " +
            oldIriSnomed.get(lname));
        }
      } else {
        String oldTerm = oldIriTerm.get(lname);
        TTIriRef core = importMaps.getReferenceFromCoreTerm(oldTerm, GRAPH.DISCOVERY);
        if (core != null) {
          lname = core.getIri().split("#")[1];
          checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
          return true;
        }
      }
    }
    return false;
  }

  private void checkEntity(String scheme, String lname, String im1Scheme, String term, String code, String oldIri,
                           String description) throws IOException {

    if (entities.containsKey(scheme + lname)) {
      addIM1id(scheme + lname, oldIri);
    } else {
      if (oldIri.startsWith("CM_")) {
        String potential = oldIri.substring(oldIri.lastIndexOf("_") + 1);
        TTIriRef core = importMaps.getReferenceFromCoreTerm(getPhrase(potential), GRAPH.DISCOVERY);
        if (core != null) {
          addIM1id(core.getIri(), oldIri);
        } else
          createUnassigned(scheme, lname, im1Scheme, term, code, oldIri, description);
      } else {
        if (!scheme.equals(SNOMED.NAMESPACE)) {
          createUnassigned(scheme, lname, im1Scheme, term, code, oldIri, description);
        }

      }
    }

  }

  private void createUnassigned(String scheme, String lname, String im1Scheme, String term, String code, String oldIri,
                                String description) throws IOException {
    TTEntity unassigned = new TTEntity();

    unassigned.setGraph(TTIriRef.iri(scheme));
    unassigned.setIri(scheme + lname);
    unassigned.addType(iri(IM.CONCEPT));
    unassigned.setStatus(iri(IM.UNASSIGNED));
    unassigned.set(iri(IM.IM_1_ID), TTLiteral.literal(oldIri));
    unassigned.setName(term);
    if (description != null)
      unassigned.setDescription(description);
    if (code != null) {
      unassigned.set(iri(IM.CODE), TTLiteral.literal(code));
      unassigned.setScheme(TTIriRef.iri(scheme));
    }
    unassigned.set(iri(IM.IM_1_SCHEME), TTLiteral.literal(im1Scheme));
    if (scheme.equals(SCHEME.ENCOUNTERS))
      unassigned.set(iri(IM.PRIVACY_LEVEL), TTLiteral.literal(1));
    if (used.get(oldIri) != null && used.get(oldIri) > 0)
      unassigned.set(iri(IM.USAGE_TOTAL), TTLiteral.literal(used.get(oldIri)));
    if (scheme.equals(IM.NAMESPACE)) {
      if (oldIri.startsWith("DM_")) {
        unassigned.set(iri(RDF.TYPE), iri(RDF.PROPERTY));
      } else
        unassigned.set(iri(RDF.TYPE), iri(IM.CONCEPT));
    } else
      unassigned.set(iri(RDF.TYPE), iri(IM.CONCEPT));
    oldIriEntity.put(oldIri, unassigned);
    document.addEntity(unassigned);
    writer.write(oldIri + "\t" + term + "\t" + im1Scheme + "\t" + code + "\t" + description + "\t" + (used.getOrDefault(oldIri, 0)) + "\n");

  }

  private TTEntity addNewCoreEntity(String newIri, String term, TTIriRef type) {
    TTIriRef graph = TTIriRef.iri(newIri.substring(0, newIri.lastIndexOf("#") + 1));
    TTEntity entity = new TTEntity()
      .addType(type)
      .setGraph(graph)
      .setIri(newIri)
      .setName(term)
      .setScheme(TTIriRef.iri(newIri.substring(0, newIri.lastIndexOf("#") + 1)));
    document.addEntity(entity);
    return entity;
  }

  private TTEntity addNewEntity(String newIri, String matchedIri, String term, String code, String scheme, String oldIri,
                                String description, TTIriRef type) {
    TTIriRef graph = TTIriRef.iri(newIri.substring(0, newIri.lastIndexOf("#") + 1));
    TTEntity entity = new TTEntity()
      .addType(type)
      .setGraph(graph)
      .setIri(newIri)
      .setName(term)
      .setScheme(TTIriRef.iri(newIri.substring(0, newIri.lastIndexOf("#") + 1)));

    if (code != null)
      entity.setCode(code);
    if (description != null) {
      entity.setDescription(description);
    }
    if (oldIri != null)
      entity.set(iri(IM.IM_1_ID), TTLiteral.literal(oldIri));
    if (scheme != null)
      entity.set(iri(IM.IM_1_SCHEME), TTLiteral.literal(scheme));
    if (matchedIri != null) {
      entity.addObject(iri(IM.MATCHED_TO), TTIriRef.iri(matchedIri));
      entity.setStatus(iri(IM.DRAFT));
      Set<String> matches = entities.get(matchedIri);
      if (matches != null) {
        for (String iri : matches)
          entity.addObject(iri(IM.MATCHED_TO), TTIriRef.iri(iri));
      }
    } else {
      if (entity.getScheme().equals(iri(GRAPH.DISCOVERY)))
        entity.setStatus(iri(IM.DRAFT));
      else
        entity.setStatus(iri(IM.UNASSIGNED));
    }
    if (used.containsKey(oldIri))
      entity.set(iri(IM.USAGE_TOTAL), TTLiteral.literal(used.get(oldIri)));
    document.addEntity(entity);
    if (oldIri.equals("CM_CritCareSrcLctn04"))
      LOG.info("CM_CritCareSrcLctn04");
    oldIriEntity.put(oldIri, entity);
    return entity;
  }

  private void addIM1id(String iri, String oldIri) {
    TTEntity im1 = new TTEntity().setIri(iri).addType(iri(IM.CONCEPT));
    TTIriRef graph = TTIriRef.iri(iri.substring(0, iri.lastIndexOf("#") + 1));
    im1.setGraph(graph);
    im1.addObject(iri(IM.IM_1_ID), TTLiteral.literal(oldIri));

    Integer usedCount = 0;
    if (used.containsKey(oldIri) && im1.get(iri(IM.USAGE_TOTAL)) != null)
      usedCount = im1.get(iri(IM.USAGE_TOTAL)).asLiteral().intValue();

    if (used.get(oldIri) != null)
      usedCount = used.get(oldIri);
    if (idToDbid.get(oldIri) != null) {
      Integer dbid = idToDbid.get(oldIri);
      if (numericConcepts.contains(dbid))
        im1.set(iri(IM.HAS_NUMERIC), TTLiteral.literal("true"));
      Integer dbused = usedDbid.get(dbid);
      if (dbused != null)
        usedCount = usedCount + dbused;
    }
    if (usedCount > 0)
      im1.set(iri(IM.USAGE_TOTAL), TTLiteral.literal(usedCount));
    if (oldIri.equals("CM_CritCareSrcLctn04"))
      LOG.info("CM_CritCareSrcLctn04");
    oldIriEntity.put(oldIri, im1);
    document.addEntity(im1);
  }

  private void importContext(String inFolder) throws IOException, DataFormatException {
    Set<String> contexts = new HashSet<>();
    Set<String> nodeValues = new HashSet<>();
    HashMap<String, TTEntity> nodeMaps = new HashMap<>();
    for (String statsFile : context) {
      Path file = ImportUtils.findFilesForId(inFolder, statsFile).get(0);
      LOG.info("Retrieving context maps from... {}", file.toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine();  // NOSONAR - Skipping header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          String[] fields = line.split("\t");
          String node = fields[0];
          String publisher = fields[1];
          String system = fields[2];
          String schema = fields[3];
          String table = fields[4];
          String field = fields[5];
          String targetProperty = fields[6];
          String sourceValue = fields[7];
          if (sourceValue.equals("NULL"))
            sourceValue = null;
          String lookupConcept = fields[8];

          String regexSrc = fields[9];
          if (regexSrc.equals("NULL"))
            regexSrc = null;
          String regex = fields[10];
          if (regex.equals("NULL"))
            regex = null;
          String regexConcept = fields[11];

          TTIriRef scheme = getPublisherSystemScheme(publisher, system);

          TTEntity propertyEntity = oldIriEntity.get(targetProperty);
          TTIriRef propertyIri;
          if (propertyEntity == null) {
            String lname = targetProperty.split("_")[1];
            if (!entities.containsKey(IM.NAMESPACE + lname)) {
              propertyEntity = addNewEntity(IM.NAMESPACE + lname,
                null, getTerm(lname), null, "CM_DiscoveryCode", targetProperty, null, iri(RDF.PROPERTY));
              propertyIri = TTIriRef.iri(propertyEntity.getIri());
              propertyEntity.addObject(iri(RDFS.SUBCLASS_OF), TTIriRef.iri(IM.NAMESPACE + "dataModelObjectProperty"));
            } else {
              propertyIri = TTIriRef.iri(IM.NAMESPACE + lname);
            }
          } else
            propertyIri = TTIriRef.iri(propertyEntity.getIri());

          String contextId = publisher + "/" + system + "/" + schema + "/" + table + "/" + field;
          String nodeIri = IM.CONTEXT_NODE + node;

          if (!contexts.contains(contextId)) {
            createContextEntity(contexts, publisher, system, schema, table, field, contextId, nodeIri);
          }

          TTEntity mapNode = nodeMaps.get(nodeIri);
          if (mapNode == null) {
            mapNode = createMapNode(nodeMaps, nodeIri, node, propertyIri);
          }

          TTArray maps = mapNode.get(iri(IM.HAS_MAP));

          String oldIri = "NULL".equals(lookupConcept) ? regexConcept : lookupConcept;

          TTEntity entity = oldIriEntity.get(oldIri);
          if (entity == null) {
            entity = addOldEntity(scheme, oldIri, publisher, system, sourceValue, regexSrc, regex);
          }


          if (sourceValue != null) {
            if (!nodeValues.contains(node + "/" + sourceValue)) {
              nodeValues.add(node + "/" + sourceValue);
              TTNode map = new TTNode();
              maps.add(map);
              map.set(iri(IM.SOURCE_VALUE), sourceValue);
              map.set(iri(IM.CONCEPT), entity.getIri());
            }
          } else if (regexSrc != null) {
            if (!nodeValues.contains(node + "/" + regexSrc + "/" + regex)) {
              nodeValues.add(node + "/" + regexSrc + "/" + regex);
              TTNode map = new TTNode();
              maps.add(map);
              map.set(iri(IM.SOURCE_VALUE), regexSrc);
              map.set(iri(IM.SOURCE_REGEX), regex);
              map.set(iri(IM.CONCEPT), entity.getIri());
            }
          } else {
            LOG.error("Both source value and regex is null - unhandled context function?");
          }

          line = reader.readLine();
        }
      }
    }
  }

  private TTEntity createMapNode(HashMap<String, TTEntity> nodeMaps, String nodeIri, String node, TTIriRef propertyIri) {
    TTEntity mapNode = new TTEntity(nodeIri)
      .addType(iri(IM.CONCEPT))
      .set(iri(IM.HAS_MAP), new TTArray())
      .setName(node)
      .set(iri(IM.TARGET_PROPERTY), propertyIri);

    document.addEntity(mapNode);
    nodeMaps.put(nodeIri, mapNode);

    return mapNode;
  }

  private void createContextEntity(Set<String> contexts, String publisher, String system, String schema, String table, String field, String contextId, String nodeIri) {
    contexts.add(contextId);
    TTEntity context = new TTEntity(IM.SOURCE_CONTEXT + "/" + UUID.randomUUID());
    context.addType(iri(IM.CONCEPT));
    document.addEntity(context);
    TTIriRef organisation;
    if (organisationMap.get(publisher) != null) {
      organisation = new TTIriRef().setIri(ORG.ORGANISATION_NAMESPACE + organisationMap.get(publisher));
    } else
      organisation = new TTIriRef().setIri(ORG.ORGANISATION_NAMESPACE + UUID.randomUUID());
    context.set(iri(IM.SOURCE_PUBLISHER), organisation);
    if (!"NULL".equals(system)) context.set(iri(IM.SOURCE_SYSTEM), new TTIriRef(IM.SYSTEM_NAMESPACE + system));
    if (!"NULL".equals(schema)) context.set(iri(IM.SOURCE_SCHEMA), TTLiteral.literal(schema));
    if (!"NULL".equals(table)) context.set(iri(IM.SOURCE_TABLE), TTLiteral.literal(table));
    if (!"NULL".equals(field)) context.set(iri(IM.SOURCE_FIELD), TTLiteral.literal(field));
    context.set(iri(IM.CONTEXT_NODE), new TTIriRef(nodeIri));
  }

  private String getTerm(String lname) {
    StringBuilder term = new StringBuilder(lname.substring(0, 1));
    for (int i = 1; i < lname.length(); i++) {
      if (Character.isUpperCase(lname.charAt(i)))
        term.append(" ").append(lname.charAt(i));
      else
        term.append(lname.charAt(i));

    }
    return term.toString();
  }

  private TTEntity addOldEntity(TTIriRef newScheme, String oldIri, String publisher, String system, String value, String headerCode, String regex) throws IOException {
    String term = null;
    if (headerCode != null) {
      term = oldIriTerm.get(headerCode);
      if (term != null && regex != null) {
        term = "Orginal term : " + headerCode + " parsed " + regex;
      }
    }


    publisher = publisher.split("_")[2];
    system = system.split("_")[2];
    if (term == null) {
      term = "Original term : " + value + "(" + publisher + " " + system + ")";
    }
    if (oldIri.startsWith("CM_"))
      oldIri = oldIri.split("_")[1];
    if (value == null)
      value = oldIri;


    TTEntity entity = new TTEntity()
      .setGraph(newScheme)
      .setIri(newScheme.getIri() + oldIri)
      .addType(iri(IM.CONCEPT))
      .setName(term)
      .setScheme(newScheme)
      .setCode(value)
      .set(iri(IM.IM_1_ID), TTLiteral.literal(oldIri))
      .setStatus(iri(IM.UNASSIGNED));
    document.addEntity(entity);
    oldIriEntity.put(oldIri, entity);
    if (value != null) {
      String coreTerm = getPhrase(oldIri);
      TTIriRef core = importMaps.getReferenceFromCoreTerm(coreTerm, GRAPH.DISCOVERY);
      if (core != null)
        entity.addObject(iri(IM.MATCHED_TO), TTIriRef.iri(core.getIri()));
    }

    return entity;
  }

  private TTIriRef getPublisherSystemScheme(String publisher, String system) throws DataFormatException {
    if (publisher.equals("CM_Org_Barts") && system.equals("CM_Sys_Cerner"))
      return iri(SCHEME.BARTS_CERNER);
    if (publisher.equals("CM_Org_BHRUT") && system.equals("CM_Sys_Medway"))
      return (TTIriRef.iri(IM.DOMAIN + "bhrutm#"));
    if (publisher.equals("CM_Org_CWH") && system.equals("CM_Sys_Cerner"))
      return (TTIriRef.iri(IM.DOMAIN + "cwhcc#"));
    if (publisher.equals("CM_Org_Imperial") && system.equals("CM_Sys_Cerner"))
      return (TTIriRef.iri(IM.DOMAIN + "impc#"));
    if (publisher.equals("CM_Org_Kings") && system.equals("CM_Sys_PIMS"))
      return (TTIriRef.iri(IM.DOMAIN + "kingsp#"));
    if (publisher.equals("CM_Org_LNWH"))
      if (system.equals("CM_Sys_Silverlink")) {
        return (TTIriRef.iri(IM.DOMAIN + "lnwhsl#"));
      } else if (system.equals("CM_Sys_Symphony")) {
        return TTIriRef.iri(IM.DOMAIN + "lnwhsy#");
      }
    if (publisher.equals("CM_Org_THH") && system.equals("CM_Sys_Silverlink"))
      return (TTIriRef.iri(IM.DOMAIN + "thhsl#"));
    throw new DataFormatException("Unrecognised publisher and system : " + publisher + " " + system);

  }

  private void newSchemes() {
    TTEntity newScheme = addNewCoreEntity(IM.DOMAIN + "bhrutm#", "BHRUT Medway code scheme and graph",
      iri(IM.GRAPH));
    newScheme.addObject(iri(RDFS.SUBCLASS_OF), iri(IM.GRAPH));
    newScheme = addNewCoreEntity(IM.DOMAIN + "cwhcc#", "CWHC Cerner code scheme and graph",
      iri(IM.GRAPH));
    newScheme.addObject(iri(RDFS.SUBCLASS_OF), iri(IM.GRAPH));
    newScheme = addNewCoreEntity(IM.DOMAIN + "impc#", "Imperial Cerner code scheme and graph",
      iri(IM.GRAPH));
    newScheme.addObject(iri(RDFS.SUBCLASS_OF), iri(IM.GRAPH));
    newScheme = addNewCoreEntity(IM.DOMAIN + "kingsp#", "KCH PIMS code scheme and graph",
      iri(IM.GRAPH));
    newScheme.addObject(iri(RDFS.SUBCLASS_OF), iri(IM.GRAPH));
    newScheme = addNewCoreEntity(IM.DOMAIN + "lnwhsl#", "LNWH Silverlink code scheme and graph",
      iri(IM.GRAPH));
    newScheme.addObject(iri(RDFS.SUBCLASS_OF), iri(IM.GRAPH));
    newScheme = addNewCoreEntity(IM.DOMAIN + "lnwhsy#", "LNWH Symphony code scheme and graph",
      iri(IM.GRAPH));
    newScheme.addObject(iri(RDFS.SUBCLASS_OF), iri(IM.GRAPH));
    newScheme = addNewCoreEntity(IM.DOMAIN + "thhsl#", "THH Silverlink code scheme and graph",
      iri(IM.GRAPH));
    newScheme.addObject(iri(RDFS.SUBCLASS_OF), iri(IM.GRAPH));

  }

  private void importOld(String inFolder) throws IOException {
    for (String oldFile : oldIris) {
      Path file = ImportUtils.findFilesForId(inFolder, oldFile).get(0);
      LOG.info("Retrieving old iri maps from... {}", file.toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine();  // NOSONAR - Skipping header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          String[] fields = line.split("\t");
          String oldIri = fields[0];
          if (oldIri.startsWith(":"))
            oldIri = oldIri.substring(1);
          String snomed = fields[1];
          String term = fields[2];
          if (oldIri.startsWith("CM"))
            oldIriSnomed.put(oldIri, snomed);
          oldIriTerm.put(oldIri, term);
          line = reader.readLine();
        }
      }
    }
  }

}
