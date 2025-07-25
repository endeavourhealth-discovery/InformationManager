package org.endeavourhealth.informationmanager.transforms.sources;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.model.imq.ECLQueryRequest;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.imapi.model.customexceptions.EclFormatException;
import org.endeavourhealth.imapi.model.imq.Bool;
import org.endeavourhealth.imapi.model.imq.Match;
import org.endeavourhealth.imapi.model.imq.Query;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.ECLToIMQ;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
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

public class SnomedImporter implements TTImport {
  public static final String[] concepts = {
    ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_InternationalRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_Snapshot_INT_.*\\.txt",
    ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKCLSnapshot_.*\\.txt",
    ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKEditionRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKEDSnapshot_.*\\.txt",
    ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRefsetsRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKCRSnapshot_.*\\.txt",
    ".*\\\\PRIMARY\\\\.*\\\\SnomedCT_UKPrimaryCareRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKPCSnapshot_.*\\.txt",
    ".*\\\\DRUG\\\\.*\\\\SnomedCT_UKDrugRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKDGSnapshot_.*\\.txt",
    ".*\\\\DRUG\\\\.*\\\\SnomedCT_UKEditionRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKEDSnapshot_.*\\.txt"
  };
  public static final String[] refsets = {
    ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRefsetsRF2_.*\\\\Snapshot\\\\Refset\\\\Content\\\\der2_Refset_SimpleUKCRSnapshot_.*\\.txt",
    ".*\\\\PRIMARY\\\\.*\\\\SnomedCT_UKPrimaryCareRF2_.*\\\\Snapshot\\\\Refset\\\\Content\\\\der2_Refset_SimpleUKPCSnapshot_.*\\.txt"
  };
  public static final String[] dmd_vmp = {
    ".*\\\\DMD\\\\.*\\\\f_vmp_VmpType.csv"
  };
  public static final String[] dmd_amp = {
    ".*\\\\DMD\\\\.*\\\\f_amp_AmpType.csv"
  };
  public static final String[] dmd_vpi = {
    ".*\\\\DMD\\\\.*\\\\f_vmp_VpiType.csv"
  };
  public static final String[] dmd_route = {
    ".*\\\\DMD\\\\.*\\\\f_vmp_DrugRouteType.csv"
  };
  public static final String[] dmd_form = {
    ".*\\\\DMD\\\\.*\\\\f_vmp_DrugFormType.csv"
  };
  public static final String[] descriptions = {
    ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_InternationalRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_Snapshot-en_INT_.*\\.txt",
    ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKCLSnapshot-en_.*\\.txt",
    ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKEditionRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKEDSnapshot-en_.*\\.txt",
    ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRefsetsRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKCRSnapshot-en_.*\\.txt",
    ".*\\\\PRIMARY\\\\.*\\\\SnomedCT_UKPrimaryCareRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKPCSnapshot-en_.*\\.txt",
    ".*\\\\DRUG\\\\.*\\\\SnomedCT_UKDrugRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKDGSnapshot-en_.*\\.txt",
    ".*\\\\DRUG\\\\.*\\\\SnomedCT_UKEditionRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKEDSnapshot-en_.*\\.txt"
  };
  public static final String[] relationships = {
    ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_InternationalRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_Snapshot_INT_.*\\.txt",
    ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKCLSnapshot_.*\\.txt",
    ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKEditionRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKEDSnapshot_.*\\.txt",
    ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRefsetsRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKCRSnapshot_.*\\.txt",
    ".*\\\\PRIMARY\\\\.*\\\\SnomedCT_UKPrimaryCareRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKPCSnapshot_.*\\.txt",
    ".*\\\\DRUG\\\\.*\\\\SnomedCT_UKDrugRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKDGSnapshot_.*\\.txt",
    ".*\\\\DRUG\\\\.*\\\\SnomedCT_UKEditionRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKEDSnapshot_.*\\.txt"
  };
  public static final String[] substitutions = {
    ".*\\\\HISTORY\\\\.*\\\\SnomedCT_UKClinicalRF2_.*\\\\Resources\\\\QueryTable\\\\xres2_SNOMEDQueryTable_.*\\.txt",
  };
  public static final String[] attributeRanges = {
    ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_InternationalRF2_.*\\\\Snapshot\\\\Refset\\\\Metadata\\\\der2_ssccRefset_MRCMAttributeRangeSnapshot_INT_.*\\.txt",
  };
  public static final String[] attributeDomains = {
    ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_InternationalRF2_.*\\\\Snapshot\\\\Refset\\\\Metadata\\\\der2_cissccRefset_MRCMAttributeDomainSnapshot_INT_.*\\.txt",
  };
  public static final String[] usage_clinical = {
    ".*\\\\DiscoveryNoneCore\\\\SNOMED_code_usage_[0-9\\\\-]*.txt"
  };
  public static final String[] usage_drug = {
    ".*\\\\DiscoveryNoneCore\\\\SNOMED_drug_usage_[0-9\\\\-]*.txt"
  };

  public static final String[] pcdClusters = {
    ".*\\\\QOF\\\\.*\\_PCD_Refset_Content.txt"};

  public static final String FULLY_SPECIFIED = "900000000000003001";
  public static final String DEFINED = "900000000000073002";
  public static final String IS_A = "116680003";
  public static final String SN = "http://snomed.info/sct#";
  public static final String ALL_CONTENT = "723596005";
  public static final String ACTIVE = "1";
  public static final String REPLACED_BY = "370124000";
  public static final String SNOMED_ATTRIBUTE = "sn:106237007";
  public static final String SNOMED_REFERENCE_SETS = Namespace.IM + "SnomedCTReferenceSets";
  private static final Logger LOG = LoggerFactory.getLogger(SnomedImporter.class);
  private final ECLToIMQ eclConverter = new ECLToIMQ();
  private Map<String, TTEntity> conceptMap;
  private Map<String, TTEntity> refsetMap;
  private TTDocument document;
  private final Map<String, Set<String>> vmp_ingredient = new HashMap<>();
  private final Map<String, String> vmp_route = new HashMap<>();
  private final Map<String, String> vmp_form = new HashMap<>();

  //======================PUBLIC METHODS============================

  /**
   * Loads a multi country RF2 release package into a Discovery ontology will process international followed by uk clinical
   * followed by uk drug. Loads MRCM models also. Does not load reference sets.
   *
   * @param config import configuration
   * @throws ImportException thrown from document filer
   */

  @Override
  public void importData(TTImportConfig config) throws ImportException {
    validateFiles(config.getFolder());
    conceptMap = new HashMap<>();
    try (TTManager dmanager = new TTManager()) {

      document = dmanager.createDocument();
      TTEntity scheme = dmanager.createNamespaceEntity(
        Namespace.SNOMED,
        "Snomed-CT code scheme and graph",
        "An international or UK Snomed code scheme and graph. This does not include supplier specfic, local, or Discovery namespace extensions"
      );
      scheme.addObject(TTIriRef.iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.CORE_SCHEMES));
      document.addEntity(scheme);

      importConceptFiles(config.getFolder());
      importDescriptionFiles(config.getFolder());
      // removeQualifiers(document);
      importMRCMRangeFiles(config.getFolder());
      importMRCMDomainFiles(config.getFolder());
      // importStatedFiles(config.folder); No longer bothers with OWL axioms;
      importRelationshipFiles(config.getFolder());
      importSubstitution(config.getFolder());
      importDmdContents(config.getFolder());
      importVmp(config.getFolder());
      importAmp(config.getFolder());
      addSpecials(document);
      importClinicalUsage(config.getFolder());
      importDrugUsage(config.getFolder());

      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
        filer.fileDocument(document);
      }

      document = dmanager.createDocument();
      setRefSetRoot();
      importRefsetFiles(config.getFolder());
      try (QOFRefSetImport qofImporter = new QOFRefSetImport(document, conceptMap)) {
        qofImporter.importData(config);
      }
      conceptMap.clear();

      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
        filer.fileDocument(document);
      }

    } catch (Exception ex) {
      throw new ImportException(ex.getMessage());
    }


  }

  private void addSpecials(TTDocument document) {
    //Snomed telephone is a device
    TTEntity telephone = new TTEntity()
      .setIri(Namespace.SNOMED + "359993007")
      .setScheme(Namespace.SNOMED.asIri())
      .setCrud(iri(IM.ADD_QUADS));
    telephone.addObject(iri(RDFS.SUBCLASS_OF), iri(Namespace.IM + "71000252102"));
    document.addEntity(telephone);
    TTEntity specific = conceptMap.get("10362801000001104");
    specific.addObject(iri(RDFS.SUBCLASS_OF), iri(Namespace.SNOMED + "127489000"));
    specific = conceptMap.get("10363001000001101");
    specific.addObject(iri(RDFS.SUBCLASS_OF), iri(Namespace.SNOMED + "127489000"));

    TTEntity dmd = conceptMap.get("8653001000001100");
    dmd.addObject(TTIriRef.iri(RDFS.DOMAIN), TTIriRef.iri(Namespace.SNOMED + "763158003"));
    dmd.addObject(TTIriRef.iri(RDFS.RANGE), TTIriRef.iri(Namespace.SNOMED + "8653201000001106"));
  }


  private void setRefSetRoot() {
    TTEntity root = new TTEntity()
      .setIri(SNOMED_REFERENCE_SETS)
      .setName("Snomed-CT reference sets")
      .addType(iri(IM.FOLDER))
      .setScheme(Namespace.SNOMED.asIri());
    root.set(iri(IM.IS_CONTAINED_IN), new TTArray().add(iri(Namespace.IM + "QueryConceptSets")));
    document.addEntity(root);
    conceptMap.put(root.getIri(), root);

  }


  private void importSubstitution(String path) throws IOException {
    int i = 0;
    for (String relationshipFile : substitutions) {
      Path file = ImportUtils.findFilesForId(path, relationshipFile).get(0);
      LOG.info("Processing substitutions in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine(); // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          String[] fields = line.split("\t");
          String supertype = fields[0];
          if (!supertype.equals("138875005")) {
            String subtype = fields[1];
            if (!subtype.equals(supertype)) {
              String provenance = fields[2];
              TTEntity subEntity = conceptMap.get(subtype);
              if (subEntity == null) {
                subEntity = new TTEntity().setIri(SN + subtype)
                  .addType(iri(IM.CONCEPT))
                  .setStatus(iri(IM.INACTIVE))
                  .setCode(subtype)
                  .setScheme(iri(Namespace.SNOMED));
                document.addEntity(subEntity);
              }
              TTEntity superEntity = conceptMap.get(supertype);
              if (superEntity == null) {
                superEntity = new TTEntity().setIri(SN + supertype)
                  .addType(iri(IM.CONCEPT))
                  .setStatus(iri(IM.INACTIVE))
                  .setCode(supertype)
                  .setScheme(iri(Namespace.SNOMED));
                document.addEntity(subEntity);
              }
              String subStatus = subEntity.getStatus().getIri();
              String superStatus = superEntity.getStatus().getIri();
              i++;

              switch (provenance) {
                case "0" -> {
                  if (superStatus.equals(IM.ACTIVE)) {
                    if (subStatus.equals(IM.INACTIVE)) {
                      subEntity.addObject(iri(IM.SUBSUMED_BY), iri(SN + supertype));
                    }
                  } else {
                    subEntity.addObject(iri(IM.SUBSUMED_BY), iri(SN + supertype));
                  }
                }
                case "1" -> {
                  if (superStatus.equals(IM.ACTIVE)) {
                    if (subStatus.equals(IM.INACTIVE)) {
                      subEntity.addObject(iri(IM.SUBSUMED_BY), iri(SN + supertype));
                    }
                  } else {
                    subEntity.addObject(iri(IM.MAY_BE_SUBSUMED_BY), iri(SN + supertype));
                  }
                }
                case "2" -> subEntity.addObject(iri(IM.APPROXIMATE_SUBSUMED_BY), iri(SN + supertype));
                case "3" -> {
                  if (superStatus.equals(IM.ACTIVE)) {
                    if (subStatus.equals(IM.INACTIVE)) {
                      subEntity.addObject(iri(IM.SUBSUMED_BY), iri(SN + supertype));
                    }
                  } else {
                    if (subStatus.equals(IM.ACTIVE)) {
                      subEntity.addObject(iri(IM.APPROXIMATE_SUBSUMED_BY), iri(SN + supertype));

                    }
                  }
                }
              }
            }
          }
          line = reader.readLine();
        }
      }
    }
    LOG.info("Imported {} subsumed concepts", i);
  }


  //=================private methods========================


  private void importDmdContents(String path) throws IOException {
    int i = 0;
    for (String[] dmdFile : List.of(dmd_vpi, dmd_route, dmd_form)) {
      for (String conceptFile : dmdFile) {
        Path file = ImportUtils.findFilesForId(path, conceptFile).get(0);
        LOG.info("Processing concepts in {}", file.getFileName().toString());
        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
          reader.readLine();     // NOSONAR - Skip header
          String line = reader.readLine();
          while (line != null && !line.isEmpty()) {
            String[] fields = line.split("\\|");
            if (dmdFile == dmd_vpi) {
              vmp_ingredient.computeIfAbsent(fields[0], e -> new HashSet<>()).add(fields[1]);
            } else if (dmdFile == dmd_route) {
              vmp_route.put(fields[0], fields[1]);
            } else if (dmdFile == dmd_form) {
              vmp_form.put(fields[0], fields[1]);
            }
            i++;
            line = reader.readLine();
          }
        }
      }
    }
    LOG.info("Imported {} concepts", i);
  }

  private void importVmp(String path) throws IOException {
    int i = 0;
    for (String conceptFile : dmd_vmp) {
      Path file = ImportUtils.findFilesForId(path, conceptFile).get(0);
      LOG.info("Processing concepts in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine();     // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          processVmpLine(line);

          i++;
          line = reader.readLine();
        }
      }
    }
    LOG.info("Imported {} concepts", i);
  }

  private void processVmpLine(String line) {
    String[] fields = line.split("\\|");
    String code = fields[0];
    TTEntity c = conceptMap.get(code);
    if (c != null) {
      c.set(iri(IM.PREFERRED_NAME), TTLiteral.literal(fields[5]));
      if (!TTManager.termUsed(c, fields[5]))
        TTManager.addTermCode(c, fields[5], null);
      if (!fields[2].equals("")) {
        TTEntity prev = conceptMap.get(fields[2]);
        if (prev != null) {
          prev.addObject(TTIriRef.iri(IM.PREVIOUS_ENTITY_OF), TTIriRef.iri(c.getIri()));
        }
      }
      setDmdProperties(c, code);
    }
  }

  private void setDmdProperties(TTEntity entity, String code) {
    if (entity.get(iri(IM.ROLE_GROUP)) == null) {
      String route = vmp_route.get(code);
      String form = vmp_form.get(code);
      Set<String> ingredients = vmp_ingredient.get(code);
      if (route != null || form != null || ingredients != null) {
        TTNode group = new TTNode();
        entity.addObject(iri(IM.ROLE_GROUP), group);
        if (route != null) {
          group.set(iri(Namespace.SNOMED + "26643006"), iri(Namespace.SNOMED + route));
        }
        if (form != null) {
          group.set(iri(Namespace.SNOMED + "10362901000001105"), iri(Namespace.SNOMED + form));
        }
        if (ingredients != null) {
          int i = 0;
          for (String ingredient : ingredients) {
            i++;
            if (i == 1)
              group.set(iri(Namespace.SNOMED + "127489000"), iri(Namespace.SNOMED + ingredient));
            else {
              TTNode newGroup = new TTNode();
              entity.addObject(iri(IM.ROLE_GROUP), newGroup);
              newGroup.set(iri(Namespace.SNOMED + "127489000"), iri(Namespace.SNOMED + ingredient));
            }
          }
        }
      }
    }
  }


  private void importAmp(String path) throws IOException {
    int i = 0;
    for (String conceptFile : dmd_amp) {
      Path file = ImportUtils.findFilesForId(path, conceptFile).get(0);
      LOG.info("Processing concepts in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine();     // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          processAmpLine(line);
          i++;
          line = reader.readLine();
        }
      }
    }
    LOG.info("Imported {} concepts", i);
  }

  private void processAmpLine(String line) {
    String[] fields = line.split("\\|");
    TTEntity amp = conceptMap.get(fields[0]);
    if (amp != null) {
      if (amp.get(iri(IM.ROLE_GROUP)) == null) {
        String vmpid = fields[2];
        if (amp.get(iri(IM.ROLE_GROUP)) == null) {
          setDmdProperties(amp, vmpid);
        }
      }
    }
  }


  private void importConceptFiles(String path) throws IOException {
    int i = 0;
    for (String conceptFile : concepts) {
      Path file = ImportUtils.findFilesForId(path, conceptFile).get(0);
      LOG.info("Processing concepts in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine();     // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          processConceptLine(conceptFile, line);

          i++;
          line = reader.readLine();
        }
      }
    }
    LOG.info("Imported {} concepts", i);
  }

  private void processConceptLine(String conceptFile, String line) {
    String[] fields = line.split("\t");
    if (!conceptMap.containsKey(fields[0])) {
      TTEntity c = new TTEntity();
      c.setIri(SN + fields[0]);
      c.setCode(fields[0]);
      c.setScheme(iri(Namespace.SNOMED));
      if (conceptFile.contains("Refset") || conceptFile.contains("UKPrimaryCare"))
        c.addType(iri(IM.CONCEPT_SET));
      else
        c.addType(iri(IM.CONCEPT));
      if (fields[4].equals(DEFINED))
        c.set(iri(IM.DEFINITIONAL_STATUS), iri(IM.SUFFICIENTLY_DEFINED));
      c.setStatus(ACTIVE.equals(fields[2]) ? iri(IM.ACTIVE) : iri(IM.INACTIVE));
      if (fields[0].equals("138875005")) { // snomed root
        c.set(iri(IM.IS_CONTAINED_IN), new TTArray().add(iri(Namespace.IM + "HealthModelOntology")));
        c.set(iri(SHACL.ORDER), TTLiteral.literal(1));
      }
      document.addEntity(c);
      conceptMap.put(fields[0], c);
    } else {
      TTEntity c = conceptMap.get(fields[0]);
      c.setStatus(ACTIVE.equals(fields[2]) ? iri(IM.ACTIVE) : iri(IM.INACTIVE));

    }
  }

  private void importClinicalUsage(String path) throws IOException {
    int i = 0;
    refsetMap = new HashMap<>();
    for (String usageFile : usage_clinical) {
      List<Path> paths = ImportUtils.findFilesForId(path, usageFile);
      Path file = paths.get(0);
      LOG.info("Processing usages in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine();     // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          processClinicalUsageLine(line);
          i++;
          line = reader.readLine();
        }

      }
    }
    LOG.info("Imported {} usages", i);
  }

  private void processClinicalUsageLine(String line) {
    String[] fields = line.split("\t");
    TTEntity c = conceptMap.get(fields[0]);
    try {
      Integer usage = Integer.parseInt(fields[1]);
      c.set(iri(IM.USAGE_TOTAL), TTLiteral.literal(usage));
    } catch (NumberFormatException ignored) {
    }

  }

  private void importDrugUsage(String path) throws IOException {
    int i = 0;
    refsetMap = new HashMap<>();
    for (String usageFile : usage_drug) {
      List<Path> paths = ImportUtils.findFilesForId(path, usageFile);
      Path file = paths.get(0);
      LOG.info("Processing usages in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine();     // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          processDrugUsageLine(line);
          i++;
          line = reader.readLine();
        }

      }
    }
    LOG.info("Imported {} usages", i);
  }

  private void processDrugUsageLine(String line) {
    String[] fields = line.split("\t");
    TTEntity c = conceptMap.get(fields[0]);
    try {
      Integer usage = Integer.parseInt(fields[2]);
      c.set(iri(IM.USAGE_TOTAL), TTLiteral.literal(usage));
    } catch (NumberFormatException ignored) {
    }

  }


  private void importRefsetFiles(String path) throws IOException {
    int i = 0;
    refsetMap = new HashMap<>();
    for (String refsetFile : refsets) {
      List<Path> paths = ImportUtils.findFilesForId(path, refsetFile);
      Path file = paths.get(0);
      LOG.info("Processing refsets in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine();     // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          processRefsetLine(line);
          i++;
          line = reader.readLine();
        }

      }

    }
    LOG.info("Imported {} refset", i);
  }

  private void processRefsetLine(String line) {
    String[] fields = line.split("\t");
    if (fields[2].equals(ACTIVE)) {
      TTEntity c = conceptMap.get(fields[4]);
      c.setType(new TTArray().add(iri(IM.CONCEPT_SET)));
      if (refsetMap.get(fields[4]) == null) {
        refsetMap.put(fields[4], c);
        document.addEntity(c);
      }
      c.set(iri(IM.IS_CONTAINED_IN), iri(SNOMED_REFERENCE_SETS));
      c.setScheme(Namespace.SNOMED.asIri());
      c.addObject(iri(IM.HAS_MEMBER), iri(Namespace.SNOMED + fields[5]));
    }
  }

  private void importDescriptionFiles(String path) throws IOException {
    int i = 0;
    for (String descriptionFile : descriptions) {

      Path file = ImportUtils.findFilesForId(path, descriptionFile).get(0);

      LOG.info("Processing  descriptions in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine();  // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          processDescriptionLine(line);
          i++;
          line = reader.readLine();
        }
      }

    }
    LOG.info("Imported {} descriptions", i);
  }

  private void processDescriptionLine(String line) {
    String[] fields = line.split("\t");
    TTEntity c = conceptMap.get(fields[4]);
    String term = fields[7];

    if (c != null) {
      if (term.contains("(attribute)")) {
        c.addType(iri(RDF.PROPERTY));
      }
      if (FULLY_SPECIFIED.equals(fields[6]) || c.getName() == null) {
        c.setName(term);
        if (term.contains(" General practice data extraction - ")) {
          term = term.split(" General practice data extraction - ")[1];
          if (term.contains(" simple reference set")) {
            term = term.split(" simple reference set")[0];
            term = StringUtils.capitalize(term) + " (NHS GP value set)";
          }
      }
      if (ACTIVE.equals(fields[2]))
        TTManager.addTermCode(c, term, fields[0], iri(IM.ACTIVE));
      else
        TTManager.addTermCode(c, null, fields[0], iri(IM.INACTIVE));
      }
    }
  }


  private void importMRCMDomainFiles(String path) throws IOException {
    int i = 0;

    //gets attribute domain files (usually only 1)
    for (String domainFile : attributeDomains) {
      Path file = ImportUtils.findFilesForId(path, domainFile).get(0);
      LOG.info("Processing property domains in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine();     // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          String[] fields = line.split("\t");
          //Only process axioms relating to all snomed authoring
          if (fields[11].equals(ALL_CONTENT)) {
            TTEntity op = conceptMap.get(fields[5]);
            addSnomedPropertyDomain(op, fields[6]);
          }
          i++;
          line = reader.readLine();
        }
      }
    }
    LOG.info("Imported {} property domain axioms", i);
  }

  private void importMRCMRangeFiles(String path) throws IOException, EclFormatException {
    int i = 0;
    //gets attribute range files (usually only 1)
    for (String rangeFile : attributeRanges) {
      Path file = ImportUtils.findFilesForId(path, rangeFile).get(0);
      LOG.info("Processing property ranges in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine();     // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          String[] fields = line.split("\t");
          if (fields[2].equals("1")) {
            TTEntity op = conceptMap.get(fields[5]);
            addSnomedPropertyRange(op, fields[6]);
          }
          i++;
          line = reader.readLine();
        }
      }
    }
    LOG.info("Imported {} property range axioms", i);
  }

  private void addSnomedPropertyRange(TTEntity op, String ecl) throws EclFormatException {
    if (ecl.matches("^[a-zA-Z].*")) {
      return;
    }
    ECLQueryRequest eclQuery = new ECLQueryRequest();
    eclQuery.setEcl(ecl);
    eclConverter.getQueryFromECL(eclQuery);
    Query expression = eclQuery.getQuery();
    if (expression.getInstanceOf() != null) {
      op.addObject(iri(RDFS.RANGE), iri(expression.getInstanceOf().get(0).getIri()));
    }
    if (expression.getOr() != null) {
      for (Match match : expression.getOr()) {
        if (match.getInstanceOf() != null) {
          op.addObject(iri(RDFS.RANGE), iri(match.getInstanceOf().get(0).getIri()));
        } else {
          if (match.getOr() != null) {
            for (Match or : match.getOr()) {
              op.addObject(iri(RDFS.RANGE), iri(or.getInstanceOf().get(0).getIri()));
            }
          } else
            throw new EclFormatException("ecl of this kind is not supported for ranges");
        }
      }
    }
  }


  private void addSnomedPropertyDomain(TTEntity op, String domain) {
    //Assumes all properties may or may nor in a group
    //therefore groups are not modelled in this version
    if (op.get(iri(RDFS.DOMAIN)) == null)
      op.set(iri(RDFS.DOMAIN), new TTArray());
    op.get(iri(RDFS.DOMAIN)).add(iri(SN + domain));
  }

  private void importRelationshipFiles(String path) throws IOException {
    int i = 0;
    for (String relationshipFile : relationships) {
      Path file = ImportUtils.findFilesForId(path, relationshipFile).get(0);
      LOG.info("Processing relationships in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine();  // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          processRelationshipLine(line);
          i++;
          line = reader.readLine();
        }
      }

    }
    LOG.info("Imported {} relationships", i);
  }

  private void processRelationshipLine(String line) {
    String[] fields = line.split("\t");
    TTEntity c = conceptMap.get(fields[4]);
    if (c != null) {
      int group = Integer.parseInt(fields[6]);
      String relationship = fields[7];
      String target = fields[5];
      if (conceptMap.get(target) == null) {
        System.err.println("Missing target entity in relationship" + target);
      }
      if (ACTIVE.equals(fields[2]) || (relationship.equals(REPLACED_BY))) {
        addRelationship(c, group, relationship, target);
      }
    }
  }

  private void addIsa(TTEntity entity, String parent) {
    TTIriRef isa = iri(RDFS.SUBCLASS_OF);
    if (entity.get(isa) == null) {
      TTArray isas = new TTArray();
      entity.set(isa, isas);
    }
    TTArray isas = entity.get(isa);
    isas.add(iri(SN + parent));

  }

  private void addRelationship(TTEntity c, Integer group, String relationship, String target) {
    if (relationship.equals(IS_A)) {
      addIsa(c, target);
      if (c.getIri().equals(SNOMED_ATTRIBUTE))
        c.addObject(iri(RDFS.SUBCLASS_OF), iri(RDF.PROPERTY));
    } else {
      TTNode roleGroup = getRoleGroup(c, group);
      roleGroup.addObject(iri(SN + relationship), iri(SN + target));
    }
  }

  private TTNode getRoleGroup(TTEntity c, Integer groupNumber) {
    // if (groupNumber==0)
    //  return c;
    if (c.get(iri(IM.ROLE_GROUP)) == null) {
      TTArray roleGroups = new TTArray();
      c.set(iri(IM.ROLE_GROUP), roleGroups);
    }
    TTArray groups = c.get(iri(IM.ROLE_GROUP));
    for (TTValue group : groups.getElements()) {
      if (Integer.parseInt(group.asNode().get(iri(IM.GROUP_NUMBER)).asLiteral().getValue()) == groupNumber)
        return group.asNode();
    }
    TTNode newGroup = new TTNode();
    TTLiteral groupCount = TTLiteral.literal(groupNumber.toString());
    groupCount.setType(iri(XSD.INTEGER));
    newGroup.set(iri(IM.GROUP_NUMBER), groupCount);
    groups.add(newGroup);
    return newGroup;
  }

  public void validateFiles(String inFolder) {
    ImportUtils.validateFiles(inFolder, usage_clinical, concepts, descriptions,
      relationships, refsets, attributeRanges, attributeDomains, substitutions, dmd_vmp, dmd_amp, dmd_form, dmd_route,
      dmd_vpi, usage_clinical, usage_drug, pcdClusters);

  }

  @Override
  public void close() throws Exception {
    if (conceptMap != null)
      conceptMap.clear();
  }
}
