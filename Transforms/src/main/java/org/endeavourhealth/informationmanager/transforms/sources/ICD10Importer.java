package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.logic.exporters.ImportMaps;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.DataFormatException;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class ICD10Importer implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(ICD10Importer.class);

  private static final String[] entities = {".*\\\\ICD10\\\\.*\\\\Content\\\\ICD10_Edition5_CodesAndTitlesAndMetadata_GB_.*\\.txt"};
  private static final String[] maps = {".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRF2_PRODUCTION_.*\\\\Snapshot\\\\Refset\\\\Map\\\\der2_iisssciRefset_ExtendedMapUKCLSnapshot_GB1000000_.*\\.txt"};
  private static final String[] chapters = {".*\\\\ICD10\\\\ICD10-Chapters.txt"};

  private final TTIriRef icd10Codes = TTIriRef.iri(GRAPH.ICD10 + "ICD10Codes");
  private final TTManager manager = new TTManager();
  private Set<String> snomedCodes;
  private final Map<String, TTEntity> startChapterMap = new HashMap<>();
  private final List<String> startChapterList = new ArrayList<>();
  private TTDocument document;
  private TTDocument mapDocument;
  private final Map<String, TTEntity> codeToEntity = new HashMap<>();
  private final Map<String, TTEntity> altCodeToEntity = new HashMap<>();
  private final ImportMaps importMaps = new ImportMaps();

  @Override
  public void importData(TTImportConfig config) throws Exception {
    validateFiles(config.getFolder());
    LOG.info("Importing ICD10....");
    LOG.info("Getting snomed codes");
    snomedCodes = importMaps.getCodes(SNOMED.NAMESPACE);
    document = manager.createDocument(GRAPH.ICD10);
    document.addEntity(manager.createGraph(GRAPH.ICD10, "ICD10  code scheme and graph", "The ICD10 code scheme and graph including links to core"));
    createTaxonomy();
    importChapters(config.getFolder(), document);
    importEntities(config.getFolder(), document);
    createHierarchy();


    mapDocument = manager.createDocument(GRAPH.ICD10);
    importMaps(config.getFolder());
    try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
      filer.fileDocument(document);
    }
    try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
      filer.fileDocument(mapDocument);
    }
  }

  private void createHierarchy() {
    Collections.sort(startChapterList);
    for (Map.Entry<String, TTEntity> entry : codeToEntity.entrySet()) {
      String code = entry.getKey();
      TTEntity icd10Entity = entry.getValue();
      if (code.contains(".")) {
        String qParent = code.substring(0, code.indexOf("."));
        TTEntity parent = codeToEntity.get(qParent);
        icd10Entity.addObject(iri(IM.IS_CHILD_OF), TTIriRef.iri(parent.getIri()));
      } else {
        int insertion = Collections.binarySearch(startChapterList, code);
        int parentIndex;
        if (insertion > -1)
          parentIndex = insertion;
        else
          parentIndex = -(insertion + 1) - 1;
        String qParent = startChapterList.get(parentIndex);
        TTEntity parent = startChapterMap.get(qParent);
        // LOG.info("{} in {}?", code, parent.getCode());
        icd10Entity.addObject(iri(IM.IS_CHILD_OF), TTIriRef.iri(parent.getIri()));
      }

    }

  }

  private void createTaxonomy() {
    TTEntity icd10 = new TTEntity()
      .setIri(icd10Codes.getIri())
      .setName("ICD10 5th edition classification codes")
      .addType(iri(IM.CONCEPT))
      .setCode("ICD10Codes")
      .setScheme(iri(GRAPH.ICD10))
      .setDescription("ICD1O classification used in backward maps from Snomed");
    icd10.addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "CodeBasedTaxonomies"));
    document.addEntity(icd10);

  }

  public void importMaps(String folder) throws IOException, DataFormatException {

    validateFiles(folder);
    Path file = ImportUtils.findFileForId(folder, maps[0]);
    ComplexMapImporter mapImport = new ComplexMapImporter();
    mapImport.importMap(file.toFile(), mapDocument, altCodeToEntity, "999002271000000101", snomedCodes);
  }


  private void importChapters(String folder, TTDocument document) throws IOException {

    Path file = ImportUtils.findFileForId(folder, chapters[0]);
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      reader.readLine();  // NOSONAR - Skipping header
      String line = reader.readLine();
      int count = 0;
      while (line != null && !line.isEmpty()) {
        count++;
        if (count % 10000 == 0) {
          LOG.info("Processed {} records", count);
        }
        String[] fields = line.split("\t");
        String iri = GRAPH.ICD10 + fields[1];
        String code = fields[1];
        String label = "Chapter " + fields[0] + ": " + fields[2];
        TTEntity c = new TTEntity()
          .setCode(code)
          .setName(label)
          .setIri(iri)
          .setScheme(iri(GRAPH.ICD10))
          .setStatus(iri(IM.ACTIVE))
          .addType(iri(IM.CONCEPT));
        c.addObject(iri(IM.IS_CHILD_OF), icd10Codes);
        startChapterMap.put(code.substring(0, code.indexOf("-")), c);
        startChapterList.add(code.substring(0, code.indexOf("-")));
        document.addEntity(c);
        line = reader.readLine();
      }
      LOG.info("Process ended with {} chapter records", count);
    }

  }


  private void importEntities(String folder, TTDocument document) throws IOException {

    Path file = ImportUtils.findFileForId(folder, entities[0]);
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      reader.readLine();  // NOSONAR - Skipping header
      String line = reader.readLine();
      int count = 0;
      while (line != null && !line.isEmpty()) {
        count++;
        if (count % 10000 == 0) {
          LOG.info("Processed {} records", count);
        }
        String[] fields = line.split("\t");
        TTEntity c = new TTEntity()
          .setCode(fields[0])
          .setScheme(iri(GRAPH.ICD10))
          .setIri(GRAPH.ICD10 + (fields[0].replace(".", "")))
          .addType(iri(IM.CONCEPT));
        if (fields[4].length() > 250) {
          c.setName(fields[4].substring(0, 200));
          c.setDescription(fields[4]);
        } else {
          c.setName(fields[4]);
        }


        codeToEntity.put(fields[0], c);
        altCodeToEntity.put(fields[1], c);
        document.addEntity(c);
        line = reader.readLine();
      }
      LOG.info("Process ended with {} entities", count);

    }

  }

  public void validateFiles(String path) {
    ImportUtils.validateFiles(path, entities, maps, chapters);
  }

  @Override
  public void close() throws Exception {
    startChapterMap.clear();
    startChapterList.clear();
    codeToEntity.clear();
    altCodeToEntity.clear();
    importMaps.close();
    manager.close();
  }
}
