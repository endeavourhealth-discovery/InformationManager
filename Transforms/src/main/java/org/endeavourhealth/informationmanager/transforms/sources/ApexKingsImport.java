package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.logic.exporters.ImportMaps;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class ApexKingsImport implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(ApexKingsImport.class);

  private static final String[] kingsPath = {".*\\\\Kings\\\\KingsPathMap.txt"};
  private static final String KINGS_APEX_CODES = SCHEME.KINGS_APEX + "KingsApexCodes";
  private TTDocument document;
  private Map<String, Set<String>> readToSnomed = new HashMap<>();
  private final Map<String, String> apexToRead = new HashMap<>();
  private final ImportMaps importMaps = new ImportMaps();


  @Override
  public void importData(TTImportConfig config) throws ImportException {

    try (TTManager manager = new TTManager()) {
      document = manager.createDocument();
      document.addEntity(manager.createScheme(SCHEME.KINGS_APEX, "Kings Apex pathology code scheme and graph",
        "The Kings Apex LIMB local code scheme and graph"));

      importR2Matches();
      setTopLevel();
      importApexKings(config.getFolder());
      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
        filer.fileDocument(document);
      }
    } catch (Exception e) {
      throw new ImportException(e.getMessage(),e);
    }
  }

  private void setTopLevel() {
    TTEntity kings = new TTEntity()
      .setIri(KINGS_APEX_CODES)
      .addType(iri(IM.CONCEPT))
      .setName("Kings College Hospital Apex path codes")
      .setCode("KingsApexCodes")
      .setScheme(iri(SCHEME.KINGS_APEX))
      .setDescription("Local codes for the Apex pathology system in kings")
      .set(iri(IM.IS_CONTAINED_IN), new TTArray().add(TTIriRef.iri(IM.NAMESPACE + "CodeBasedTaxonomies")));
    document.addEntity(kings);
  }


  private void importR2Matches() throws SQLException, TTFilerException, IOException {
    LOG.info("Retrieving read vision 2 snomed map");
    readToSnomed = importMaps.importReadToSnomed(GRAPH.DISCOVERY);

  }

  private void importApexKings(String folder) throws IOException {
    LOG.info("Importing kings code file");

    Path file = ImportUtils.findFileForId(folder, kingsPath[0]);
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      Stream<String> lines = reader.lines().skip(1);

      AtomicInteger count = new AtomicInteger();
      lines.forEachOrdered(line -> {
        String[] fields = line.split("\t");
        String readCode = fields[0];
        String code = fields[1];
        String iri = SCHEME.KINGS_APEX + (fields[1].replaceAll("[ .,\"%]", ""));
        TTEntity entity = new TTEntity()
          .setIri(iri)
          .addType(iri(IM.CONCEPT))
          .setName(fields[2])
          .setDescription("Local apex Kings trust pathology system entity ")
          .setCode(code)
          .setScheme(iri(SCHEME.KINGS_APEX))
          .set(iri(IM.IS_CHILD_OF), new TTArray().add(TTIriRef.iri(KINGS_APEX_CODES)));
        document.addEntity(entity);
        apexToRead.put(code, readCode);
        if (readToSnomed.get(readCode) != null) {
          for (String snomed : readToSnomed.get(readCode)) {
            entity.addObject(iri(IM.MATCHED_TO), TTIriRef.iri(SNOMED.NAMESPACE + snomed));
          }
        }
        count.getAndIncrement();
        if (count.get() % 500 == 0) {
          LOG.info("Processed {} records", count);
        }
      });
      LOG.info("Process ended with {} records", count);
    }

  }


  @Override
  public void validateFiles(String inFolder) {
    ImportUtils.validateFiles(inFolder, kingsPath);
  }


  @Override
  public void close() throws Exception {
    readToSnomed.clear();
    apexToRead.clear();
    importMaps.close();
  }
}
