package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.logic.exporters.ImportMaps;
import org.endeavourhealth.imapi.model.tripletree.TTArray;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class WinPathKingsImport implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(WinPathKingsImport.class);

  private static final String[] kingsWinPath = {".*\\\\Kings\\\\Winpath.txt"};
  private TTDocument document;
  private Map<String, Set<String>> readToSnomed = new HashMap<>();
  private final ImportMaps importMaps = new ImportMaps();

  @Override
  public void importData(TTImportConfig config) throws ImportException {
    try (TTManager manager = new TTManager()) {
      document = manager.createDocument();
      document.addEntity(manager.createNamespaceEntity(Namespace.KINGS_WINPATH,
        "Kings Winpath pathology code scheme and graph",
        "The Kings pathology Winpath LIMB local code scheme and graph"));
      setTopLevel();
      importR2Matches();
      importWinPathKings(config.getFolder());
      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
        filer.fileDocument(document, Graph.IM);
      }
    } catch (Exception e) {
      throw new ImportException(e.getMessage(),e);
    }
  }

  private void setTopLevel() {
    TTEntity kings = new TTEntity()
      .setIri(Namespace.KINGS_WINPATH + "KingsWinPathCodes")
      .addType(iri(IM.CONCEPT))
      .setName("Kings College Hospital  Winpath codes")
      .setCode("KingsWinPathCodes")
      .setScheme(iri(Namespace.KINGS_WINPATH))
      .setDescription("Local codes for the Winpath pathology system in kings")
      .set(iri(IM.IS_CONTAINED_IN), new TTArray().add(TTIriRef.iri(Namespace.IM + "CodeBasedTaxonomies")));
    document.addEntity(kings);
  }


  private void importR2Matches() throws TTFilerException, IOException {
    LOG.info("Retrieving read vision 2 snomed map");
    readToSnomed = importMaps.importReadToSnomed(List.of(Graph.IM));

  }

  private void importWinPathKings(String folder) throws IOException {
    LOG.info("Importing kings code file");

    Path file = ImportUtils.findFileForId(folder, kingsWinPath[0]);
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      reader.readLine(); // NOSONAR - Skip header
      String line = reader.readLine();
      int count = 0;
      while (line != null && !line.isEmpty()) {
        String[] fields = line.split("\t");
        String readCode = fields[2];
        String code = fields[0];
        String iri = Namespace.KINGS_WINPATH + (fields[0].replaceAll("[ %,.\"]", ""));
        TTEntity entity = new TTEntity()
          .setIri(iri)
          .addType(iri(IM.CONCEPT))
          .setName(fields[1])
          .setDescription("Local winpath Kings trust pathology system entity ")
          .setScheme(iri(Namespace.KINGS_WINPATH))
          .set(iri(IM.IS_CHILD_OF), new TTArray().add(TTIriRef.iri(Namespace.KINGS_APEX + "KingsWinPathCodes")))
          .setCode(code);
        document.addEntity(entity);
        if (readToSnomed.get(readCode) != null) {
          for (String snomed : readToSnomed.get(readCode)) {
            entity.addObject(iri(IM.MATCHED_TO), TTIriRef.iri(Namespace.SNOMED + snomed));
          }
        }

        count++;
        if (count % 500 == 0) {
          LOG.info("Processed {} records", count);
        }
        line = reader.readLine();
      }
      LOG.info("Process ended with {} records", count);
    }

  }


  @Override
  public void validateFiles(String inFolder) {
    ImportUtils.validateFiles(inFolder, kingsWinPath);
  }


  @Override
  public void close() throws Exception {
    readToSnomed.clear();
    importMaps.close();
  }
}
