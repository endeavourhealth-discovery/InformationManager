package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.vocabulary.Graph;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.imapi.model.imq.QueryException;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;


public class DiscoveryReportsImporter implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(DiscoveryReportsImporter.class);

  private static final String[] ReportsConcepts = {".*\\\\StatsReports-inferred.json"};

  public void validateFiles(String inFolder) {
    ImportUtils.validateFiles(inFolder, ReportsConcepts);
  }

  /**
   * Imports the reports document
   *
   * @param config import configuration
   * @return TTImport object builder pattern
   * @throws Exception invalid document
   */
  public void importData(TTImportConfig config) throws ImportException {
    LOG.info("Importing Reports concepts");
    try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
      TTDocument document = loadFile(config.getFolder());
      filer.fileDocument(document);
    } catch (Exception e) {
      throw new ImportException(e.getMessage(), e);
    }
  }

  /**
   * Loads the core ontology document, available as TTDocument for various purposes
   *
   * @param inFolder root folder containing the document
   * @return TTDocument containing Discovery ontology
   * @throws IOException in the event of an IO failure
   */
  public TTDocument loadFile(String inFolder) throws IOException {
    Path file = ImportUtils.findFileForId(inFolder, ReportsConcepts[0]);
    try (TTManager manager = new TTManager()) {
      return manager.loadDocument(file.toFile());
    }
  }

  @Override
  public void close() throws Exception {

  }
}
