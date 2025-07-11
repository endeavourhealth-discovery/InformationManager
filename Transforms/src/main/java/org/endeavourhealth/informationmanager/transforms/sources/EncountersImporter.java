package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.vocabulary.Graph;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public class EncountersImporter implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(EncountersImporter.class);

  private static final String[] encounters = {".*\\\\DiscoveryNoneCore\\\\Encounters.json"};


  public void importData(TTImportConfig config) throws ImportException {
    LOG.info("Importing Discovery encounters");
    try {
      importNoneCoreFile(config);
    } catch (Exception ex) {
      throw new ImportException(ex.getMessage(),ex);
    }
  }

  private void importNoneCoreFile(TTImportConfig config) throws Exception {
    Path file = ImportUtils.findFileForId(config.getFolder(), encounters[0]);
    try(TTManager manager = new TTManager()) {
      TTDocument document = manager.loadDocument(file.toFile());
      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
        filer.fileDocument(document);
      }
    }
  }

  public void validateFiles(String inFolder) {
    ImportUtils.validateFiles(inFolder, encounters);
  }


  @Override
  public void close() throws Exception {

  }
}
