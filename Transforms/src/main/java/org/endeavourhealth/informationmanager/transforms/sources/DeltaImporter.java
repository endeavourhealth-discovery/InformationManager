package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.imq.QueryException;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DeltaImporter implements TTImport {
  private static final String[] delta = {".*\\\\Deltas"};

  @Override
  public void importData(TTImportConfig config) throws ImportException {
    try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
      Path file = ImportUtils.findFileForId(config.getFolder(), delta[0]);
      filer.fileDeltas(file.toString());
    } catch(Exception ex) {
      throw new ImportException(ex.getMessage(),ex);
    }
  }

  @Override
  public void validateFiles(String inFolder) throws TTFilerException {
    if (!Files.exists(Paths.get(inFolder + "\\\\Deltas")))
      throw new TTFilerException(("No files found in [" + inFolder + "\\Deltas" + "]"));
  }

  @Override
  public void close() throws Exception {

  }
}
