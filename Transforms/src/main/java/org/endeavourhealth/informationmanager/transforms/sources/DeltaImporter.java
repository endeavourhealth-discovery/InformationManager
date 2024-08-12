package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.imq.QueryException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DeltaImporter implements TTImport {
  private static final String[] delta = {".*\\\\Deltas"};

  @Override
  public void importData(TTImportConfig config) throws IOException, TTFilerException, QueryException {
    Path file = ImportUtils.findFileForId(config.getFolder(), delta[0]);
    try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
      filer.fileDeltas(file.toString());
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
