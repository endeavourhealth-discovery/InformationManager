package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.filer.rdf4j.TTTransactionFiler;
import org.endeavourhealth.imapi.vocabulary.Graph;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class DeltaImporter implements TTImport {
  private static final String[] delta = {".*\\\\Deltas"};

  @Override
  public void importData(TTImportConfig config) throws ImportException {
    try (TTTransactionFiler filer = new TTTransactionFiler(Graph.IM)) {
      Path file = ImportUtils.findFileForId(config.getFolder(), delta[0]);
      filer.fileDeltas(file.toString(), Graph.IM);
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
