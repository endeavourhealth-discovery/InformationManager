package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.interfacemanager.model.*;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class NHSTfcImport implements TTImport {
  private static final String[] treatmentCodes = {".*\\\\NHSDD\\\\TreatmentFunctionCodes.txt"};
  private TTManager manager = new TTManager();
  private TTDocument document;
  private TTIriRef nhsTfc;

  @Override
  public void importData(TTImportConfig config) throws ImportException {
    try {
      document = manager.createDocument();
      document.addEntity(manager.createNamespaceEntity(NAMESPACE.NHS_TFC,
        "NHS Data Dictionary Speciality and Treatment function codes"
        , "NHS Data dictionary concepts that are not snomed"));
      setNHSDD();
      importFunctionCodes(config.getFolder());
      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(GRAPH.IM)) {
        filer.fileDocument(document);
      }
    } catch (Exception ex) {
      throw new ImportException(ex.getMessage(),ex);
    }
  }

  private void setNHSDD() {
    nhsTfc = new TTIriRef(NAMESPACE.NHS_TFC + "NHSTfc");
    TTEntity nhs = new TTEntity()
      .setIri(nhsTfc.getIri())
      .setName("Main Specialty and Treatment Function Codes")
      .setScheme(new TTIriRef(NAMESPACE.NHS_TFC))
      .setCode("0")
      .addType(new TTIriRef(IM.CONCEPT))
      .setStatus(new TTIriRef(IM.ACTIVE));
    nhs.addObject(new TTIriRef(IM.IS_CONTAINED_IN), new TTIriRef(NAMESPACE.IM + "CodeBasedTaxonomies"));
    document.addEntity(nhs);
  }

  private void importFunctionCodes(String folder) throws IOException {

    Path file = ImportUtils.findFileForId(folder, treatmentCodes[0]);
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      reader.readLine();  // NOSONAR - Skipping header
      String line = reader.readLine();
      int count = 0;
      while (line != null && !line.isEmpty()) {
        count++;
        String[] fields = line.split("\t");
        String code = fields[0];
        String term = fields[1];
        String snomed = fields[2];
        TTEntity tfc = new TTEntity()
          .setIri(NAMESPACE.NHS_TFC + code)
          .setName(term)
          .setScheme(new TTIriRef(NAMESPACE.NHS_TFC))
          .setCode(code)
          .addType(new TTIriRef(IM.CONCEPT))
          .setStatus(new TTIriRef(IM.ACTIVE));
        tfc.addObject(new TTIriRef(IM.IS_CHILD_OF), nhsTfc);
        tfc.addObject(new TTIriRef(IM.MATCHED_TO), new TTIriRef(NAMESPACE.SNOMED + snomed));
        document.addEntity(tfc);
        line = reader.readLine();
      }
    }
  }

  @Override
  public void validateFiles(String inFolder) throws TTFilerException {
    ImportUtils.validateFiles(inFolder, treatmentCodes);
  }

  @Override
  public void close() throws Exception {
    manager.close();
  }
}
