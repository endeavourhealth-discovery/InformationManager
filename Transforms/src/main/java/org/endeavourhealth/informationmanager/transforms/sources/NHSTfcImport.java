package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;

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
      document.addEntity(manager.createNamespaceEntity(Namespace.NHS_TFC,
        "NHS Data Dictionary Speciality and Treatment function codes"
        , "NHS Data dictionary concepts that are not snomed"));
      setNHSDD();
      importFunctionCodes(config.getFolder());
      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
        filer.fileDocument(document);
      }
    } catch (Exception ex) {
      throw new ImportException(ex.getMessage(),ex);
    }
  }

  private void setNHSDD() {
    nhsTfc = TTIriRef.iri(Namespace.NHS_TFC + "NHSTfc");
    TTEntity nhs = new TTEntity()
      .setIri(nhsTfc.getIri())
      .setName("Main Specialty and Treatment Function Codes")
      .setScheme(iri(Namespace.NHS_TFC))
      .setCode("0")
      .addType(iri(IM.CONCEPT))
      .setStatus(iri(IM.ACTIVE));
    nhs.addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(Namespace.IM + "CodeBasedTaxonomies"));
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
          .setIri(Namespace.NHS_TFC + code)
          .setName(term)
          .setScheme(iri(Namespace.NHS_TFC))
          .setCode(code)
          .addType(iri(IM.CONCEPT))
          .setStatus(iri(IM.ACTIVE));
        tfc.addObject(iri(IM.IS_CHILD_OF), nhsTfc);
        tfc.addObject(iri(IM.MATCHED_TO), TTIriRef.iri(Namespace.SNOMED + snomed));
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
