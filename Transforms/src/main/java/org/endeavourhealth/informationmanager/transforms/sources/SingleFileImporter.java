package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.logic.reasoner.Reasoner;
import org.endeavourhealth.imapi.model.imq.QueryException;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class SingleFileImporter implements TTImport {


  private static TTDocument generateInferred(TTDocument document) throws OWLOntologyCreationException {

    Reasoner reasoner = new Reasoner();
    TTDocument inferred = reasoner.generateInferred(document);
    inferred = reasoner.inheritShapeProperties(inferred);
    return inferred;
  }


  @Override
  public void importData(TTImportConfig ttImportConfig) throws ImportException {
    try (TTManager manager = new TTManager()) {
      manager.loadDocument(new File(ttImportConfig.getFolder().replaceAll("%", " ")));
      manager.setDocument(generateInferred(manager.getDocument()));
      TTDocumentFiler filer = TTFilerFactory.getDocumentFiler();
      filer.fileDocument(manager.getDocument());
    } catch (Exception ex) {
      throw new ImportException(ex.getMessage(), ex);
    }
  }

  @Override
  public void validateFiles(String fileName) throws TTFilerException {
    fileName = fileName.replaceAll("%", " ");
    if (!new File(fileName).exists())
      throw new TTFilerException(fileName + " not found");
  }

  @Override
  public void close() throws Exception {

  }
}
