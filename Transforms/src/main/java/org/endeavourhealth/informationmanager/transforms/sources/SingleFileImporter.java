package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.logic.reasoner.Reasoner;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import java.io.File;
import java.io.IOException;
import java.util.zip.DataFormatException;

public class SingleFileImporter implements TTImport {


  private static TTDocument generateInferred(TTDocument document) throws IOException, DataFormatException, OWLOntologyCreationException {

    Reasoner reasoner = new Reasoner();
    TTDocument inferred = reasoner.generateInferred(document);
    inferred = reasoner.inheritShapeProperties(inferred);
    return inferred;
  }


  @Override
  public void importData(TTImportConfig ttImportConfig) throws Exception {
    try (TTManager manager = new TTManager()) {
      manager.loadDocument(new File(ttImportConfig.getFolder().replaceAll("%", " ")));
      manager.setDocument(generateInferred(manager.getDocument()));
      TTDocumentFiler filer = TTFilerFactory.getDocumentFiler();
      filer.fileDocument(manager.getDocument());
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
