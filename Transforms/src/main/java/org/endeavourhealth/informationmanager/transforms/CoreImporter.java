package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.transforms.ReasonerPlus;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.*;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.zip.DataFormatException;

public class CoreImporter implements TTImport {
   private static final String[] coreEntities = {
     ".*\\\\SemanticWeb\\\\RDFOntology.json",
     ".*\\\\SemanticWeb\\\\RDFSOntology.json",
     ".*\\\\SemanticWeb\\\\OWLOntology.json",
     ".*\\\\SemanticWeb\\\\SHACLOntology.json",
     ".*\\\\DiscoveryCore\\\\CoreOntology.json",
     ".*\\\\DiscoveryCore\\\\CoreOntology-more-inferred.json",
  ".*\\\\DiscoveryCore\\\\StatsReports.json"
   };



   public CoreImporter validateFiles(String inFolder){
      ImportUtils.validateFiles(inFolder,coreEntities);
      return this;
   }


   /**
    * Imports the core ontology document
    * @param config import config
    * @return TTImport object builder pattern
    * @throws Exception invalid document
    */
   @Override
   public TTImport importData(TTImportConfig config) throws Exception {
     System.out.println("Generating inferred ontologies...");
     generateInferred(config);
     importNamespaces();
     System.out.println("Importing Core entities");
      for (String coreFile : coreEntities) {
        if (!coreFile.contains("-inferred.json"))
          coreFile = coreFile.substring(0, coreFile.indexOf(".json")) + "-inferred.json";
        TTManager manager = new TTManager();
         Path path = ImportUtils.findFileForId(config.folder, coreFile);
         manager.loadDocument(path.toFile());
         TTDocument document= manager.getDocument();
        System.out.println("Filing  "+ document.getGraph().getIri() + " from " + coreFile);
         try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
             filer.fileDocument(document);
         }
      }
      CoreQueryImporter qryImporter= new CoreQueryImporter();
      qryImporter.importData(config);
      return this;
   }

  private static void generateInferred(TTImportConfig config) throws IOException, DataFormatException, OWLOntologyCreationException {

    for(String coreFile:coreEntities) {
      if (!coreFile.contains("-inferred.json")) {
        TTManager manager = new TTManager();
        Path path = ImportUtils.findFileForId(config.folder, coreFile);
        TTDocument document = manager.loadDocument(path.toFile());
        ReasonerPlus reasoner = new ReasonerPlus();
        TTDocument inferred = reasoner.generateInferred(document);
        manager = new TTManager();
        manager.setDocument(inferred);
        String inferredFile = path.toString().substring(0, path.toString().indexOf(".json")) + "-inferred.json";
        manager.saveDocument(new File(inferredFile));
      }

    }
  }

   private void importNamespaces() throws Exception {
      TTManager manager= new TTManager();
      manager.createDocument(IM.GRAPH_DISCOVERY.getIri());
      try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
          filer.fileDocument(manager.getDocument());
      }
   }

   public void fileDocument(TTDocument document) throws Exception {


   }

   /**
    * Loads the core ontology document, available as TTDocument for various purposes
    * @param inFolder root folder containing core ontology document
    * @return TTDocument containing Discovery ontology
    * @throws IOException in the event of an IO failure
    */
   public TTDocument loadFile(String inFolder) throws IOException {
      Path file = ImportUtils.findFileForId(inFolder, coreEntities[0]);
      TTManager manager= new TTManager();
      return manager.loadDocument(file.toFile());
   }


}
