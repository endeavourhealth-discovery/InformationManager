package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTDocumentFilerJDBC;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.TTImportConfig;
import org.endeavourhealth.informationmanager.common.transform.TTManager;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;

public class CoreImporter implements TTImport {
   private static final String[] coreEntities = {
     ".*\\SemanticWeb\\RDFOntology.json",
     ".*\\SemanticWeb\\RDFSOntology.json",
     ".*\\\\SemanticWeb\\\\OWLOntology.json",
     ".*\\\\SemanticWeb\\\\SHACLOntology.json",
     ".*\\\\DiscoveryCore\\\\CoreOntologyDocument.json"
   };



   public CoreImporter validateFiles(String inFolder){
      ImportUtils.validateFiles(inFolder,coreEntities);
      return this;
   }

   @Override
   public TTImport validateLookUps(Connection conn) {
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
      importNamespaces();
     System.out.println("Importing Core entities");
      for (String coreFile : coreEntities) {
        TTManager manager = new TTManager();
         Path path = ImportUtils.findFileForId(config.folder, coreFile);
         manager.loadDocument(path.toFile());
         TTDocument document= manager.getDocument();
         TTDocumentFiler filer = new TTDocumentFilerJDBC();
        filer.fileDocument(document);
      }
      return this;
   }

   private void importNamespaces() throws Exception {
      TTManager manager= new TTManager();
      manager.createDocument(IM.GRAPH_DISCOVERY.getIri());
      TTDocumentFiler filer= new TTDocumentFilerJDBC();
      filer.fileDocument(manager.getDocument());

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
      TTDocument document = manager.loadDocument(file.toFile());
      return document;

   }


   @Override
   public void close() throws Exception {

   }
}
