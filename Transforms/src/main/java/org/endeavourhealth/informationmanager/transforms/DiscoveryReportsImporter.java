package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.common.transform.TTManager;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;


public class DiscoveryReportsImporter implements TTImport {
   private static final String[] ReportsConcepts = {".*\\\\StatsReportsDocument.json"};



   public DiscoveryReportsImporter validateFiles(String inFolder){
      ImportUtils.validateFiles(inFolder, ReportsConcepts);
      return this;
   }

   @Override
   public TTImport validateLookUps(Connection conn) {
      return null;
   }

   /**
    * Imports the reports document
    * @param inFolder root folder containing the reports document
    * @return TTImport object builder pattern
    * @throws Exception invalid document
    */
   public TTImport importData(String inFolder) throws Exception {
      System.out.println("Importing Reports concepts");
      TTDocument document= loadFile(inFolder);
      TTDocumentFiler filer= new TTDocumentFiler(document.getGraph());
      filer.fileDocument(document);
      return this;
   }

   /**
    * Loads the core ontology document, available as TTDocument for various purposes
    * @param inFolder root folder containing the document
    * @return TTDocument containing Discovery ontology
    * @throws IOException in the event of an IO failure
    */
   public TTDocument loadFile(String inFolder) throws IOException {
      Path file = ImportUtils.findFileForId(inFolder, ReportsConcepts[0]);
      TTManager manager= new TTManager();
      TTDocument document = manager.loadDocument(file.toFile());
      return document;

   }


   @Override
   public void close() throws Exception {

   }
}
