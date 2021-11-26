package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTFilerFactory;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.TTImportConfig;
import org.endeavourhealth.imapi.transforms.TTManager;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;


public class DiscoveryReportsImporter implements TTImport {
   private static final String[] ReportsConcepts = {".*\\\\StatsReports-inferred.json"};



   public DiscoveryReportsImporter validateFiles(String inFolder){
      ImportUtils.validateFiles(inFolder, ReportsConcepts);
      return this;
   }



   /**
    * Imports the reports document
    * @param config import configuration
    * @return TTImport object builder pattern
    * @throws Exception invalid document
    */
   public TTImport importData(TTImportConfig config) throws Exception {
      System.out.println("Importing Reports concepts");
      TTDocument document= loadFile(config.folder);
       try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
           filer.fileDocument(document);
       }
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


}
