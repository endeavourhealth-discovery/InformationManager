package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.transforms.TTManager;

import java.io.IOException;
import java.nio.file.Path;


public class DiscoveryReportsImporter implements TTImport {
   private static final String[] ReportsConcepts = {".*\\\\StatsReports-inferred.json"};



   public void validateFiles(String inFolder){
      ImportUtils.validateFiles(inFolder, ReportsConcepts);
   }



   /**
    * Imports the reports document
    * @param config import configuration
    * @return TTImport object builder pattern
    * @throws Exception invalid document
    */
   public void importData(TTImportConfig config) throws Exception {
      System.out.println("Importing Reports concepts");
      TTDocument document= loadFile(config.getFolder());
       try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
           filer.fileDocument(document);
       }
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
