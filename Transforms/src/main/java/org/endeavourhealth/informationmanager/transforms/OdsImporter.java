package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTFilerFactory;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.TTImportConfig;

import java.nio.file.Path;
import java.sql.Connection;

public class OdsImporter implements TTImport {
   private static final String[] organisationFiles = {
     ".*\\\\ODS\\\\Organisation_Details_mapped.json"
   };

   public OdsImporter validateFiles(String inFolder){
      ImportUtils.validateFiles(inFolder, organisationFiles);
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
     System.out.println("Importing Organisation files");
      for (String orgFile : organisationFiles) {
        TTManager manager = new TTManager();
         Path path = ImportUtils.findFileForId(config.folder, orgFile);
         manager.loadDocument(path.toFile());
         TTDocument document= manager.getDocument();
          try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
              filer.fileDocument(document);
          }
      }
      return this;
   }

   @Override
   public void close() throws Exception {

   }
}
