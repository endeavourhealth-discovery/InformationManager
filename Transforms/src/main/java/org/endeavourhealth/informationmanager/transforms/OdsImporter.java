package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
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



   /**
    * Imports the core ontology document
    * @param config import config
    * @return TTImport object builder pattern
    * @throws Exception invalid document
    */
   @Override
   public TTImport importData(TTImportConfig config) throws Exception {
     System.out.println("Importing Organisation files");
     Boolean hasGraph=false;
      for (String orgFile : organisationFiles) {
        TTManager manager = new TTManager();
         Path path = ImportUtils.findFileForId(config.folder, orgFile);
         manager.loadDocument(path.toFile());
         TTDocument document= manager.getDocument();
         if (!hasGraph)
           document.addEntity(manager.createGraph(IM.GRAPH_ODS.getIri(),"ODS Organisational code scheme and graph",
           "Official ODS code scheme and graph"));
         hasGraph=true;
          try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
              filer.fileDocument(document);
          }
      }
      return this;
   }

}
