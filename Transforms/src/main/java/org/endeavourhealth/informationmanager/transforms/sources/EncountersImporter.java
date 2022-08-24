package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.transforms.TTManager;

import java.nio.file.Path;

public class EncountersImporter implements TTImport {
   private static final String[] encounters ={ ".*\\\\DiscoveryNoneCore\\\\Encounters.json"};


   public void importData(TTImportConfig config) throws Exception {
      System.out.println("Importing Discovery entities");
      importNoneCoreFile(config);
   }

   private void importNoneCoreFile(TTImportConfig config) throws Exception {
      Path file = ImportUtils.findFileForId(config.getFolder(), encounters[0]);
      TTManager manager= new TTManager();
      TTDocument document = manager.loadDocument(file.toFile());
       try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
           filer.fileDocument(document);
       }
   }

   public void validateFiles(String inFolder){
      ImportUtils.validateFiles(inFolder,encounters);
   }


    @Override
    public void close() throws Exception {

    }
}
