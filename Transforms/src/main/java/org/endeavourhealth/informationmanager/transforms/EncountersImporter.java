package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.informationmanager.*;
import org.endeavourhealth.imapi.transforms.TTManager;

import java.nio.file.Path;

public class EncountersImporter implements TTImport {
   private static final String[] encounters ={ ".*\\\\DiscoveryNoneCore\\\\Encounters.json"};


   public TTImport importData(TTImportConfig config) throws Exception {
      System.out.println("Importing Discovery entities");
      importNoneCoreFile(config);
      return this;
   }

   private void importNoneCoreFile(TTImportConfig config) throws Exception {
      Path file = ImportUtils.findFileForId(config.folder, encounters[0]);
      TTManager manager= new TTManager();
      TTDocument document = manager.loadDocument(file.toFile());
       try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
           filer.fileDocument(document);
       }
   }

   public EncountersImporter validateFiles(String inFolder){
      ImportUtils.validateFiles(inFolder,encounters);
      return this;
   }




}