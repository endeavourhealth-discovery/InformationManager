package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.logic.reasoner.Reasoner;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.zip.DataFormatException;

public class CoreImporter implements TTImport {
   private static final String[] coreEntities = {
     ".*\\\\SemanticWeb\\\\RDFOntology.json",
     ".*\\\\SemanticWeb\\\\RDFSOntology.json",
     ".*\\\\SemanticWeb\\\\OWLOntology.json",
     ".*\\\\SemanticWeb\\\\SHACLOntology.json",
     ".*\\\\DiscoveryCore\\\\CoreOntology.json",
     ".*\\\\DiscoveryCore\\\\CoreOntology-more-inferred.json",
     ".*\\\\DiscoveryCore\\\\StatsReports.json",
     ".*\\\\DiscoveryCore\\\\QueryHelpers.json",
     ".*\\\\DiscoveryCore\\\\Sets.json"
   };

   private static final String INFERRED_SUFFIX = "-inferred.json";



   public void validateFiles(String inFolder){
      ImportUtils.validateFiles(inFolder,coreEntities);
   }


   /**
    * Imports the core ontology document
    * @param config import config
    * @return TTImport object builder pattern
    * @throws Exception invalid document
    */
   @Override
   public void importData(TTImportConfig config) throws Exception {
     System.out.println("Generating inferred ontologies...");
     generateInferred(config);
     importNamespaces();
     System.out.println("Importing Core entities");
      for (String coreFile : coreEntities) {
        if (!coreFile.contains(INFERRED_SUFFIX))
          coreFile = coreFile.substring(0, coreFile.indexOf(".json")) + INFERRED_SUFFIX;
        TTManager manager = new TTManager();
        Path path = ImportUtils.findFileForId(config.getFolder(), coreFile);
        manager.loadDocument(path.toFile());
        TTDocument document = manager.getDocument();
        System.out.println("Filing  " + document.getGraph().getIri() + " from " + coreFile);
          try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
          }
        }

   }



  private static void generateInferred(TTImportConfig config) throws IOException, DataFormatException, OWLOntologyCreationException {

    for(String coreFile:coreEntities) {
      if (!coreFile.contains(INFERRED_SUFFIX)) {
        TTManager manager = new TTManager();
        Path path = ImportUtils.findFileForId(config.getFolder(), coreFile);
        TTDocument document = manager.loadDocument(path.toFile());
        if(".*\\\\DiscoveryCore\\\\CoreOntology.json".equals(coreFile)) {
            codeToString(document);
        }
        System.out.println("Generating inferred document from "+ coreFile);
        Reasoner reasoner = new Reasoner();
        TTDocument inferred = reasoner.generateInferred(document);
        inferred= reasoner.inheritShapeProperties(inferred);
        manager = new TTManager();
        manager.setDocument(inferred);
        String inferredFile = path.toString().substring(0, path.toString().indexOf(".json")) + INFERRED_SUFFIX;
        manager.saveDocument(new File(inferredFile));

      }

    }
  }

  public static void codeToString(TTDocument document) {
      document.getEntities().forEach(e -> {
          if (e.getCode() != null) {
              String code = e.getCode().toString();
              e.setCode(code);
          }
      });
  }

   private void importNamespaces() throws Exception {
      TTManager manager= new TTManager();
      manager.createDocument(IM.GRAPH_DISCOVERY.getIri());
      try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
          filer.fileDocument(manager.getDocument());
      }
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

    @Override
    public void close() throws Exception {

    }
}
