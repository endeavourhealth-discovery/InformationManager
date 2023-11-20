package org.endeavourhealth.informationmanager.transforms.sources;

import org.apache.commons.lang3.StringUtils;
import org.eclipse.rdf4j.common.text.StringUtil;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.imq.Bool;
import org.endeavourhealth.imapi.model.imq.Match;
import org.endeavourhealth.imapi.model.imq.Query;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.informationmanager.common.ECLToIML;
import org.endeavourhealth.imapi.transforms.OWLToTT;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.DataFormatException;

public class SnomedImporter implements TTImport {

   private Map<String, TTEntity> conceptMap;
  private Map<String, TTEntity> refsetMap;
   private final ECLToIML eclConverter = new ECLToIML();
   private TTDocument document;
   private Integer counter;


   public static final String[] concepts = {
       ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_InternationalRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_Snapshot_INT_.*\\.txt",
       ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKCLSnapshot_.*\\.txt",
       ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKEditionRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKEDSnapshot_.*\\.txt",
       ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRefsetsRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKCRSnapshot_.*\\.txt",
       ".*\\\\PRIMARY\\\\.*\\\\SnomedCT_UKPrimaryCareRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKPCSnapshot_.*\\.txt",
       ".*\\\\DRUG\\\\.*\\\\SnomedCT_UKDrugRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKDGSnapshot_.*\\.txt",
     ".*\\\\DRUG\\\\.*\\\\SnomedCT_UKEditionRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKEDSnapshot_.*\\.txt"
   };

   public static final String[] refsets= {
     ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRefsetsRF2_.*\\\\Snapshot\\\\Refset\\\\Content\\\\der2_Refset_SimpleUKCRSnapshot_.*\\.txt",
     ".*\\\\PRIMARY\\\\.*\\\\SnomedCT_UKPrimaryCareRF2_.*\\\\Snapshot\\\\Refset\\\\Content\\\\der2_Refset_SimpleUKPCSnapshot_.*\\.txt"
     };

   public static final String[] vmp= {
     ".*\\\\DMD\\\\.*\\\\f_vmp_VmpType.csv"
   };

  public static final String[] amp= {
    ".*\\\\DMD\\\\.*\\\\.*\\\\f_amp_AmpType.csv"
  };



  public static final String[] qofClusters= {
    ".*_PCD_Refset_Content.txt"};

   public static final String[] descriptions = {
       ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_InternationalRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_Snapshot-en_INT_.*\\.txt",
       ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKCLSnapshot-en_.*\\.txt",
       ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKEditionRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKEDSnapshot-en_.*\\.txt",
       ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRefsetsRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKCRSnapshot-en_.*\\.txt",
       ".*\\\\PRIMARY\\\\.*\\\\SnomedCT_UKPrimaryCareRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKPCSnapshot-en_.*\\.txt",
       ".*\\\\DRUG\\\\.*\\\\SnomedCT_UKDrugRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKDGSnapshot-en_.*\\.txt",
     ".*\\\\DRUG\\\\.*\\\\SnomedCT_UKEditionRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKEDSnapshot-en_.*\\.txt"
   };


   public static final String[] relationships = {
       ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_InternationalRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_Snapshot_INT_.*\\.txt",
       ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKCLSnapshot_.*\\.txt",
       ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKEditionRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKEDSnapshot_.*\\.txt",
       ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRefsetsRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKCRSnapshot_.*\\.txt",
       ".*\\\\PRIMARY\\\\.*\\\\SnomedCT_UKPrimaryCareRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKPCSnapshot_.*\\.txt",
       ".*\\\\DRUG\\\\.*\\\\SnomedCT_UKDrugRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKDGSnapshot_.*\\.txt",
     ".*\\\\DRUG\\\\.*\\\\SnomedCT_UKEditionRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKEDSnapshot_.*\\.txt"
   };

   public static final String[] substitutions = {
       ".*\\\\HISTORY\\\\.*\\\\SnomedCT_UKClinicalRF2_.*\\\\Resources\\\\QueryTable\\\\xres2_SNOMEDQueryTable_.*\\.txt",
   };

   public static final String[] attributeRanges = {
       ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_InternationalRF2_.*\\\\Snapshot\\\\Refset\\\\Metadata\\\\der2_ssccRefset_MRCMAttributeRangeSnapshot_INT_.*\\.txt",
   };

   public static final String[] attributeDomains = {
       ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_InternationalRF2_.*\\\\Snapshot\\\\Refset\\\\Metadata\\\\der2_cissccRefset_MRCMAttributeDomainSnapshot_INT_.*\\.txt",
   };
   public static final String[] statedAxioms = {
       ".*\\\\CLINICAL\\\\.*\\\\SnomedCT_InternationalRF2_.*\\\\Snapshot\\\\Terminology\\\\sct2_sRefset_OWLExpressionSnapshot_INT_.*\\.txt"
   };

   public static final String[] importList = {"991181000000109"};



   public static final String FULLY_SPECIFIED = "900000000000003001";
   public static final String DEFINED= "900000000000073002";
   public static final String IS_A = "116680003";
   public static final String SN = "http://snomed.info/sct#";
   public static final String ALL_CONTENT = "723596005";
   public static final String ACTIVE = "1";
   public static final String REPLACED_BY = "370124000";
   public static final String SNOMED_ATTRIBUTE = "sn:106237007";

   //======================PUBLIC METHODS============================

   /**
    * Loads a multi country RF2 release package into a Discovery ontology will process international followed by uk clinical
    * followed by uk drug. Loads MRCM models also. Does not load reference sets.
    *
    * @param config import configuration
    * @throws Exception thrown from document filer
    */

   @Override
   public void importData(TTImportConfig config) throws Exception {
      validateFiles(config.getFolder());
      conceptMap = new HashMap<>();
      try (TTManager dmanager= new TTManager()) {

          document = dmanager.createDocument(SNOMED.GRAPH_SNOMED.getIri());
          document.addEntity(dmanager.createGraph(SNOMED.GRAPH_SNOMED.getIri(), "Snomed-CT code scheme and graph",
              "An international or UK Snomed code scheme and graph. This does not include supplier specfic, local, or Discovery namespace extensions"));

          importConceptFiles(config.getFolder());
          importDescriptionFiles(config.getFolder());
          // removeQualifiers(document);
          importMRCMRangeFiles(config.getFolder());
          importMRCMDomainFiles(config.getFolder());
          // importStatedFiles(config.folder); No longer bothers with OWL axioms;
          importRelationshipFiles(config.getFolder());
          importSubstitution(config.getFolder());
          importVmp(config.getFolder());

          addSpecials(document);
          try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
              filer.fileDocument(document);
          }

          document = dmanager.createDocument(SNOMED.GRAPH_SNOMED.getIri());
          setRefSetRoot();
          importRefsetFiles(config.getFolder());
          importQof(config.getFolder());

          conceptMap.clear();
          try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
              filer.fileDocument(document);
          }
      }
   }

  private void addSpecials(TTDocument document) {
    //Snomed telephone is a device
    TTEntity telephone= new TTEntity()
      .setIri(SNOMED.NAMESPACE+"359993007")
      .setCrud(IM.ADD_QUADS)
        .setGraph(IM.CODE_SCHEME_DISCOVERY);
    telephone.addObject(RDFS.SUBCLASSOF, TTIriRef.iri(IM.NAMESPACE+"71000252102"));
    document.addEntity(telephone);
    TTEntity specific= conceptMap.get("10362801000001104");
    specific.addObject(RDFS.SUBCLASSOF,TTIriRef.iri(SNOMED.NAMESPACE+"127489000"));
  }

   private void removeQualifiers(TTDocument document) {
      System.out.println("Removing bracketed qualifiers");
      Set<String> duplicates= new HashSet<>();
      for (TTEntity entity:document.getEntities()){
         String name= entity.getName();
         if (name.contains(" (")){
            String[] parts= name.split(" \\(");
            String qualifier=" ("+parts[parts.length-1];
            int endIndex= name.indexOf(qualifier);
            String shortName= name.substring(0,endIndex);
            if (!duplicates.contains(shortName)) {
               entity.setName(shortName);
               entity.setDescription(name);
               duplicates.add(shortName);
            }
         }
      }
   }

   private void setRefSetRoot() {
      TTEntity root= new TTEntity().setIri(SNOMED.NAMESPACE+"900000000000455006");
      document.addEntity(root);
      conceptMap.put(root.getIri(),root);
      root.set(IM.IS_CONTAINED_IN,new TTArray().add(TTIriRef.iri(IM.NAMESPACE+"QueryConceptSets")));
   }

  private void importQof(String path) throws IOException {
    int i = 0;
    counter = 0;
    for (String clusterFile : qofClusters) {
      Path file =  ImportUtils.findFilesForId(path, clusterFile).get(0);
      System.out.println("Processing qof cluster synonyms in " + file.getFileName().toString());
      String qofFile= file.toFile().getName();
      String version= qofFile.split("_")[0];
      TTEntity clusters= new TTEntity()
        .setIri(IM.NAMESPACE+"QofClusters")
        .setName("QOF Code clusters")
        .setDescription("QOF code cluster reference sets issued on "+version)
        .addType(IM.FOLDER);
      clusters.addObject(IM.CONTENT_TYPE,IM.CONCEPT_SET);
      clusters
        .addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"QueryConceptSets"));
      document.addEntity(clusters);
      TTEntity clusterFolder= new TTEntity()
        .setIri(IM.NAMESPACE+"QofClusters"+version)
        .setName("QOF Code clusters - "+ version)
        .setDescription("QOF code cluster reference sets issued on "+version)
        .addType(IM.FOLDER);
        clusterFolder
        .addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"QofClusters"));
        clusterFolder.addObject(IM.CONTENT_TYPE,IM.CONCEPT_SET);
      document.addEntity(clusterFolder);

      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine(); // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          String[] fields = line.split("\t");
          String refset= fields[4];
          String clusterTerm=fields[0];
          String normalTerm= StringUtils.capitalize(fields[1]);
          normalTerm=normalTerm.split(" codes")[0]+" (primary care value set)";
          String imTerm = clusterTerm +" code cluster";
          TTEntity c= conceptMap.get(refset);
          c.set(IM.PREFERRED_NAME,TTLiteral.literal(normalTerm));
          if (!hasTermCode(c,imTerm)) {
            c.addObject(IM.HAS_TERM_CODE, new TTNode().set(RDFS.LABEL, TTLiteral.literal(imTerm)));
            c.addObject(IM.IS_CONTAINED_IN, TTIriRef.iri(clusterFolder.getIri()));
          }
          line= reader.readLine();
        }
      }
    }
  }
  private boolean hasTermCode(TTEntity entity,String term){
     if (entity.get(IM.HAS_TERM_CODE)==null)
       return false;
     for  (TTValue tc:entity.get(IM.HAS_TERM_CODE).getElements()) {
       if (tc.asNode().get(RDFS.LABEL).asLiteral().getValue()
         .equals(term))
         return true;
     }
     return false;

  }

   private void importSubstitution(String path) throws IOException {
      int i = 0;
      counter=0;
      for (String relationshipFile : substitutions) {
         Path file =  ImportUtils.findFilesForId(path, relationshipFile).get(0);
            System.out.println("Processing substitutions in " + file.getFileName().toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
               reader.readLine(); // NOSONAR - Skip header
               String line = reader.readLine();
               while (line != null && !line.isEmpty()) {
                  String[] fields = line.split("\t");
                  String supertype= fields[0];
                  if (!supertype.equals("138875005")) {
                    String subtype = fields[1];
                    String provenance = fields[2];
                    TTEntity c = conceptMap.get(subtype);
                    if (c == null) {
                      c = new TTEntity().setIri(SN + subtype);
                      document.addEntity(c);
                      c.addType(IM.CONCEPT);
                      c.setStatus(IM.INACTIVE);
                      c.setCode(subtype);
                      c.setScheme(SNOMED.GRAPH_SNOMED);
                    }
                    switch (provenance) {
                      case "0" -> c.addObject(IM.SUBSUMED_BY, TTIriRef.iri(SN + supertype));
                      case "1" -> c.addObject(IM.USUALLY_SUBSUMED_BY, TTIriRef.iri(SN + supertype));
                      case "2" -> c.addObject(IM.APPROXIMATE_SUBSUMED_BY, TTIriRef.iri(SN + supertype));
                      case "3" -> c.addObject(IM.MULTIPLE_SUBSUMED_BY, TTIriRef.iri(SN + supertype));
                    }
                  }
                  line = reader.readLine();
               }
            }

      }
      System.out.println("Imported " + i + " relationships");
      System.out.println("isas added "+ counter);
   }



   //=================private methods========================

  private void importVmp(String path) throws IOException {
    int i = 0;
    for (String conceptFile : vmp) {
      Path file =  ImportUtils.findFilesForId(path, conceptFile).get(0);
      System.out.println("Processing concepts in " + file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine();     // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          processVmpLine(line);

          i++;
          line = reader.readLine();
        }
      }
    }
    System.out.println("Imported " + i + " concepts");
  }

  private void processVmpLine(String line) {
    String[] fields = line.split("\\|");
    TTEntity c = conceptMap.get(fields[0]);
    if (c!=null) {
      c.set(IM.PREFERRED_NAME,TTLiteral.literal(fields[5]));
      if (!TTManager.termUsed(c,fields[5]))
        TTManager.addTermCode(c,fields[5],null);
    }
  }





   private void importConceptFiles(String path) throws IOException {
      int i = 0;
      for (String conceptFile : concepts) {
         Path file =  ImportUtils.findFilesForId(path, conceptFile).get(0);
            System.out.println("Processing concepts in " + file.getFileName().toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
               reader.readLine();     // NOSONAR - Skip header
               String line = reader.readLine();
               while (line != null && !line.isEmpty()) {
                   processConceptLine(conceptFile, line);

                   i++;
                  line = reader.readLine();
               }
         }
      }
      System.out.println("Imported " + i + " concepts");
   }

    private void processConceptLine(String conceptFile, String line) {
        String[] fields = line.split("\t");
        if (!conceptMap.containsKey(fields[0])) {
           TTEntity c = new TTEntity();
           c.setIri(SN + fields[0]);
           c.setCode(fields[0]);
           c.setScheme(SNOMED.GRAPH_SNOMED);
           if (conceptFile.contains("Refset") || conceptFile.contains("UKPrimaryCare"))
              c.addType(IM.CONCEPT_SET);
           else
              c.addType(IM.CONCEPT);
           if (fields[4].equals(DEFINED))
              c.set(IM.DEFINITIONAL_STATUS,IM.SUFFICIENTLY_DEFINED);
           c.setStatus(ACTIVE.equals(fields[2]) ? IM.ACTIVE : IM.INACTIVE);
           if (fields[0].equals("138875005")) { // snomed root
               c.set(IM.IS_CONTAINED_IN, new TTArray().add(TTIriRef.iri(IM.NAMESPACE + "HealthModelOntology")));
           }
           document.addEntity(c);
           conceptMap.put(fields[0], c);
        }
        else {
          TTEntity c= conceptMap.get(fields[0]);
          c.setStatus(ACTIVE.equals(fields[2]) ? IM.ACTIVE : IM.INACTIVE);

        }
    }

    private void importRefsetFiles(String path) throws IOException {
      int i = 0;
      refsetMap= new HashMap<>();
      for (String refsetFile : refsets) {
         List<Path> paths =  ImportUtils.findFilesForId(path, refsetFile);
         Path file = paths.get(0);
         System.out.println("Processing refsets in " + file.getFileName().toString());
         try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            reader.readLine();     // NOSONAR - Skip header
            String line = reader.readLine();
            while (line != null && !line.isEmpty()) {
                processRefsetLine(line);
                i++;
               line = reader.readLine();
            }

         }

      }
      System.out.println("Imported " + i + " refset");
   }

    private void processRefsetLine(String line) {
        String[] fields = line.split("\t");
        if (fields[2].equals(ACTIVE)) {
          TTEntity c = conceptMap.get(fields[4]);
         // if (fields[4].equals("999035921000230109"))
           // System.out.println(fields[4]);
          if (refsetMap.get(fields[4])==null) {
            refsetMap.put(fields[4], c);
            document.addEntity(c);
          }
         /* if (c == null) {
            c = new TTEntity().setIri(SNOMED.NAMESPACE + fields[4]);
            c.setType(new TTArray().add(IM.CONCEPT_SET));
            conceptMap.put(fields[4], c);
            document.addEntity(c);
          }
          */
          c.addObject(IM.HAS_MEMBER, TTIriRef.iri(SNOMED.NAMESPACE + fields[5]));
        }
    }

    private void importDescriptionFiles(String path) throws IOException {
      int i = 0;
      for (String descriptionFile : descriptions) {

         Path file =  ImportUtils.findFilesForId(path, descriptionFile).get(0);

            System.out.println("Processing  descriptions in " + file.getFileName().toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
               reader.readLine();  // NOSONAR - Skip header
               String line = reader.readLine();
               while (line != null && !line.isEmpty()) {
                   processDescriptionLine(line);
                   i++;
                  line = reader.readLine();
               }
            }

      }
      System.out.println("Imported " + i + " descriptions");
   }

    private void processDescriptionLine(String line) {
        String[] fields = line.split("\t");
        // if (fields[4].equals("900000000000455006"))
        // System.out.println(fields[7]);
        TTEntity c = conceptMap.get(fields[4]);
        String term=fields[7];

        if (c!=null) {
          if (term.contains("(attribute)")) {
            c.addType(RDF.PROPERTY);
          }
          if (FULLY_SPECIFIED.equals(fields[6]) || c.getName() == null) {
            c.setName(fields[7]);
          }
          if (c.getStatus().equals(IM.ACTIVE)) {
            if (ACTIVE.equals(fields[2]))
              TTManager.addTermCode(c, term, fields[0], IM.ACTIVE);
            else
              TTManager.addTermCode(c, term, fields[0], IM.INACTIVE);
          }
          if (term.contains(" General practice data extraction - ")) {
            term = term.split(" General practice data extraction - ")[1];
            if (term.contains(" simple reference set")) {
              term = term.split(" simple reference set")[0];
              term = StringUtils.capitalize(term) + " (NHS GP value set)";
              c.set(IM.PREFERRED_NAME,TTLiteral.literal(term));
              TTManager.addTermCode(c, term, fields[0]);
            }
          }
        }
    }

    private void importStatedFiles(String path) throws IOException {
      int i = 0;
      OWLToTT owlConverter= new OWLToTT();
      TTContext statedContext= new TTContext();
      statedContext.add(SNOMED.NAMESPACE,"");
      statedContext.add(IM.NAMESPACE,"im");
      for (String relationshipFile : statedAxioms) {
         Path file =  ImportUtils.findFilesForId(path, relationshipFile).get(0);
            System.out.println("Processing owl expressions in " + file.getFileName().toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
               reader.readLine();  // NOSONAR - Skip header
               String line = reader.readLine();
               while (line != null && !line.isEmpty()) {
                   processStatedLine(owlConverter, statedContext, line);
                   i++;
                   line = reader.readLine();
               }
            } catch (Exception e){
               System.err.println(Arrays.toString(e.getStackTrace()));
               throw new IOException("stated file input problem");
            }

      }

      System.out.println("Imported " + i + " OWL Axioms");
   }

    private void processStatedLine(OWLToTT owlConverter, TTContext statedContext, String line) throws IOException {
        String[] fields = line.split("\t");
        TTEntity c = conceptMap.get(fields[5]);
        String axiom = fields[6];
        if (!axiom.startsWith("Prefix") && ACTIVE.equals(fields[2]) && !axiom.startsWith("Ontology"))
            try {
                //System.out.println(c.getIri());
                axiom = axiom.replace(":609096000", "im:roleGroup");
                owlConverter.convertAxiom(c, axiom, statedContext);
            } catch (Exception e) {
                System.err.println(Arrays.toString(e.getStackTrace()));
                throw new IOException("owl parser error");
            }
    }

    private void importMRCMDomainFiles(String path) throws IOException {
      int i = 0;

      //gets attribute domain files (usually only 1)
      for (String domainFile : attributeDomains) {
         Path file =  ImportUtils.findFilesForId(path, domainFile).get(0);
         System.out.println("Processing property domains in " + file.getFileName().toString());
         try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            reader.readLine();     // NOSONAR - Skip header
            String line = reader.readLine();
            while (line != null && !line.isEmpty()) {
               String[] fields = line.split("\t");
               //Only process axioms relating to all snomed authoring
               if (fields[11].equals(ALL_CONTENT)) {
                  TTEntity op = conceptMap.get(fields[5]);
                  addSnomedPropertyDomain(op, fields[6]);
               }
               i++;
               line = reader.readLine();
            }
         }
      }
      System.out.println("Imported " + i + " property domain axioms");
   }

   private void importMRCMRangeFiles(String path) throws IOException,DataFormatException {
      int i = 0;
      //gets attribute range files (usually only 1)
      for (String rangeFile : attributeRanges) {
         Path file =  ImportUtils.findFilesForId(path, rangeFile).get(0);
         System.out.println("Processing property ranges in " + file.getFileName().toString());
         try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            reader.readLine();     // NOSONAR - Skip header
            String line = reader.readLine();
            while (line != null && !line.isEmpty()) {
               String[] fields = line.split("\t");
               if (fields[2].equals("1")) {
                  TTEntity op = conceptMap.get(fields[5]);
                  addSnomedPropertyRange(op, fields[6]);
               }
               i++;
               line = reader.readLine();
            }
         }
      }
      System.out.println("Imported " + i + " property range axioms");
   }

   private void addSnomedPropertyRange(TTEntity op, String ecl) throws DataFormatException {
       if(ecl.matches("^[a-zA-Z].*")){
           return;
       }
      Query expression= eclConverter.getQueryFromECL(ecl);
       for (Match match:expression.getMatch()){
         if (match.getInstanceOf()!=null) {
           op.addObject(RDFS.RANGE, TTIriRef.iri(match.getInstanceOf().getIri()));
         }
         else {
           if (match.getBool().equals(Bool.or)){
             for (Match or:match.getMatch()){
               op.addObject(RDFS.RANGE, TTIriRef.iri(or.getInstanceOf().getIri()));
             }
           }
           else
            throw new DataFormatException("ecl of this kind is not supported for ranges");
         }
       }
   }


   private void addSnomedPropertyDomain(TTEntity op, String domain) {
      //Assumes all properties may or may nor in a group
      //therefore groups are not modelled in this version
      if (op.get(RDFS.DOMAIN)==null)
         op.set(RDFS.DOMAIN,new TTArray());
      op.get(RDFS.DOMAIN).add(TTIriRef.iri(SN+ domain));
   }

   private void importRelationshipFiles(String path) throws IOException {
      int i = 0;
      counter=0;
      for (String relationshipFile : relationships) {
         Path file =  ImportUtils.findFilesForId(path, relationshipFile).get(0);
            System.out.println("Processing relationships in " + file.getFileName().toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
               reader.readLine();  // NOSONAR - Skip header
               String line = reader.readLine();
               while (line != null && !line.isEmpty()) {
                   processRelationshipLine(line);
                   i++;
                  line = reader.readLine();
               }
            }

      }
      System.out.println("Imported " + i + " relationships");
      System.out.println("isas added "+ counter);
   }

    private void processRelationshipLine(String line) {
        String[] fields = line.split("\t");
      //  if (fields[4].equals("158743007")) {
        //   System.out.println(line);
        //}
        TTEntity c = conceptMap.get(fields[4]);
        if (c!=null) {
           int group = Integer.parseInt(fields[6]);
           String relationship = fields[7];
           String target = fields[5];
          // if (target.equals("900000000000455006"))
            //  System.out.println(c.getName()+" "+fields[4]+" is a ref set");

           if (conceptMap.get(target) == null) {
              System.err.println("Missing target entity in relationship" + target);
           }
           if (ACTIVE.equals(fields[2]) || (relationship.equals(REPLACED_BY))) {
              addRelationship(c, group, relationship, target);
           }
        }
    }

    private void addIsa(TTEntity entity,String parent){
      TTIriRef isa=  RDFS.SUBCLASSOF;
      if (entity.get(isa)==null) {
         TTArray isas = new TTArray();
         entity.set(isa, isas);
      }
      TTArray isas= entity.get(isa);
      isas.add(TTIriRef.iri(SN+ parent));
      counter++;

   }

   private void addRelationship(TTEntity c, Integer group, String relationship, String target) {
     if (relationship.equals(IS_A)) {
       addIsa(c,target);
       if (c.getIri().equals(SNOMED_ATTRIBUTE))
         c.addObject(RDFS.SUBCLASSOF,RDF.PROPERTY);
     }
     else {
       TTNode roleGroup = getRoleGroup(c, group);
       roleGroup.set(TTIriRef.iri(SN+relationship),TTIriRef.iri(SN+target));
     }
   }

   private TTNode getRoleGroup(TTEntity c, Integer groupNumber) {
     // if (groupNumber==0)
       //  return c;
      if (c.get(IM.ROLE_GROUP)==null){
         TTArray roleGroups= new TTArray();
         c.set(IM.ROLE_GROUP,roleGroups);
      }
      TTArray groups=c.get(IM.ROLE_GROUP);
      for (TTValue group:groups.getElements()) {
         if (Integer.parseInt(group.asNode().get(IM.GROUP_NUMBER).asLiteral().getValue()) == groupNumber)
            return group.asNode();
      }
      TTNode newGroup= new TTNode();
      TTLiteral groupCount= TTLiteral.literal(groupNumber.toString());
      groupCount.setType(XSD.INTEGER);
      newGroup.set(IM.GROUP_NUMBER,groupCount);
      groups.add(newGroup);
      return newGroup;
   }

   public void validateFiles(String inFolder){
       ImportUtils.validateFiles(inFolder,concepts, descriptions,
          relationships, refsets, attributeRanges, attributeDomains,substitutions,qofClusters,vmp);
   }

    @Override
    public void close() throws Exception {
       if (conceptMap != null)
            conceptMap.clear();
    }
}
