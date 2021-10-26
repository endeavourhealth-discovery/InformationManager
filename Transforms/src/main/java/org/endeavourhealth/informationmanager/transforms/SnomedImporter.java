package org.endeavourhealth.informationmanager.transforms;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.ECLToTT;
import org.endeavourhealth.imapi.transforms.OWLToTT;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDF;
import org.endeavourhealth.imapi.vocabulary.OWL;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.imapi.vocabulary.XSD;
import org.endeavourhealth.imapi.vocabulary.SHACL;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTDocumentFilerJDBC;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.TTImportConfig;

import java.io.*;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.*;
import java.util.zip.DataFormatException;

public class SnomedImporter implements TTImport {

   private Map<String, TTEntity> conceptMap;
   private final ECLToTT eclConverter = new ECLToTT();
   private TTDocument document;
   private Integer counter;


   public static final String[] concepts = {
       ".*\\\\SnomedCT_InternationalRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_Snapshot_INT_.*\\.txt",
       ".*\\\\SnomedCT_UKClinicalRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKCLSnapshot_.*\\.txt",
       ".*\\\\SnomedCT_UKDrugRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKDGSnapshot_.*\\.txt",
     ".*\\\\SnomedCT_UKEditionRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKEDSnapshot_.*\\.txt",
     ".*\\\\SnomedCT_UKClinicalRefsetsRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_Concept_UKCRSnapshot_.*\\.txt"
   };
   public static final String[] refsets= {	".*\\\\SnomedCT_UKClinicalRefsetsRF2_PRODUCTION_.*\\\\Snapshot\\\\Refset\\\\Content\\\\der2_Refset_SimpleUKCRSnapshot_.*\\.txt"};

   public static final String[] descriptions = {

       ".*\\\\SnomedCT_InternationalRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_Snapshot-en_INT_.*\\.txt",
       ".*\\\\SnomedCT_UKClinicalRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKCLSnapshot-en_.*\\.txt",
       ".*\\\\SnomedCT_UKDrugRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKDGSnapshot-en_.*\\.txt",
     ".*\\\\SnomedCT_UKEditionRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKEDSnapshot-en_.*\\.txt",
     ".*\\\\SnomedCT_UKClinicalRefsetsRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_Description_UKCRSnapshot-en_.*\\.txt"
   };


   public static final String[] relationships = {
       ".*\\\\SnomedCT_InternationalRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_Snapshot_INT_.*\\.txt",
       ".*\\\\SnomedCT_UKClinicalRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKCLSnapshot_.*\\.txt",
       ".*\\\\SnomedCT_UKDrugRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKDGSnapshot_.*\\.txt",
     ".*\\\\SnomedCT_UKEditionRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKEDSnapshot_.*\\.txt",
     ".*\\\\SnomedCT_UKClinicalRefsetsRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_Relationship_UKCRSnapshot_.*\\.txt"
   };

   public static final String[] substitutions = {
       ".*\\\\SnomedCT_UKClinicalRF2_PRODUCTION_.*\\\\Resources\\\\HistorySubstitutionTable\\\\xres2_HistorySubstitutionTable_.*\\.txt",
   };

   public static final String[] attributeRanges = {
       ".*\\\\SnomedCT_InternationalRF2_PRODUCTION_.*\\\\Snapshot\\\\Refset\\\\Metadata\\\\der2_ssccRefset_MRCMAttributeRangeSnapshot_INT_.*\\.txt",
   };

   public static final String[] attributeDomains = {
       ".*\\\\SnomedCT_InternationalRF2_PRODUCTION_.*\\\\Snapshot\\\\Refset\\\\Metadata\\\\der2_cissccRefset_MRCMAttributeDomainSnapshot_INT_.*\\.txt",
   };
   public static final String[] statedAxioms = {
       ".*\\\\SnomedCT_InternationalRF2_PRODUCTION_.*\\\\Snapshot\\\\Terminology\\\\sct2_sRefset_OWLExpressionSnapshot_INT_.*\\.txt"
   };

   public static final String[] importList = {"991181000000109"};



   public static final String FULLY_SPECIFIED = "900000000000003001";
   public static final String DEFINED= "900000000000073002";
   public static final String IS_A = "116680003";
   public static final String SN = "sn:";
   public static final String ALL_CONTENT = "723596005";
   public static final String ACTIVE = "1";
   public static final String REPLACED_BY = "370124000";

   //======================PUBLIC METHODS============================

   /**
    * Loads a multi country RF2 release package into a Discovery ontology will process international followed by uk clinical
    * followed by uk drug. Loads MRCM models also. Does not load reference sets.
    *
    * @param config import configuration
    * @throws Exception thrown from document filer
    */

   @Override
   public TTImport importData(TTImportConfig config) throws Exception {
      validateFiles(config.folder);
      conceptMap = new HashMap<>();
      TTManager dmanager= new TTManager();

      document= dmanager.createDocument(IM.GRAPH_SNOMED.getIri());

      importConceptFiles(config.folder);
      importDescriptionFiles(config.folder);
      removeQualifiers(document);
      importMRCMRangeFiles(config.folder);
      importRefsetFiles(config.folder);
      importMRCMDomainFiles(config.folder);
     // importStatedFiles(config.folder); No longer bothers with OWL axioms;
      importRelationshipFiles(config.folder);
      setRefSetRoot();
      importSubstitution(config.folder);
      conceptMap.clear();

      TTDocumentFiler filer = new TTDocumentFilerJDBC();
      filer.fileDocument(document);
      return this;
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
      TTEntity root= conceptMap.get("900000000000455006");
      root.set(IM.IS_CONTAINED_IN,new TTArray().add(TTIriRef.iri(IM.NAMESPACE+"QueryConceptSets")));
   }

   private void importSubstitution(String path) throws IOException {
      int i = 0;
      counter=0;
      for (String relationshipFile : substitutions) {
         Path file = ImportUtils.findFilesForId(path, relationshipFile).get(0);
            System.out.println("Processing substitutions in " + file.getFileName().toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
               reader.readLine(); // Skip header
               String line = reader.readLine();
               while (line != null && !line.isEmpty()) {
                  String[] fields = line.split("\t");
                  String old= fields[0];
                  String replacedBy = fields[2];
                  TTEntity c = conceptMap.get(old);
                  if (c==null){
                   c= new TTEntity().setIri(SN+old);
                   document.addEntity(c);
                   c.addType(IM.CONCEPT);
                   c.setStatus(IM.INACTIVE);
                   c.setCode(old);
                  }
                  addRelationship(c,0,REPLACED_BY,replacedBy);

                  line = reader.readLine();
               }
            }

      }
      System.out.println("Imported " + i + " relationships");
      System.out.println("isas added "+ counter);
   }

   private boolean conceptNeeded(String conceptFile, String conceptId){
      if (conceptFile.contains("Refset")) {
         return Arrays.asList(importList).contains(conceptId);
      }
      return true;
   }



   //=================private methods========================

   private void importConceptFiles(String path) throws IOException {
      int i = 0;
      for (String conceptFile : concepts) {
         Path file = ImportUtils.findFilesForId(path, conceptFile).get(0);
            System.out.println("Processing concepts in " + file.getFileName().toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
               reader.readLine();    // Skip header
               String line = reader.readLine();
               while (line != null && !line.isEmpty()) {
                  String[] fields = line.split("\t");
                  if (conceptNeeded(conceptFile,fields[0])) {
                     if (!conceptMap.containsKey(fields[0])) {
                        TTEntity c = new TTEntity();
                        c.setIri(SN + fields[0]);
                        c.setCode(fields[0]);
                        if (conceptFile.contains("Refset"))
                           c.addType(IM.CONCEPT_SET);
                        else
                           c.addType(IM.CONCEPT);
                        if (fields[4].equals(DEFINED))
                           c.set(IM.DEFINITIONAL_STATUS,IM.SUFFICIENTLY_DEFINED);
                        c.setStatus(ACTIVE.equals(fields[2]) ? IM.ACTIVE : IM.INACTIVE);
                        if (fields[0].equals("138875005")) { // snomed root
                            c.set(IM.IS_A, new TTArray().add(TTIriRef.iri(IM.NAMESPACE + "894281000252100")));
                        }
                        document.addEntity(c);
                        conceptMap.put(fields[0], c);
                     }
                  }
                  i++;
                  line = reader.readLine();
               }
         }
      }
      System.out.println("Imported " + i + " concepts");
   }

   private void importRefsetFiles(String path) throws IOException {
      int i = 0;
      for (String refsetFile : refsets) {
         List<Path> paths = ImportUtils.findFilesForId(path, refsetFile);
         Path file = paths.get(0);
         System.out.println("Processing refsets in " + file.getFileName().toString());
         try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            reader.readLine();    // Skip header
            String line = reader.readLine();
            while (line != null && !line.isEmpty()) {
               String[] fields = line.split("\t");
               TTEntity c= conceptMap.get(fields[4]);
               if (c!=null) {
                  if (c.get(IM.MEMBERS)==null)
                     c.set(IM.MEMBERS,new TTArray());
                  c.get(IM.MEMBERS).asArray().add(TTIriRef.iri(SNOMED.NAMESPACE + fields[5]));

               }
               i++;
               line = reader.readLine();
            }

         }

      }
      System.out.println("Imported " + i + " refset");
   }
   private void importDescriptionFiles(String path) throws IOException {
      int i = 0;
      for (String descriptionFile : descriptions) {

         Path file = ImportUtils.findFilesForId(path, descriptionFile).get(0);

            System.out.println("Processing  descriptions in " + file.getFileName().toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
               reader.readLine(); // Skip header
               String line = reader.readLine();
               while (line != null && !line.isEmpty()) {
                  String[] fields = line.split("\t");
                // if (fields[4].equals("900000000000455006"))
                  // System.out.println(fields[7]);
                  TTEntity c = conceptMap.get(fields[4]);
                  if (c!=null) {
                     if (fields[7].contains("(attribute)")) {
                        c.addType(RDF.PROPERTY);
                     }
                     if (ACTIVE.equals(fields[2])) {
                        if (FULLY_SPECIFIED.equals(fields[6])|c.getName()==null) {
                              c.setName(fields[7]);
                        }
                        TTManager.addTermCode(c, fields[7],fields[0]);
                     }
                  }
                  i++;
                  line = reader.readLine();
               }
            }

      }
      System.out.println("Imported " + i + " descriptions");
   }
   private void importStatedFiles(String path) throws IOException {
      int i = 0;
      OWLToTT owlConverter= new OWLToTT();
      TTContext statedContext= new TTContext();
      statedContext.add(SNOMED.NAMESPACE,"");
      statedContext.add(IM.NAMESPACE,"im");
      for (String relationshipFile : statedAxioms) {
         Path file = ImportUtils.findFilesForId(path, relationshipFile).get(0);
            System.out.println("Processing owl expressions in " + file.getFileName().toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
               reader.readLine(); // Skip header
               String line = reader.readLine();
               while (line != null && !line.isEmpty()) {
                  String[] fields = line.split("\t");
                  TTEntity c = conceptMap.get(fields[5]);
                  String axiom = fields[6];
                  if (!axiom.startsWith("Prefix"))
                     if (ACTIVE.equals(fields[2]))
                        if (!axiom.startsWith("Ontology"))
                           try {
                              //System.out.println(c.getIri());
                              axiom=axiom.replace(":609096000","im:roleGroup");
                              owlConverter.convertAxiom(c, axiom, statedContext);
                           } catch (Exception e){
                              System.err.println(Arrays.toString(e.getStackTrace()));
                              throw new IOException("owl parser error");
                           }
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

   private void importMRCMDomainFiles(String path) throws IOException {
      int i = 0;

      //gets attribute domain files (usually only 1)
      for (String domainFile : attributeDomains) {
         Path file = ImportUtils.findFilesForId(path, domainFile).get(0);
         System.out.println("Processing property domains in " + file.getFileName().toString());
         try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            reader.readLine();    // Skip header
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
         Path file = ImportUtils.findFilesForId(path, rangeFile).get(0);
         System.out.println("Processing property ranges in " + file.getFileName().toString());
         try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            reader.readLine();    // Skip header
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
      TTValue expression= eclConverter.getClassExpression(ecl);
      if (expression.isIriRef())
         op.addObject(RDFS.RANGE,expression);
      else if (expression.isNode()){
         if (expression.asNode().get(SHACL.OR)!=null){
            for (TTValue or:expression.asNode().get(SHACL.OR).asArray().getElements()){
               if (or.isIriRef())
                  op.addObject(RDFS.RANGE,or);
              //Code level range not supported
            }
         } else
            throw new DataFormatException("Unrecognised juntion type in MRCM range files");

      } else
         throw new DataFormatException("Snomed importer does not support intersections in the MRCM range file");

   }


   private void addSnomedPropertyDomain(TTEntity op, String domain) {
      //Assumes all properties may or may nor in a group
      //therefore groups are not modelled in this version
      if (op.get(RDFS.DOMAIN)==null)
         op.set(RDFS.DOMAIN,new TTArray());
      op.get(RDFS.DOMAIN).asArray().add(TTIriRef.iri(SN+ domain));
   }

   private void importRelationshipFiles(String path) throws IOException {
      int i = 0;
      counter=0;
      for (String relationshipFile : relationships) {
         Path file = ImportUtils.findFilesForId(path, relationshipFile).get(0);
            System.out.println("Processing relationships in " + file.getFileName().toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
               reader.readLine(); // Skip header
               String line = reader.readLine();
               while (line != null && !line.isEmpty()) {
                  String[] fields = line.split("\t");
                  if (fields[4].equals("158743007")) {
                     System.out.println(line);
                  }
                  TTEntity c = conceptMap.get(fields[4]);
                  if (c!=null) {
                     int group = Integer.parseInt(fields[6]);
                     String relationship = fields[7];
                     String target = fields[5];
                     if (target.equals("900000000000455006"))
                        System.out.println(c.getName()+" "+fields[4]+" is a ref set");

                     if (conceptMap.get(target) == null) {
                        System.err.println("Missing target entity in relationship" + target);
                     }
                     if (ACTIVE.equals(fields[2]) | (relationship.equals(REPLACED_BY))) {
                        addRelationship(c, group, relationship, target);
                     }
                  }
                  i++;
                  line = reader.readLine();
               }
            }

      }
      System.out.println("Imported " + i + " relationships");
      System.out.println("isas added "+ counter);
   }

   private void addIsa(TTEntity entity,String parent){
      if (entity.get(IM.IS_A)==null) {
         TTArray isas = new TTArray();
         entity.set(IM.IS_A, isas);
      }
      TTArray isas= entity.get(IM.IS_A).asArray();
      isas.add(TTIriRef.iri(SN+ parent));
      counter++;

   }

   private void addRelationship(TTEntity c, Integer group, String relationship, String target) {
      if (relationship.equals(IS_A)) {
         addIsa(c,target);

      } else if (relationship.equals(REPLACED_BY)){
         c.addObject(SNOMED.REPLACED_BY,TTIriRef.iri(SNOMED.NAMESPACE+target));
      }
      else {
         TTNode roleGroup = getRoleGroup(c, group);
         roleGroup.set(TTIriRef.iri(SN+relationship),TTIriRef.iri(SN+target));
      }
   }

   private TTNode getRoleGroup(TTEntity c, Integer groupNumber) {
      if (groupNumber==0)
         return c;
      if (c.get(IM.ROLE_GROUP)==null){
         TTArray roleGroups= new TTArray();
         c.set(IM.ROLE_GROUP,roleGroups);
      }
      TTArray groups=c.get(IM.ROLE_GROUP).asArray();
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

   public SnomedImporter validateFiles(String inFolder){
      ImportUtils.validateFiles(inFolder,concepts, descriptions,
          relationships, refsets, attributeRanges, attributeDomains,substitutions);
      return this;

   }

   @Override
   public TTImport validateLookUps(Connection conn) {
      return this;
   }



   @Override
   public void close() throws Exception {
      if (conceptMap!=null)
         conceptMap.clear();


   }
}
