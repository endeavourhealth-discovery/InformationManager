package org.endeavourhealth.informationmanager.common.transform;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.common.Logger;
import org.semanticweb.owlapi.model.OWLDocumentFormat;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

import java.io.*;
import java.util.*;
import java.util.zip.DataFormatException;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;
import static org.endeavourhealth.imapi.model.tripletree.TTLiteral.literal;

/**
 * Various utility functions to support triple tree entities and documents.
 * Create document creates a document with default common prefixes.
 */
public class TTManager {
   private Map<String, TTEntity> entityMap;
   private Map<String, TTEntity> nameMap;
   private TTDocument document;
   private TTContext context;
   // private List<TTPrefix> defaultPrefixes;
   // private Map<String,String> prefixMap;

   public TTManager() {
      createDefaultContext();
   }

   public TTManager(TTDocument document) {
      createDefaultContext();
      this.document = document;
   }

   public TTDocument createDocument(String graph) {
      createDefaultContext();
      document = new TTDocument();
      document.setContext(context);
      document.setGraph(TTIriRef.iri(graph));
      return document;
   }

   /**
    * Gets a entity from an iri or null if not found
    *
    * @param searchKey the iri or name of the entity you are looking for
    * @return entity, which may be a subtype that may be downcasted
    */
   public TTEntity getEntity(String searchKey) {
      if (entityMap == null)
         createIndex();
      TTEntity result = entityMap.get(searchKey);
      if (result != null)
         return result;
      else {
         if (searchKey.contains(":")) {
            result = entityMap.get(expand(searchKey));
            if (result != null)
               return result;
         }

         return nameMap.get(searchKey.toLowerCase());
      }
   }



   public TTContext createDefaultContext() {
      context = new TTContext();
      context.add(IM.NAMESPACE, "im");
      context.add(SNOMED.NAMESPACE, "sn");
      context.add(OWL.NAMESPACE, "owl");
      context.add(RDF.NAMESPACE, "rdf");
      context.add(RDFS.NAMESPACE, "rdfs");
      context.add(XSD.NAMESPACE, "xsd");
      context.add("http://endhealth.info/READ2#", "r2");
      context.add("http://endhealth.info/CTV3#", "ctv3");
      context.add("http://endhealth.info/ICD10#", "icd10");
      context.add("http://endhealth.info/OPCS4#", "opcs4");
      context.add("http://endhealth.info/EMIS#", "emis");
      context.add("http://endhealth.info/TPP#", "tpp");
      context.add("http://endhealth.info/Barts_Cerner#", "bc");
      context.add(SHACL.NAMESPACE, "sh");
      context.add("http://www.w3.org/ns/prov#", "prov");
      context.add("https://directory.spineservices.nhs.uk/STU3/CodeSystem/ODSAPI-OrganizationRole-1#", "orole");
      context.add("http://endhealth.info/VISION#", "vis");
      return context;
   }

   /**
    * Loads an information model document file in  JSON-LD/RDF syntax
    *
    * @param inputFile the file name to load
    * @return the IM triple tree document
    * @throws IOException covering file format exceptions and content exceptions of various kinds
    */
   public TTDocument loadDocument(File inputFile) throws IOException {
      ObjectMapper objectMapper = new ObjectMapper();
      document = objectMapper.readValue(inputFile, TTDocument.class);
      return document;
   }

   public TTDocument loadDocument(String json) throws IOException {
      ObjectMapper objectMapper = new ObjectMapper();
      document = objectMapper.readValue(json, TTDocument.class);
      return document;

   }


   /**
    * Saves an OWL ontology in functional syntax format
    *
    * @param manager    OWL ontology manager with at least one ontology
    * @param outputFile output fle name
    * @throws IOException in the event of an IO file creation failure
    */

   public void saveOWLOntology(OWLOntologyManager manager, File outputFile) throws IOException {
      FileWriter writer = new FileWriter(outputFile);
      manager.getOntologies().forEach(o -> {
         try {
            OWLDocumentFormat format = manager.getOntologyFormat(o);
            format.setAddMissingTypes(false);
            o.saveOntology(format, new FileOutputStream(outputFile));

         } catch (IOException | OWLOntologyStorageException e) {
            e.printStackTrace();
         }

      });
   }


   /**
    * Indexes the entities held in the manager's TTDocument document so they can be quicly retrieced via their IRI.
    */
   public void createIndex() {
      entityMap = new HashMap();
      nameMap = new HashMap();

      //Loops through the 3 main entity types and add them to the IRI map
      //Note that an IRI may be both a class and a property so both are added
      if (document.getEntities() != null)
         document.getEntities().forEach(p -> {
            entityMap.put(p.getIri(), p);
            if (p.getName() != null)
               nameMap.put(p.getName().toLowerCase(), p);
         });
   }

   /**
    * Expands a prefixed iri string to a full iri
    *
    * @param iri
    * @return
    */
   public String expand(String iri) {
      if (context == null)
         context = createDefaultContext();
      if (iri == null)
         return null;
      return context.expand(iri);
   }

   public TTDocument getDocument() {
      return document;
   }

   public TTManager setDocument(TTDocument document) {
      this.document = document;
      return this;
   }

   /**
    * Saves the Discovery ontology held by the manager
    *
    * @param outputFile file to save ontology to
    * @throws JsonProcessingException if deserialization fails
    */
   public void saveDocument(File outputFile) throws JsonProcessingException {
      if (document == null)
         throw new NullPointerException("Manager has no ontology document assigned");
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
      String json = objectMapper.writerWithDefaultPrettyPrinter().withAttribute(TTContext.OUTPUT_CONTEXT, true).writeValueAsString(document);
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
         writer.write(json);
      } catch (Exception e) {
         Logger.error("Unable to save ontology in JSON format: " + e.getMessage());
         e.printStackTrace();
      }

   }


   /**
    * Returns a string of JSON from a TTDocument instance
    *
    * @param document the TTDocument holding the ontology
    * @return the json serialization of the document
    * @throws JsonProcessingException
    */
   public String getJson(TTDocument document) throws JsonProcessingException {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
      String json = objectMapper.writerWithDefaultPrettyPrinter().withAttribute(TTContext.OUTPUT_CONTEXT, true).writeValueAsString(document);
      return json;
   }

   public TTDocument replaceIri(TTDocument document, TTIriRef from, TTIriRef to) {
      if (document.getEntities() != null) {
         for (TTEntity entity : document.getEntities()) {
            if (entity.getIri().equals(from.getIri()))
               entity.setIri(to.getIri());
            boolean replacedPredicate = true;
            while (replacedPredicate) {
               replacedPredicate = replaceNode(entity, from, to);
            }
         }
      }

      return document;

   }

   private boolean replaceNode(TTNode node, TTIriRef from, TTIriRef to) {
      boolean replaced = false;
      if (node.get(from) != null) {
         node.set(to, node.get(from));
         node.getPredicateMap().remove(from);
         return true;
      }
      if (node.getPredicateMap() != null) {
         HashMap<TTIriRef, TTValue> newPredicates = new HashMap<>();
         for (Map.Entry<TTIriRef, TTValue> entry : node.getPredicateMap().entrySet()) {
            TTIriRef predicate = entry.getKey();
            TTValue value = entry.getValue();
            if (value.isIriRef()) {
               if (value.asIriRef().equals(from))
                  newPredicates.put(entry.getKey(), to);
            } else if (value.isNode()) {
               if (replaceNode(value.asNode(), from, to))
                  return true;
            } else if (value.isList()) {
               List<TTValue> toRemove = new ArrayList<>();
               for (TTValue arrayValue : value.asArray().getElements()) {
                  if (arrayValue.isIriRef()) {
                     if (arrayValue.asIriRef().equals(from)) {
                        toRemove.add(arrayValue);
                     }
                  } else if (arrayValue.isNode()) {
                     replaced = replaceNode(arrayValue.asNode(), from, to);
                  }
               }
               if (!toRemove.isEmpty()) {
                  for (TTValue remove : toRemove) {
                     value.asArray().getElements().remove(remove);
                  }
                  value.asArray().add(to);
               }
            }
         }
         if (!newPredicates.isEmpty()) {
            for (Map.Entry<TTIriRef, TTValue> entry : newPredicates.entrySet()) {
               node.getPredicateMap().remove(entry.getKey());
               node.set(entry.getKey(), entry.getValue());
            }
         }
      }
      return false;
   }

   private TTArray replaceArray(TTArray array, TTIriRef from, TTIriRef to) {
      TTArray newArray = new TTArray();
      for (TTValue value : array.getElements()) {
         if (value.isIriRef()) {
            if (value.asIriRef().equals(from))
               newArray.add(to);
            else
               newArray.add(value);
         } else {
            newArray.add(value);
            if (value.isNode()) {
               replaceNode(value.asNode(), from, to);
            } else if (value.isList()) {
               newArray.add(replaceArray(value.asArray(), from, to));
            } else {
               newArray.add(value);
            }
         }
      }
      return newArray;
   }

   /**
    * Tests whether a entity is a descendant of an ancestor, entity test against iri
    * uses standard prefixes in this version
    *
    * @param descendant the descendant entity
    * @param ancestor   the ancestor IRI
    * @return true if found false if not a descendant
    */
   public boolean isA(TTEntity descendant, TTIriRef ancestor) {
      Set<TTIriRef> done = new HashSet<>();
      if (entityMap == null)
         createIndex();
      if (entityMap.get(ancestor) == null)
         throw new NoSuchElementException("ancestor not found in this module");
      return isA1(descendant, ancestor, done);
   }

   /**
    * tests isa relationship between two iris. Isa rerlationships must have previosuly been inferred.
    * This is not an entailment test using DL reasoning
    *
    * @param descendant the subtype that is being tested
    * @param ancestor   the supertype that is being tested against
    * @return true if descendent is a subtype of supertype
    */
   public boolean isA(TTIriRef descendant, TTIriRef ancestor) {
      if (descendant.equals(ancestor))
         return true;
      Set<TTIriRef> done = new HashSet<>();
      if (entityMap == null)
         createIndex();
      TTEntity descendantEntity = entityMap.get(descendant.getIri());
      if (descendantEntity == null)
         return false;
      if (entityMap.get(ancestor.getIri()) == null)
         return false;
      return isA1(descendantEntity, ancestor, done);
   }

   private boolean isA1(TTEntity descendant, TTIriRef ancestor, Set<TTIriRef> done) {
      if (TTIriRef.iri(descendant.getIri()).equals(ancestor))
         return true;
      boolean isa = false;
      if (descendant.get(IM.IS_A) != null)
         for (TTValue ref : descendant.get(IM.IS_A).asArray().getElements())
            if (ref.equals(ancestor))
               return true;
            else {
               TTIriRef parent = ref.asIriRef();
               if (!done.contains(parent)) {
                  done.add(parent);
                  TTEntity parentEntity = entityMap.get(parent.getIri());
                  if (parentEntity != null)
                     isa = isA1(parentEntity, ancestor, done);
                  if (isa)
                     return true;
               }
            }
      return false;
   }

   public static TTEntity addProperty(TTEntity entity,TTIriRef logicalConstraint,TTIriRef property,TTIriRef constraintComponent, TTIriRef valueType,Integer min, Integer max, String data){
      TTNode nodeShape= new TTNode();
      entity.addObject(logicalConstraint,nodeShape);
      nodeShape.set(SHACL.PATH,property);
      if (valueType!=null)
         nodeShape.set(constraintComponent,valueType);
      if (min!=null)
         nodeShape.set(SHACL.MINCOUNT,TTLiteral.literal(min));
      if (max!=null)
         nodeShape.set(SHACL.MAXCOUNT,TTLiteral.literal(max));
      if (data!=null)
         nodeShape.set(SHACL.HASVALUE,TTLiteral.literal(data));

      return entity;
   }

   public static void addMatch(TTEntity c, TTIriRef target) {
      TTValue maps = c.get(IM.MATCHED_TO);
      if (maps == null) {
         maps = new TTArray();
         c.set(IM.MATCHED_TO, maps);
      }
      maps.asArray().add(target);
   }

   public boolean isValidIri(String iri) {
      if (context == null)
         createDefaultContext();
      if (expand(iri) == null)
         return false;
      else
         return true;

   }

   public static TTEntity createInstance(TTIriRef iri,TTIriRef crud){
      TTEntity result= new TTEntity();
      result.setIri(iri.getIri());
      result.setCrud(crud);
      return result;
   }

   public static void addChildOf(TTEntity c, TTIriRef parent) {
      if (c.get(IM.IS_CHILD_OF) == null)
         c.set(IM.IS_CHILD_OF, new TTArray());
      c.get(IM.IS_CHILD_OF).asArray().add(parent);
   }

   public static void addSuperClass(TTEntity entity, TTIriRef andOr,TTValue superClass) {
      addESAxiom(entity,RDFS.SUBCLASSOF,andOr,superClass);

   }

   public static void addEquivalentClass(TTEntity entity, TTIriRef andOr, TTValue eqClass) {
      addESAxiom(entity,OWL.EQUIVALENTCLASS,andOr,eqClass);
   }

   private static void addESAxiom(TTEntity entity, TTIriRef axiom,
                                  TTIriRef andOr, TTValue newExpression) {
      if (entity.get(axiom) == null)
         entity.set(axiom, new TTArray());
      TTValue oldExpression;
      TTArray expressions = entity.get(axiom).asArray();
      if (expressions.getElements().size() > 0) {
         oldExpression = expressions.getElements().get(0);
         if (oldExpression.isIriRef()||oldExpression.isNode()) {
            TTNode intersection = new TTNode();
            intersection.set(andOr, new TTArray());
            intersection.get(andOr).asArray().add(oldExpression);
            intersection.get(andOr).asArray().add(newExpression);
            expressions.add(intersection);
         } else
            oldExpression.asNode().get(andOr).asArray().add(newExpression);
      } else
         expressions.add(newExpression);
      if (newExpression.isIriRef()){
         if (entity.get(IM.IS_A)==null)
            entity.set(IM.IS_A, new TTArray());
         entity.addObject(IM.IS_A,newExpression);
      }

   }

   public static void addSimpleMap(TTEntity c, TTIriRef assuranceLevel,String target, String targetTermCode,
                             TTIriRef matchType, Integer priority, String advice) {
      if (matchType==null)
         matchType= IM.MATCHED_TO;
      TTValue maps= c.get(IM.HAS_MAP);
      if (maps==null) {
         maps = new TTArray();
         c.set(IM.HAS_MAP, maps);
      }
      TTNode map= new TTNode();
      maps.asArray().add(map);
      map.set(iri(IM.NAMESPACE+"assuranceLevel"),assuranceLevel);
      TTNode mapNode= new TTNode();
      map.set(matchType,mapNode);
      mapNode.set(IM.MATCHED_TO,iri(target));
      if (targetTermCode!=null)
         if (!targetTermCode.equals(""))
            map.set(IM.MATCHED_TERM_CODE,literal(targetTermCode));
      if (advice!=null)
         mapNode.set(TTIriRef.iri(IM.NAMESPACE+ "mapAdvice"),TTLiteral.literal(advice));
      if (priority!=null)
         mapNode.set(TTIriRef.iri(IM.NAMESPACE+"mapPriority"),TTLiteral.literal(priority));
   }

   public static TTEntity createTermCode(TTIriRef iri,TTIriRef crud,
                                              String term,
                                              String code,
                                              TTIriRef scheme,
                                              String entityCode){
      TTEntity result= createInstance(iri,crud);
      addTermCode(result,term,code,scheme,entityCode);
      return result;
   }

   public static TTEntity addTermCode(TTEntity entity,
                                       String term,
                                       String code,
                                       TTIriRef scheme,
                                       String entityCode){
      TTNode termCode= new TTNode();
      termCode.set(IM.CODE,TTLiteral.literal(code));
      termCode.set(RDFS.LABEL,TTLiteral.literal(term));
      termCode.set(IM.HAS_SCHEME,scheme);
      if (entityCode!=null)
         termCode.set(IM.MATCHED_TERM_CODE,TTLiteral.literal(entityCode));
      entity.addObject(IM.HAS_TERM_CODE,termCode);
      return entity;
   }

   public TTContext getContext() {
      return context;
   }
}
