package org.endeavourhealth.informationmanager.common.transform;

import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.OWL;
import org.endeavourhealth.imapi.vocabulary.RDF;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.informationmanager.common.Logger;
import org.endeavourhealth.imapi.model.*;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.formats.FunctionalSyntaxDocumentFormat;
import org.semanticweb.owlapi.formats.PrefixDocumentFormat;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.util.DefaultPrefixManager;
import org.semanticweb.owlapi.vocab.OWL2Datatype;
import org.semanticweb.owlapi.vocab.OWLFacet;

import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

/**
 * Converts Discovery JSON Triple tree syntax document to OWL EL functional syntax using an OWL factory.
 * Note that this is a limited transform for the purposes of EL based inferrencing using a reasoner. DL axioms are ignored or converted
 * to EL similar structures
 * For example property ranges domains are ignored if present. Cardinality restrictions are converted to existential quantification.
 * Data type restrictions are ignored.
 * <p>Thus a transform back from the OWL EL version will not match the source unless the source is EL only.
 */
public class TTToOWLEL {
   private DefaultPrefixManager prefixManager;
   private OWLDataFactory dataFactory;
   private OWLOntologyManager manager;
   private Integer anon=0;
   private OWLOntology ontology;
   private TTEntity currentEntity;
   private OWLEntity placeHolder;
   private TTManager ttManager;
   private Set<String> declared;


   public TTToOWLEL() {
      manager = OWLManager.createOWLOntologyManager();
      dataFactory = manager.getOWLDataFactory();
      prefixManager = new DefaultPrefixManager();
      declared= new HashSet<>();
   }


   /**
    * Transforms an information model JSON-LD RDF ontology to an OWL ontology
    *
    * @param document  TTDocyment - the document to transform
    * @param dmanager  TTManager - The Discovery ontology manager
    * @return OWLOntology manager together with one ontology (optional) and a set of prefixes
    * @throws OWLOntologyCreationException if the owl ontology cannot be created
    * @throws DataFormatException if the owl ontology content is invalid
    */

   public OWLOntologyManager transform(TTDocument document,TTManager dmanager) throws DataFormatException, OWLOntologyCreationException {

      ttManager= dmanager;
      //if the dmanager is null create it
      if (dmanager==null) {
         ttManager = new TTManager();
         ttManager.setDocument(document);
      }

      //Create ontology
      ontology = manager.createOntology(IRI.create(document.getGraph().getIri()));

      processPrefixes(document.getPrefixes());
      processEntities(document.getEntities());
      return manager;
   }




   private void processImports(OWLOntology owlOntology, OWLDataFactory dataFactory,
                               OWLOntologyManager manager, Set<String> imports) {
      if (imports != null) {
         for (String importIri : imports) {
            OWLImportsDeclaration importDeclaration = dataFactory.getOWLImportsDeclaration(IRI.create(importIri));
            manager.applyChange(new AddImport(owlOntology, importDeclaration));
         }

      }
   }


   private void processPrefixes(List<TTPrefix> prefixes) {
      for (TTPrefix prefix: prefixes) {
         prefixManager.setPrefix(prefix.getPrefix()+":", prefix.getIri());
      }
      PrefixDocumentFormat ontologyFormat = new FunctionalSyntaxDocumentFormat();
      ontologyFormat.copyPrefixesFrom(prefixManager);
      manager.setOntologyFormat(ontology, ontologyFormat);
   }



   private void processEntities(List<TTEntity> entities) {
      if (entities == null || entities.size() == 0)
         return;
      int classno = 0;

      for (TTEntity entity : entities) {
         classno = classno + 1;
         currentEntity= entity;
         //System.out.println(entity.getIri());
         IRI iri = getIri(entity.getIri());
         addDeclaration(entity);
         Map<TTIriRef, TTValue> predicates = entity.getPredicateMap();
         for (Map.Entry<TTIriRef, TTValue> entry : predicates.entrySet()) {
            if (entry.getKey().equals(RDFS.SUBCLASSOF))
               addSubClassOf(iri, entry.getValue().asArray());
            else if (entry.getKey().equals(OWL.EQUIVALENTCLASS)) {
               TTValue equType = entry.getValue();
               if (equType.isList())
                  addEquivalentClasses(iri, entry.getValue().asArray());
               else if (equType.asNode().get(OWL.WITHRESTRICTIONS) == null)
                  Logger.error("unknown equivalent class type " + currentEntity.getIri());
            } else if (entry.getKey().equals(RDFS.SUBPROPERTYOF)) {
               TTIriRef propertyType;
               if (entity.isType(RDF.PROPERTY))
                  propertyType=OWL.DATAPROPERTY;
               else if (entity.isType(OWL.DATAPROPERTY))
                  propertyType=OWL.DATAPROPERTY;
               else
                  propertyType= OWL.OBJECTPROPERTY;
               addSubPropertyOf(iri, propertyType,entry.getValue().asArray());
            } else if (entry.getValue().isLiteral())
               addAnnotation(iri,entry.getKey(),entry.getValue());
         }
      }
   }
   private void checkUndeclared(IRI iri,OWLEntity entity){
      if (ttManager.getEntity(iri.toString())==null) {
         OWLDeclarationAxiom declaration = dataFactory.getOWLDeclarationAxiom(entity);
         manager.addAxiom(ontology, declaration);
      }
   }
   private void addEquivalentClasses(IRI iri,TTArray eqClasses) {
      if (eqClasses==null){
         Logger.error("Null equivalent classes in " + currentEntity.getIri());
      }
      for (TTValue exp:eqClasses.getElements()) {
           if (exp.isIriRef()||exp.asNode().get(OWL.WITHRESTRICTIONS) == null) {
            OWLEquivalentClassesAxiom equAx;
            equAx = dataFactory.getOWLEquivalentClassesAxiom(
                dataFactory.getOWLClass(iri),
                getOWLClassExpression(exp));
            manager.addAxiom(ontology, equAx);
         }
      }
   }
   private void addSubPropertyOf(IRI iri, TTIriRef propertyType, TTArray superClasses) {
      for (TTValue exp:superClasses.getElements()) {
         if (propertyType.equals(OWL.OBJECTPROPERTY)) {
            OWLSubObjectPropertyOfAxiom subAx = dataFactory
                .getOWLSubObjectPropertyOfAxiom(
                    dataFactory
                        .getOWLObjectProperty(iri),
                    dataFactory.getOWLObjectProperty(getIri(exp.asIriRef())));

                manager.addAxiom(ontology, subAx);
         } else {
            OWLSubDataPropertyOfAxiom subAx = dataFactory.getOWLSubDataPropertyOfAxiom(
                dataFactory.getOWLDataProperty(iri),dataFactory.getOWLDataProperty(getIri(exp.asIriRef())));
            manager.addAxiom(ontology,subAx);

         }
      }
   }

   private void addSubClassOf(IRI iri, TTArray superClasses) {
      for (TTValue exp:superClasses.getElements()) {
         OWLSubClassOfAxiom subAx;
         subAx = dataFactory.getOWLSubClassOfAxiom(
             dataFactory.getOWLClass(iri),
             getOWLClassExpression(exp));
         manager.addAxiom(ontology, subAx);
      }
   }




   private OWLClassExpression getOPERestrictionAsOWlClassExpression(TTValue cex) {
      OWLObjectPropertyExpression owlOpe;
      TTNode exp= cex.asNode();
      if (exp.get(OWL.ONPROPERTY)!=null){
         IRI prop = getIri(exp.get(OWL.ONPROPERTY).asIriRef());
         owlOpe = dataFactory.getOWLObjectProperty(prop);
      } else {
         IRI prop = getIri(exp.get(OWL.INVERSEOF).asIriRef());
         owlOpe = dataFactory
             .getOWLObjectInverseOf(
                 dataFactory.getOWLObjectProperty(prop));
      }
      if (exp.get(OWL.ALLVALUESFROM)!=null){
         return dataFactory.getOWLObjectAllValuesFrom(
             owlOpe,
             getOWLClassExpression(exp.get(OWL.ALLVALUESFROM))
         );
      } if (exp.get(OWL.SOMEVALUESFROM)!=null){
         return dataFactory.getOWLObjectAllValuesFrom(
             owlOpe,
             getOWLClassExpression(exp.get(OWL.SOMEVALUESFROM))
         );
      } else  if (exp.get(OWL.MINCARDINALITY)!=null){
         return dataFactory.getOWLObjectSomeValuesFrom(
             owlOpe,
             getOWLClassExpression(exp.get(OWL.ONCLASS))
         );
      }else  if (exp.get(OWL.MAXCARDINALITY)!=null){
         return dataFactory.getOWLObjectSomeValuesFrom(
             owlOpe,
             getOWLClassExpression(exp.get(OWL.ONCLASS))
         );
      } else if (exp.get(OWL.ONCLASS)!=null){
         return dataFactory.getOWLObjectSomeValuesFrom(
             owlOpe,
             getOWLClassExpression(exp.get(OWL.ONCLASS))
         );
      }
      else {
         Logger.error("Unknown Restriction type "+ currentEntity.getIri());
         return dataFactory.getOWLClass("not sure", prefixManager);
      }

   }

   /**
    * produces either a single data property restriction or an object intersection of several cardinalities
    *
    * @param cex Discovery propertyData expression
    * @return OWL Class expression
    */
   private OWLClassExpression getDPERestrictionAsOWLClassExpression(TTValue cex) {
      TTNode exp = cex.asNode();

      IRI prop = getIri(exp.get(OWL.ONPROPERTY).asIriRef());
      if (exp.get(OWL.SOMEVALUESFROM) != null) {
         return dataFactory.getOWLDataSomeValuesFrom(
             dataFactory.getOWLDataProperty(prop),
             getOWLDataRange(exp));
      }else if (exp.get(OWL.ALLVALUESFROM) != null) {
         return dataFactory.getOWLDataAllValuesFrom(
             dataFactory.getOWLDataProperty(prop),
             getOWLDataRange(exp));
      } else if (exp.get(OWL.ONDATARANGE)!=null) {
         return dataFactory.getOWLDataSomeValuesFrom(
             dataFactory.getOWLDataProperty(prop),
             getOWLDataRange(exp));
      }else
         return
             dataFactory.getOWLClass("OWL EL limit - cardinality and data restricions not supported", prefixManager);
   }



   public OWLClassExpression getOWLClassExpression(TTValue cex) {
      if (cex==null)
         Logger.error("null class expression in entity "+ currentEntity.getIri());
      if (cex.isIriRef()) {
         IRI iri= getIri(cex.asIriRef());
         checkUndeclared(iri,dataFactory.getOWLEntity(EntityType.CLASS,iri));
         return dataFactory.getOWLClass(getIri(cex.asIriRef()));
      } else if (cex.isNode()) {
         if (cex.asNode().get(OWL.INTERSECTIONOF) != null) {
            return dataFactory.getOWLObjectIntersectionOf(
                cex.asNode().get(OWL.INTERSECTIONOF).asArray().getElements()
                    .stream()
                    .map(this::getOWLClassExpression)
                    .collect(Collectors.toSet()));
         } else if (cex.asNode().get(OWL.UNIONOF) != null) {
            return dataFactory.getOWLObjectUnionOf(
                cex.asNode().get(OWL.UNIONOF).asArray().getElements()
                    .stream()
                    .map(this::getOWLClassExpression)
                    .collect(Collectors.toSet()));
         } else if (cex.asNode().get(OWL.ONPROPERTY) != null) {
            TTIriRef propertyType = guessPropertyType(cex.asNode());
            if (propertyType.equals(OWL.DATAPROPERTY))
               return getDPERestrictionAsOWLClassExpression(cex);
            else
               return getOPERestrictionAsOWlClassExpression(cex);
         } else if (cex.asNode().get(OWL.ONEOF) != null) {
            return getOneOfAsOWLClassExpression(cex);
         } else if (cex.asNode().get(OWL.COMPLEMENTOF) != null) {
            return (getComplementOfAsAOWLClassExpression(cex));
         } else if (cex.asNode().get(OWL.UNIONOF) != null) {
            return dataFactory.getOWLClass("unions not supported", prefixManager);
         }
      }
      Logger.error("Unknown classExpression type "+ currentEntity.getIri());
      return dataFactory.getOWLClass("not sure of type of expression", prefixManager);

}

   private TTIriRef guessPropertyType(TTNode exp) {
      TTEntity entity= ttManager.getEntity(exp.get(OWL.ONPROPERTY).asIriRef().getIri());
      if (entity==null)
         return OWL.OBJECTPROPERTY;
      if (entity.isType(OWL.DATAPROPERTY))
         return OWL.DATAPROPERTY;
      else  if (entity.isType(RDF.PROPERTY))
         return OWL.DATAPROPERTY;
      else
         return OWL.OBJECTPROPERTY;
   }


   private OWLClassExpression getComplementOfAsAOWLClassExpression(TTValue cex) {
      return dataFactory
          .getOWLObjectComplementOf(
              getOWLClassExpression(
                  cex.asNode().get(OWL.COMPLEMENTOF)));
   }

   private OWLClassExpression getOneOfAsOWLClassExpression(TTValue cex) {
      Set<OWLNamedIndividual> indiList = new HashSet<>();
      for (TTValue oneOf : cex.asArray().getElements()) {
         indiList.add(dataFactory.getOWLNamedIndividual(getIri(oneOf.asIriRef())));
      }
      return dataFactory.getOWLObjectOneOf(indiList);
   }

   private OWLDataRange getOWLDataRange(TTValue exp) {
      if (exp.asNode().get(OWL.ONDATARANGE)!=null)
         return dataFactory.getOWLDatatype(getIri(exp.asNode().get(OWL.ONDATARANGE).asIriRef()));
      else
         return dataFactory.getOWLDatatype(getIri("xsd:string"));

   }

   private void addDeclaration(TTEntity ttEntity){

      IRI iri = getIri(ttEntity.getIri());
      OWLEntity entity;;
      if (ttEntity.isType(OWL.CLASS))
            entity = dataFactory.getOWLEntity(EntityType.CLASS, iri);
         else if (ttEntity.isType(OWL.OBJECTPROPERTY))
            entity = dataFactory.getOWLEntity(EntityType.OBJECT_PROPERTY, iri);
         else if (ttEntity.isType(OWL.DATAPROPERTY))
            entity = dataFactory.getOWLEntity(EntityType.DATA_PROPERTY, iri);
      else if (ttEntity.isType(RDF.PROPERTY))
         entity = dataFactory.getOWLEntity(EntityType.DATA_PROPERTY, iri);
         else if (ttEntity.isType(OWL.ANNOTATIONPROPERTY))
            entity = dataFactory.getOWLEntity(EntityType.ANNOTATION_PROPERTY, iri);
         else if (ttEntity.isType(OWL.NAMEDINDIVIDUAL))
            entity = dataFactory.getOWLEntity(EntityType.NAMED_INDIVIDUAL, iri);
         else
            entity= dataFactory.getOWLEntity(EntityType.CLASS,iri);
      OWLDeclarationAxiom declaration = dataFactory.getOWLDeclarationAxiom(entity);
      manager.addAxiom(ontology, declaration);
   }





   private void addAnnotation(IRI iri, TTIriRef property,TTValue value){
            OWLAnnotation annotation = dataFactory.getOWLAnnotation(
                dataFactory.getOWLAnnotationProperty(getIri(property.getIri())),
                dataFactory.getOWLLiteral(value.asLiteral().getValue()));
         manager.addAxiom(ontology, dataFactory.getOWLAnnotationAssertionAxiom(iri, annotation));
      }



   private IRI getIri(TTIriRef tiri) {
      String iri = tiri.getIri();
      return getIri(iri);
   }
   private IRI getIri(String iri){
      if (iri.toLowerCase().startsWith("http:") || iri.toLowerCase().startsWith("https:"))
         return IRI.create(iri);
      else
         return prefixManager.getIRI(iri);
   }

}

