package org.endeavourhealth.informationmanager.common.transform;

import org.endeavourhealth.imapi.model.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.*;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.reasoner.*;
import uk.ac.manchester.cs.factplusplus.owlapiv3.FaCTPlusPlusReasonerFactory;
import uk.ac.manchester.cs.owl.owlapi.OWLDataFactoryImpl;

import java.util.*;
import java.util.zip.DataFormatException;

/**
 * Classifies an ontology using an owl reasoner, generating ISA relationships from a Discovery ontology document.
 * Generates inferred role groups (Snomed pattern) from the existential quntifiers and propogates them to subclasses
 * Generatesi inferred Property groups from the SHACL property definitions and propogates them to subclasses
 */
public class ReasonerPlus {
   private TTDocument document;
   private HashMap<String, TTEntity> entityMap;
   private boolean consistent;
   private OWLReasoner owlReasoner;
   private TTManager manager;
   private Set<String> done;

   public ReasonerPlus(){
      manager= new TTManager();
   }


   public TTDocument generateInferred(TTDocument document) throws OWLOntologyCreationException,DataFormatException {
      //Creates isas
      classify(document);
      manager= new TTManager();
      manager.setDocument(document);
      generateRoleGroups(document);
      generateInheritedRoles(document);
      generatePropertyGroups(document);

      return document;
   }




   private void generatePropertyGroups(TTDocument document) {
      if (document.getEntities() == null)
         return;
      for (TTEntity entity:document.getEntities()) {
         done = new HashSet<>();
         done.add(entity.getIri());
         if (entity.isType(IM.RECORD)||entity.isType(SHACL.NODESHAPE))
            setPropertyGroups(entity);
      }
   }

   private void generateRoleGroups(TTDocument document) throws DataFormatException {
      if (document.getEntities() == null)
         return;
      for (TTEntity entity:document.getEntities()) {
         setRoleGroups(entity);


      }
   }

   /**
    * Takes a classified document and propogates inherited roles and properties to descendants to enable direct access to properties
    * Creates roles in role group and properties in Property group
    * @param document The document including the inherited roles
    * @return a document containing the rols as inherited
    */

   public TTDocument generateInheritedRoles(TTDocument document) {
      //Now brings down properties
      for (TTEntity entity:document.getEntities()){
         done = new HashSet<>();
         done.add(entity.getIri());
         if (entity.get(IM.IS_A)!=null)
            for (TTValue parent:entity.get(IM.IS_A).asArray().getElements()) {
               TTEntity parentEntity= manager.getEntity(parent.asIriRef().getIri());
               if (parentEntity!=null)
                  bringDownRoles(entity, parentEntity);
            }
      }
      return document;


   }


   private void setPropertyGroups(TTEntity entity) {
      if (entity.get(IM.PROPERTY_GROUP)!=null)
         entity.getPredicateMap().remove((IM.PROPERTY_GROUP));
      done= new HashSet<>();
      done.add(entity.getIri());
      Integer groupNumber=0;
      if (entity.get(SHACL.PROPERTY)!=null){
         //Adds the entity's property list as the first Property group for the entity
         TTArray propertyGroups = new TTArray();
         entity.set(IM.PROPERTY_GROUP,propertyGroups);
         TTNode propertyGroup= new TTNode();
         propertyGroups.add(propertyGroup);
         propertyGroup.set(SHACL.PROPERTY,entity.get(SHACL.PROPERTY));
         propertyGroup.set(IM.GROUP_NUMBER,TTLiteral.literal(groupNumber.toString()));

      }
      if (entity.get(IM.IS_A)!=null)
         for (TTValue element:entity.get(IM.IS_A).asArray().getElements()){
            if (element.isIriRef()){
               TTEntity parentEntity= manager.getEntity(element.asIriRef().getIri());
               if (parentEntity!=null) {
                  groupNumber++;
                  groupNumber= bringDownPropertyGroups(entity, parentEntity,groupNumber);
               }
            }

         }
   }

   private Integer bringDownPropertyGroups(TTEntity entity, TTEntity parentEntity,Integer groupNumber) {
      if (done.contains(parentEntity.getIri()))
         return groupNumber;

      if (parentEntity.get(SHACL.PROPERTY)!=null) {
         TTNode propertyGroup = null;
         for (TTValue element : parentEntity.get(SHACL.PROPERTY).asArray().getElements()) {
            TTNode parentProperty = element.asNode();
            TTIriRef property = parentProperty.get(SHACL.PATH).asIriRef();
            TTValue value = null;
            if (parentProperty.get(SHACL.DATATYPE) != null)
               value = parentProperty.get(SHACL.DATATYPE);
            else if (parentProperty.get(SHACL.CLASS) != null) {
               value = parentProperty.get(SHACL.CLASS);
            }
            if (value != null)
               if (!overriddenProperties(property, value, entity)) {
                  if (entity.get(IM.PROPERTY_GROUP)==null){
                     TTArray propertyGroups= new TTArray();
                     entity.set(IM.PROPERTY_GROUP,propertyGroups);
                  }
                  if (propertyGroup==null){
                     propertyGroup= new TTNode();
                     propertyGroup.set(IM.GROUP_NUMBER,TTLiteral.literal(groupNumber.toString()));
                     propertyGroup.set(IM.INHERITED_FROM,TTIriRef.iri(parentEntity.getIri()));
                     entity.get(IM.PROPERTY_GROUP).asArray().add(propertyGroup);
                  }
                  if (propertyGroup.get(SHACL.PROPERTY)==null)
                     propertyGroup.set(SHACL.PROPERTY,new TTArray());
                  propertyGroup.get(SHACL.PROPERTY).asArray().add(parentProperty);
               }
         }
      }
      if (parentEntity.get(IM.IS_A)!=null)
         for (TTValue grandparent:parentEntity.get(IM.IS_A).asArray().getElements()) {
            TTEntity grandparentEntity= manager.getEntity(grandparent.asIriRef().getIri());
            if (grandparentEntity!=null) {
               groupNumber++;
               return bringDownPropertyGroups(entity, grandparentEntity,groupNumber);
            }
         }
      return groupNumber;

   }



   private void bringDownRoles(TTEntity entity,TTEntity parent) {
      if (done.contains(parent.getIri()))
         return;
      done.add(parent.getIri());
      TTArray roleGroups=null;
      if (parent.get(IM.ROLE_GROUP)!=null) {
         TTArray parentRoleGroups = parent.get(IM.ROLE_GROUP).asArray();
         TTNode newGroup=null;
         for (TTValue element : parentRoleGroups.getElements()) {
            TTNode roleGroup = element.asNode();
            for (TTValue role:roleGroup.get(IM.ROLE).asArray().getElements()){
               TTIriRef property= role.asNode().get(OWL.ONPROPERTY).asIriRef();
               if (!overriddenRole(property,entity))
                  newGroup= addInheritedRole(entity,newGroup,role);
            }
         }
      }
      if (parent.get(IM.IS_A)!=null)
         for (TTValue grandparent:parent.get(IM.IS_A).asArray().getElements()) {
            TTEntity grandparentEntity= manager.getEntity(grandparent.asIriRef().getIri());
            if (grandparentEntity!=null)
               bringDownRoles(entity, grandparentEntity);
         }

   }

   private TTNode addInheritedRole(TTEntity entity, TTNode newGroup, TTValue role) {
      Integer nextGroup = 1;
      if (entity.get(IM.ROLE_GROUP) != null) {
         nextGroup = entity.get(IM.ROLE_GROUP).asArray().size();
      } else{
         TTArray groups= new TTArray();
         entity.set(IM.ROLE_GROUP,groups);
      }
      if (newGroup==null){
         newGroup = new TTNode();
         newGroup.set(IM.GROUP_NUMBER,TTLiteral.literal(nextGroup));
         newGroup.set(IM.ROLE,new TTArray());
         entity.get(IM.ROLE_GROUP).asArray().add(newGroup);
      }
      newGroup.get(IM.ROLE).asArray().add(role);
      return newGroup;
   }

   private boolean overriddenRole(TTIriRef property, TTEntity entity) {
      if (entity.get(IM.ROLE_GROUP)!=null)
         for (TTValue roleGroup : entity.get(IM.ROLE_GROUP).asArray().getElements()) {
            for (TTValue role:roleGroup.asNode().get(IM.ROLE).asArray().getElements()){
               Set<Map.Entry<TTIriRef,TTValue>> predicates= role.asNode().getPredicateMap().entrySet();
               for (Map.Entry<TTIriRef,TTValue> entry:predicates){
                  TTIriRef subProperty = entry.getKey();
                  if (manager.isA(subProperty, property))
                     return true;
               }
            }

         }
      return false;
   }



   private boolean overriddenProperties(TTIriRef property, TTValue value, TTEntity entity) {
      if (entity.get(IM.PROPERTY_GROUP) != null) {
         TTArray propertyGroups = entity.get(IM.PROPERTY_GROUP).asArray();
         for (TTValue element : propertyGroups.getElements()) {
            TTNode propertyGroup = element.asNode();
            TTArray rootProperties = propertyGroup.get(SHACL.PROPERTY).asArray();
            for (TTValue PropertyElement : rootProperties.getElements()) {
               TTNode Property = PropertyElement.asNode();
               if (manager.isA(Property.get(SHACL.PATH).asIriRef(), property)) {
                  if (Property.get(SHACL.DATATYPE) != null) {
                     if (value.asIriRef().equals(Property.get(SHACL.DATATYPE).asIriRef()))
                        return true;
                  } else {
                     if (Property.get(SHACL.CLASS) != null) {
                        TTValue rootValue = Property.get(SHACL.CLASS);
                        if (rootValue.isIriRef())
                           if (manager.isA(Property.get(SHACL.CLASS).asIriRef(), value.asIriRef()))
                              return true;
                     }
                  }
               }
            }
         }
      }
      return false;
   }

   private void setRoleGroups(TTEntity entity) throws DataFormatException {
      if (entity.get(IM.ROLE_GROUP)!=null)
         entity.getPredicateMap().remove(IM.ROLE_GROUP);
      if (entity.get(RDFS.SUBCLASSOF)!=null){
         for (TTValue superClass:entity.get(RDFS.SUBCLASSOF).asArray().getElements()){
            setExpression(entity,superClass);

         }

      }
      if (entity.get(OWL.EQUIVALENTCLASS)!=null) {
         if (entity.get(OWL.EQUIVALENTCLASS).isList()) {
            for (TTValue equClass : entity.get(OWL.EQUIVALENTCLASS).asArray().getElements()) {
               setExpression(entity, equClass);
            }
         }
      }
   }

   private void setExpression(TTEntity entity, TTValue exp) throws DataFormatException {
      if (exp.isIriRef())
         return;
      if (exp.asNode().get(OWL.INTERSECTIONOF) != null) {
         for (TTValue subExp : exp.asNode().get(OWL.INTERSECTIONOF).asArray().getElements()) {
            setExpression(entity, subExp);
         }
      } else if (exp.asNode().get(OWL.UNIONOF)!=null) {
         for (TTValue subExp : exp.asNode().get(OWL.UNIONOF).asArray().getElements()) {
            setExpression(entity, subExp);
         }
      }else
         addRole(entity,exp.asNode());
   }

   private void addRole(TTEntity entity, TTNode attribute) throws DataFormatException {


      TTValue roleGroups= entity.get(IM.ROLE_GROUP);
      if (roleGroups==null) {
         roleGroups = new TTArray();
         entity.set(IM.ROLE_GROUP, roleGroups);
         TTNode roleGroup= new TTNode();
         roleGroups.asArray().add(roleGroup);
         roleGroup.set(IM.GROUP_NUMBER,TTLiteral.literal(0));
      }
      TTNode roleGroup= roleGroups.asArray().getElements().stream().findFirst().get().asNode();
      if (attribute.get(OWL.ONPROPERTY) != null) {
         if (attribute.get(OWL.ONCLASS) != null)
               roleGroup.set(attribute.get(OWL.ONPROPERTY).asIriRef(),attribute.get(OWL.ONCLASS));
         else if (attribute.get(OWL.SOMEVALUESFROM) != null)
               roleGroup.set(attribute.get(OWL.ONPROPERTY).asIriRef(),attribute.get(OWL.SOMEVALUESFROM));
         else if (attribute.get(OWL.ALLVALUESFROM) != null)
               roleGroup.set(attribute.get(OWL.ONPROPERTY).asIriRef(),attribute.get(OWL.ALLVALUESFROM));
         else if (attribute.get(OWL.ONDATARANGE) != null)
               roleGroup.set(attribute.get(OWL.ONPROPERTY).asIriRef(),attribute.get(OWL.ONDATARANGE));
         else if (attribute.get(OWL.ONDATATYPE) != null)
               roleGroup.set(attribute.get(OWL.ONPROPERTY).asIriRef(),attribute.get(OWL.ONDATATYPE));
         else if (attribute.get(OWL.HASVALUE) != null)
               roleGroup.set(attribute.get(OWL.ONPROPERTY).asIriRef(),attribute.get(OWL.HASVALUE));
            else
               throw new DataFormatException("unknown property construct");
         } else
            if (attribute.get(OWL.WITHRESTRICTIONS)==null)
               throw new DataFormatException("unknown class expression format");
   }



   /**
    * Classifies an ontology using an OWL Reasoner
    *
    * @return set of child -  parent "isa" nodes
    * @param document The TTDocument to classify
    * @throws  OWLOntologyCreationException for invalid owl formats leading to inability to create ontology
    * @throws DataFormatException for invalid owl content
    */

   public TTDocument classify(TTDocument document) throws OWLOntologyCreationException, DataFormatException {
      this.document = document;
      if (document.getEntities() == null)
         return document;
      entityMap = new HashMap<>();
      //builds entity map for later look up
      document.getEntities().forEach(c -> entityMap.put(c.getIri(), c));
      TTToOWLEL transformer = new TTToOWLEL();
      TTManager dmanager = new TTManager();
      dmanager.setDocument(document);
      OWLOntologyManager owlManager = transformer.transform(document, dmanager);
      Set<OWLOntology> owlOntologySet = owlManager.getOntologies();
      Optional<OWLOntology> owlOntology = owlOntologySet.stream().findFirst();
      if (owlOntology.isPresent()) {
         OWLReasonerConfiguration config = new SimpleConfiguration();
         OWLOntology o = owlOntology.get();
         owlReasoner = new FaCTPlusPlusReasonerFactory().createReasoner(o, config);
         owlReasoner.precomputeInferences();
         if (!owlReasoner.isConsistent()) {
            consistent = false;
            return null;
         }
         consistent = true;
         OWLDataFactory dataFactory = new OWLDataFactoryImpl();
         //First removes the current "isas"
         for (TTEntity c : document.getEntities()) {
            c.getPredicateMap().remove(IM.IS_A);
            if (c.isType(OWL.OBJECTPROPERTY)) {
               OWLObjectPropertyExpression ope = dataFactory.getOWLObjectProperty(IRI.create(c.getIri()));
               NodeSet<OWLObjectPropertyExpression> superOb = owlReasoner.getSuperObjectProperties(ope, true);
               if (superOb != null) {
                  TTArray parents = new TTArray();
                  c.set(IM.IS_A, parents);
                  superOb.forEach(sob -> {
                     if (!sob.getRepresentativeElement().isAnonymous())
                        parents.add(TTIriRef
                       .iri(sob.getRepresentativeElement()
                         .asOWLObjectProperty()
                         .getIRI()
                         .toString()));});
                  }
            } else if (c.isType(OWL.DATAPROPERTY)) {
               OWLDataProperty dpe = dataFactory.getOWLDataProperty(IRI.create(c.getIri()));
               NodeSet<OWLDataProperty> superP = owlReasoner.getSuperDataProperties(dpe, true);
               if (superP != null) {
                  TTArray parents = new TTArray();
                  c.set(IM.IS_A, parents);
                  superP.forEach(sob -> {
                     if (!sob.getRepresentativeElement().isAnonymous())
                        parents.add(TTIriRef
                       .iri(sob
                         .getRepresentativeElement().asOWLDataProperty()
                         .getIRI()
                         .toString()));});
                  };
            } else {
                  OWLClassExpression owlClass = dataFactory.getOWLClass(IRI.create(c.getIri()));
                  NodeSet<OWLClass> superClasses = owlReasoner.getSuperClasses(owlClass, true);
                  if (superClasses != null) {
                     superClasses.forEach(sup -> {TTIriRef iri= TTIriRef.iri(sup.getRepresentativeElement()
                         .asOWLClass()
                         .getIRI()
                         .toString());
                     if (!iri.equals(OWL.THING))
                        addIsa(c,iri);}
                     );
                  }
                  Node<OWLClass> equClasses= owlReasoner.getEquivalentClasses(owlClass);
                  if (equClasses!=null){
                     equClasses.forEach(sup -> {if (sup.isOWLClass()){
                        TTIriRef superIri= TTIriRef.iri(sup.getIRI().toString());
                        if (!superIri.equals(TTIriRef.iri(c.getIri())))
                           addIsa(c,superIri);}
                           ;});
                  }


               }
            }
         }
      return document;
   }
   private void addIsa(TTEntity entity,TTIriRef parent){
      if (entity.get(IM.IS_A)==null)
         entity.set(IM.IS_A,new TTArray());
      entity.get(IM.IS_A).asArray().add(parent);
   }

}
