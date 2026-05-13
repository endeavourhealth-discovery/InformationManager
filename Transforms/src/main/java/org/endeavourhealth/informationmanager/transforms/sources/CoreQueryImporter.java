package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.logic.reasoner.IndicatorGenerator;
import org.endeavourhealth.imapi.model.customexceptions.EQDException;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class CoreQueryImporter implements TTImport {
  public TTDocument document;

  @Override
  public void importData(TTImportConfig config) throws ImportException {
    try (TTManager manager = new TTManager()) {
      document = manager.createDocument();
      addressProperty("homeAddress", "home");
      addressProperty("workAddress", "work");
      addressProperty("temporaryAddress", "temp");
      addressProperty("placeOfResidenceAtEvent", "home");
      telephoneProperty("homeTelephoneNumber", "home");
      telephoneProperty("mobileTelephoneNumber", "mobile");
      telephoneProperty("workTelephoneNumber", "mobile");
      mainLanguage();
      ethnicity();
      entityFilter();
      age();
      ageAtEvent();
      placeOfResidenceAtEvent();
      gmsRegistration();
      gmsRegistrationStatus();
      gmsRegisteredPractice();
      getDescendants();
      getSubclasses();
      getConcepts();
      getAllowableProperties();
      getAllowablePropertyAncestors();
      isValidProperty();
      isValidType();
      isValidDescendant();
      isAllowableRange();
      getSearchAll();
      allowableSubTypes();
      currentGMS();
      deleteSets();
      getAncestors();
      getSubsets();
      testQuery();
      objectPropertyRangeSuggestions();
      dataPropertyRangeSuggestions();
      dataModelPropertyRange();
      dataModelPropertyByShape();
      searchFolders();
      searchContainedIn();
      searchAllowableSubclass();
      searchAllowableChildOf();
      searchAllowableContainedIn();
      generateDefaultCohorts(manager);
      //generateDefaultIndicators(manager);
      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(GRAPH.IM)) {
        filer.fileDocument(document);
      }
    } catch (Exception e) {
      throw new ImportException(e.getMessage(), e);
    }
  }
  private void generateDefaultIndicators(TTManager manager) throws JsonProcessingException {
    TTEntity defaults= new TTEntity()
      .setIri(NAMESPACE.IM+"StandardIndicators")
      .setName("Standard indicators")
      .addType(iri(IM.FOLDER))
      .setScheme(iri(NAMESPACE.IM))
      .addObject(iri(IM.IS_CONTAINED_IN), iri(NAMESPACE.IM+"Indicators"));
    defaults.set(IM.ORDER,TTLiteral.literal(1));
    manager.getDocument().addEntity(defaults);
    IndicatorGenerator generator= new IndicatorGenerator();
    TTEntity GMSIndicator= generator.createIndicator(NAMESPACE.IM+"GMSIndicator"
      ,"GMS Registered patients (indicator)"
      ,"The indicator for GMS registered patients used by most patient indicators"
      ,NAMESPACE.IM,
      null,
      NAMESPACE.IM+"Q_RegisteredGMS",
      null);
    GMSIndicator.set(iri(IM.IS_CONTAINED_IN), iri(NAMESPACE.IM+"StandardIndicators"));
    manager.getDocument().addEntity(GMSIndicator);
  }

  private void generateDefaultCohorts(TTManager manager) throws JsonProcessingException {
    TTEntity gms = manager.getEntity(NAMESPACE.IM + "Q_RegisteredGMS");
    gms.addObject(TTIriRef.iri(IM.IS_CONTAINED_IN), TTVariable.iri(NAMESPACE.IM + "Q_DefaultCohorts"));
    gms.addObject(iri(IM.CONTEXT_ORDER), new TTNode().set(SHACL.ORDER, TTLiteral.literal(1))
      .set(IM.CONTEXT, TTIriRef.iri(NAMESPACE.IM + "Q_DefaultCohorts")));
    int order = 1;
    for (String defaultFolder : List.of("Patient", "PeopleAndThings", "ClinicalInformation", "PersonalHealthManagement", "ProcessOfCare", "Q_Queries")) {
      order++;
      addToDefaults(defaultFolder, manager, order);
    }
  }

  private void addToDefaults(String defaultEntity, TTManager manager, int order) {
    TTEntity entity = new TTEntity()
      .setIri(NAMESPACE.IM + defaultEntity)
      .setScheme(NAMESPACE.IM.asIri())
      .setCrud(iri(IM.ADD_QUADS));
    entity.addObject(TTVariable.iri(IM.IS_CONTAINED_IN), TTVariable.iri(NAMESPACE.IM + "Q_DefaultCohorts"));
    entity.addObject(iri(IM.CONTEXT_ORDER), new TTNode().set(SHACL.ORDER, TTLiteral.literal(order)).set(IM.CONTEXT, TTIriRef.iri(NAMESPACE.IM + "Q_DefaultCohorts")));
    manager.getDocument().addEntity(entity);
  }


  private void gmsRegisteredPractice() throws JsonProcessingException {
    TTEntity gms = new TTEntity()
      .setIri(NAMESPACE.IM + "gmsRegisteredPractice")
      .setDescription("Returns the practice if the patient is registered as a GMS patient on the reference date")
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .setScheme(NAMESPACE.IM.asIri())
      .addObject(iri(SHACL.PARAMETER), new TTNode()
        .set(iri(RDFS.LABEL), TTLiteral.literal("searchDate"))
        .set(iri(SHACL.DATATYPE), iri(NAMESPACE.IM + "DateTime")));
    Query query = getGmsIsRegistered();
    query
      .orderBy(o -> o.addProperty(new OrderDirection().setNodeRef("reg").setIri(NAMESPACE.IM + "effectiveDate").setDirection(Order.descending)).setLimit(1));
    query.return_(p -> p
      .setNodeRef("reg")
      .setIri(NAMESPACE.IM + "provider"));
    query.setName("GMS registered practice");
    gms.set(iri(IM.DEFINITION), TTLiteral.literal(query));
    document.addEntity(gms);
  }

  private Query getGmsIsRegistered() {
    return new Query()
      .setName("Patient registered as GMS on the reference date")
      .setDescription("Is the patient registered as a GMS patient on the reference date?")
      .setTypeOf(NAMESPACE.IM + "Patient")
      .and(m -> m
        .setTypeOf(NAMESPACE.IM + "EpisodeOfCare")
        .where(w -> w
          .and(pv -> pv
            .setIri(NAMESPACE.IM + "gpPatientType")
            .addIs(new Node().setIri("http://hl7.org/fhir/registration-type/r").setName("Regular GMS patient")))
          .and(pv -> pv
            .setIri(NAMESPACE.IM + "effectiveDate")
            .setOperator(Operator.lte)
            .setCompare(new Compare()
              .setLeft(new ValueSource()
                .setIri(NAMESPACE.IM+"effectiveDate"))
              .setRight(new ValueSource()
                .setParameter("$searchDate"))))
          .and(pv -> pv
            .or(pv1 -> pv1
              .setIri(NAMESPACE.IM + "endDate")
              .setIsNull(true))
            .or(pv1 -> pv1
              .setIri(NAMESPACE.IM + "endDate")
              .setOperator(Operator.gt)
              .setCompare(new Compare()
                .setLeft(new ValueSource()
                  .setIri(NAMESPACE.IM+"endDate"))
                .setRight(new ValueSource()
                  .setParameter("$searchDate")))))));
  }


  private void gmsRegistration() throws JsonProcessingException {
    TTEntity gms = new TTEntity()
      .setIri(NAMESPACE.IM + "gmsRegistrationAtEvent")
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .setScheme(NAMESPACE.IM.asIri())
      .addObject(iri(SHACL.PARAMETER), new TTNode()
        .set(iri(RDFS.LABEL), TTLiteral.literal("searchDate"))
        .set(iri(SHACL.DATATYPE), iri(NAMESPACE.IM + "DateTime")))
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(getGmsQuery()));

    document.addEntity(gms);
  }

  private Query getGmsQuery() {
    return new Query()
      .setName("GP GMS registration at a reference date")
      .setDescription("Retrieves the Registration status of active, left or died")
      .setTypeOf(NAMESPACE.IM + "Patient")
      .and(m -> m
        .where(w -> w
          .setIri(NAMESPACE.IM + "gmsRegistrationStatus")
          .is(is -> is
            .setIri(NAMESPACE.IM + "CaseloadStatusActive"))));
  }

  private void gmsRegistrationStatus() throws JsonProcessingException {
    Query query = getGmsQuery();
    query.setName("Returns the gpRegistration status of a patient if they are currently registered as a regular GMS patient, or if died");
    query.setNode("currentEpisode");
    Return returnProperty = new Return();
    query.addReturn(returnProperty);
    returnProperty.case_(c -> c
      .when(when -> when
        .where(w -> w
          .or(w1 -> w1
            .setIri(NAMESPACE.IM + "dateOfDeath")
            .setIsNull(true))
          .or(w1 -> w1
            .setIri(NAMESPACE.IM + "dateOfDeath")
            .setOperator(Operator.lt)
            .setCompare(new Compare()
              .setLeft(new ValueSource()
                .setIri(NAMESPACE.IM+"dateOfDeath"))
              .setRight(new ValueSource()
                .setParameter("$searchDate")))))
        .setThen(NAMESPACE.IM + "CaseloadStatusDead"))
      .when(when -> when
        .where(pv -> pv
          .or(pv1 -> pv1
            .setNodeRef("currentEpisode")
            .setIri(NAMESPACE.IM + "endDate")
            .setIsNull(true))
          .or(pv1 -> pv1
            .setNodeRef("currentEpisode")
            .setIri(NAMESPACE.IM + "endDate")
            .setCompare(new Compare()
              .setLeft(new ValueSource()
                .setNodeRef("currentEpisode")
                .setIri(NAMESPACE.IM+"endDate"))
              .setRight(new ValueSource()
                .setParameter("$searchDate")))
            .setOperator(Operator.gt)))
        .setThen(NAMESPACE.IM + "CaseloadStatusActive"))
      .setElse(NAMESPACE.IM + "CaseloadStatusLeft"));

    TTEntity gms = new TTEntity()
      .setIri(NAMESPACE.IM + "gmsRegistrationStatus")
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .setScheme(NAMESPACE.IM.asIri())
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(query));
    document.addEntity(gms);

  }


  private void ethnicity() throws JsonProcessingException {
    TTEntity ethnicity = new TTEntity()
      .setIri(NAMESPACE.IM + "ethnicity")
      .setCrud(iri(IM.ADD_QUADS))
      .setScheme(NAMESPACE.IM.asIri())
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Ethnicity definition")
          .setTypeOf(NAMESPACE.IM + "Observation")
          .where(ob->ob
            .setIri(NAMESPACE.IM + "concept")
            .is(is -> is.setIri(NAMESPACE.IM + "im:VSET_Ethnicity").setMemberOf(true)))
          .orderBy(ob -> ob.addProperty(new OrderDirection().setIri(NAMESPACE.IM + "effectiveDate").setDirection(Order.descending)).setLimit(1))));
    document.addEntity(ethnicity);

  }

  private void mainLanguage() throws JsonProcessingException {
    TTEntity ethnicity = new TTEntity()
      .setIri(NAMESPACE.IM + "mainSpokenLanguage")
      .setCrud(iri(IM.ADD_QUADS))
      .setScheme(NAMESPACE.IM.asIri())
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Main language definition")
          .setTypeOf(NAMESPACE.IM + "Observation")
          .where(ob->ob
            .setIri(NAMESPACE.IM + "concept")
            .is(is -> is.setIri(NAMESPACE.SNOMED+ "370157003").setDescendantsOrSelfOf(true)))
          .orderBy(ob -> ob.addProperty(new OrderDirection().setIri(NAMESPACE.IM + "effectiveDate").setDirection(Order.descending)).setLimit(1))));
    document.addEntity(ethnicity);

  }


  private void addressProperty(String propertyName, String value) throws JsonProcessingException {
    TTEntity address = new TTEntity()
      .setIri(NAMESPACE.IM + propertyName)
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .setScheme(NAMESPACE.IM.asIri())
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName(value + " address property definition")
          .path(p -> p.setIri(NAMESPACE.IM + "address").setTypeOf(NAMESPACE.IM + "AssignedAddress")
            .setNode("Address")
          )
          .where(and -> and
            .and(w -> w
              .setNodeRef("Address")
              .setIri(NAMESPACE.IM + "effectiveDate")
              .setCompare(new Compare()
                .setLeft(new ValueSource()
                  .setNodeRef("Address")
                  .setIri(NAMESPACE.IM+"effectiveDate"))
                .setRight(new ValueSource()
                  .setParameter("$eventDate")))
              .setOperator(Operator.lte))
            .and(w -> w
              .or(or->or
                .setNodeRef("Address")
                .setIri(NAMESPACE.IM + "endDate")
                .setIsNull(true))
              .or(or -> or
                .setNodeRef("Address")
                .setIri(NAMESPACE.IM + "endDate")
                .setOperator(Operator.gt)
                .setCompare(new Compare()
                  .setLeft(new ValueSource()
                    .setNodeRef("Address")
                    .setIri(NAMESPACE.IM+"endDate"))
                  .setRight(new ValueSource()
                    .setParameter("$eventDate")))))
            .and(w -> w
              .setNodeRef("Address")
              .setIri(NAMESPACE.IM + "addressUse")
              .is(is -> is.setIri("http://hl7.org/fhir/fhir-address-use/" + value))))
          .orderBy(ob -> ob.addProperty(new OrderDirection().setNodeRef("Address").setIri(NAMESPACE.IM + "effectiveDate").setDirection(Order.descending)).setLimit(1))));
    TTNode parameter = new TTNode();
    parameter.set(iri(RDFS.LABEL), TTLiteral.literal("eventDate"));
    parameter.set(iri(SHACL.DATATYPE), iri(NAMESPACE.IM + "DateTime"));
    address.addObject(iri(SHACL.PARAMETER), parameter);
    document.addEntity(address);


  }

  private void telephoneProperty(String propertyName, String value) throws JsonProcessingException {
    TTEntity address = new TTEntity()
      .setIri(NAMESPACE.IM + propertyName)
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .setScheme(NAMESPACE.IM.asIri())
      .set(iri(IM.DEFINITION), TTLiteral.literal(new Query()
        .setName(value + " telephone property definition")
        .path(p -> p.setIri(NAMESPACE.IM + propertyName).setTypeOf(NAMESPACE.IM + "TelephoneNumber")
          .setNode("Telephone")
        )
        .where(and -> and
          .and(w -> w
            .setNodeRef("Telephone")
            .setIri(IM.STATUS)
            .is(is -> is.setIri(IM.ACTIVE.toString())))
          .and(w -> w
            .setNodeRef("Telephone")
            .setIri(NAMESPACE.IM + "use")
            .is(is -> is.setIri("http://hl7.org/fhir/contact-point-use/" + value))))
        .orderBy(o -> o.addProperty(new OrderDirection().setNodeRef("Telephone").setIri(NAMESPACE.IM + "effectiveDate").setDirection(Order.descending)).setLimit(1))));
    document.addEntity(address);

  }

  private void age() throws JsonProcessingException {
    Query ageQuery = new Query();
    ageQuery.setName("Age function");
    ageQuery.where(w->w
      .compare(c->c
        .left(l->l
          .setIri(NAMESPACE.IM + "dateOfBirth"))
        .right(r->r
          .setParameter("$searchDate"))));
    TTEntity age = new TTEntity()
      .setIri(NAMESPACE.IM + "age")
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .setScheme(NAMESPACE.IM.asIri())
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(ageQuery));
    TTNode parameter = new TTNode();
    parameter.set(iri(RDFS.LABEL), TTLiteral.literal("searchDate"));
    parameter.set(iri(SHACL.DATATYPE), iri(NAMESPACE.IM + "DateTime"));
    age.addObject(iri(SHACL.PARAMETER), parameter);
    document.addEntity(age);
  }
  private void ageAtEvent() throws JsonProcessingException {
    Query ageQuery = new Query();
    ageQuery.setName("Age at event function")
    .path(p->p
      .setIri(NAMESPACE.IM + "patient")
        .setNode("pat")
          .setTypeOf(NAMESPACE.IM.toString() + "Patient"))
    .where(w->w
      .compare(c->c
        .left(l->l
          .setIri(NAMESPACE.IM + "dateOfBirth"))
        .right(r->r
          .setIri(NAMESPACE.IM + "effectiveDate"))));
    TTEntity age = new TTEntity()
      .setIri(NAMESPACE.IM + "ageAtEvent")
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .setScheme(NAMESPACE.IM.asIri())
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(ageQuery));
    document.addEntity(age);
  }

  private void placeOfResidenceAtEvent() throws JsonProcessingException {
    TTEntity entity = new TTEntity()
      .setIri(NAMESPACE.IM + "placeOfResidenceAtEvent")
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .setScheme(NAMESPACE.IM.asIri())
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("place of residence at event")
          .path(p -> p.setIri(NAMESPACE.IM + "address").setTypeOf(NAMESPACE.IM + "AssignedAddress")
            .setNode("Address")
          )
          .where(and -> and
            .and(w -> w
              .setNodeRef("Address")
              .setIri(NAMESPACE.IM + "effectiveDate")
              .setCompare(new Compare()
                .setLeft(new ValueSource()
                  .setNodeRef("Address")
                  .setIri(NAMESPACE.IM+"effectiveDate"))
                .setRight(new ValueSource()
                  .setIri(NAMESPACE.IM.toString()+"effectiveDate")))
              .setOperator(Operator.lte))
            .and(w -> w
              .or(or->or
                .setNodeRef("Address")
                .setIri(NAMESPACE.IM + "endDate")
                .setIsNull(true))
              .or(or -> or
                .setNodeRef("Address")
                .setIri(NAMESPACE.IM + "endDate")
                .setOperator(Operator.gt)
                .setCompare(new Compare()
                  .setLeft(new ValueSource()
                    .setNodeRef("Address")
                    .setIri(NAMESPACE.IM+"endDate"))
                  .setRight(new ValueSource()
                    .setIri(NAMESPACE.IM.toString()+"effectiveDate")))))
            .and(w -> w
              .setNodeRef("Address")
              .setIri(NAMESPACE.IM + "addressUse")
              .is(is -> is.setIri("http://hl7.org/fhir/fhir-address-use/home" ))))
          .orderBy(ob -> ob.addProperty(new OrderDirection().setNodeRef("Address").setIri(NAMESPACE.IM + "effectiveDate").setDirection(Order.descending)).setLimit(1))));
    document.addEntity(entity);

  }




  private void objectPropertyRangeSuggestions() throws JsonProcessingException {
    TTEntity query = getFormValidationEntity("ObjectPropertyRangeSuggestions", "Range suggestions for object property", "takes account of the data model shape that the property is part of")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        getRangeSuggestion()));

    document.addEntity(query);
  }

  private Query getRangeSuggestion() {
    return new Query()
      .setName("Suggested range for property")
      .setDescription("get node or class values (ranges) of properties that have $this as their path")
      .and(m -> m
        .setName("property range(s)")
        .setDescription("Range(s) (SHACL node or SHACL class) of (implied) object properties")
        .setWhere(new Where()
          .or(p -> p
            .setIri(SHACL.NODE)
            .setNode("range"))
          .or(p -> p
            .setIri(SHACL.DATATYPE)
            .setNode("range"))
          .or(p -> p
            .setIri(SHACL.CLASS)
            .setNode("range"))))
      .and(m -> m
        .setName("Path is $this")
        .setDescription("have $this as their path")
        .setWhere(new Where()
          .setIri(SHACL.PATH)
          .addIs(new Node().setParameter("this"))))
      .return_(r->r.setNodeRef("range").setIri(RDFS.LABEL));

  }

  private void dataModelPropertyByShape() throws JsonProcessingException {
    TTEntity query = getFormValidationEntity("DataModelPropertyByShape", "Data model property", "takes account of the data model shape that the property is part of")
      .set(IM.DEFINITION.asIri(), TTLiteral.literal(
        new Query()
          .setName("Data model property")
          .setDescription("get properties of property objects for specific data model and property")
          .and(m -> m
            .setName("Data model property")
            .setDescription("A given property ($myProperty) of a given data model ($myDataModel)")
            .addIs(new Node()
              .setParameter("myDataModel"))
            .addPath(new Path()
              .setIri(SHACL.PROPERTY.toString())
              .setName("Property $myProperty")
              .setDescription("Property $myProperty that exists on a data model (via a path)")
              .setNode("shaclProperty"))
            .setWhere(new Where()
              .setNodeRef("shaclProperty")
              .setIri(SHACL.PATH)
              .addIs(new Node().setParameter("myProperty"))))
          .setReturn(List.of(
            new Return()
              .setNodeRef("shaclProperty")
              .setIri(SHACL.CLASS)
              .setReturn(List.of(new Return()
                .setIri(RDFS.LABEL))),
            new Return()
              .setNodeRef("shaclProperty")
              .setIri(SHACL.NODE)
              .setReturn(List.of(new Return()
                .setIri(RDFS.LABEL))),
            new Return()
              .setNodeRef("shaclProperty")
              .setIri(SHACL.DATATYPE)
              .setReturn(List.of(new Return()
                .setIri(RDFS.LABEL))),
            new Return()
              .setNodeRef("shaclProperty")
              .setIri(SHACL.GROUP)
              .setReturn(List.of(new Return()
                .setIri(RDFS.LABEL))),
            new Return()
              .setNodeRef("shaclProperty")
              .setIri(SHACL.FUNCTION)
              .setReturn(List.of(new Return()
                .setIri(RDFS.LABEL))),
            new Return()
              .setNodeRef("shaclProperty")
              .setIri(SHACL.INVERSEPATH)
              .setReturn(List.of(new Return()
                .setIri(RDFS.LABEL))),
            new Return()
              .setNodeRef("shaclProperty")
              .setIri(SHACL.ORDER),
            new Return()
              .setNodeRef("shaclProperty")
              .setIri(SHACL.MAXCOUNT),
            new Return()
              .setNodeRef("shaclProperty")
              .setIri(SHACL.MINCOUNT)
          ))
      ));

    document.addEntity(query);
  }

  private void dataModelPropertyRange() throws JsonProcessingException {
    TTEntity query = getFormValidationEntity("DataModelPropertyRange", "Data model property range", "returns a flat list of data model property ranges based on input data model and property")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Data model property range")
          .setDescription("get node, class or datatype value (range)  of property objects for specific data model and property")
          .and(m -> m
            .setName("Data model property ranges")
            .setDescription("The range (node, class or datatype) of $myProperty on $myDataModel")
            .addIs(new Node()
              .setParameter("myDataModel"))
            .addPath(new Path()
              .setIri("http://www.w3.org/ns/shacl#property")
              .setName("Property range")
              .setDescription("The range (node, class or datatype) of $myProperty")
              .setNode("shaclProperty"))
            .where(and -> and
              .and(p2 -> p2
                .setNodeRef("shaclProperty")
                .setIri(SHACL.PATH)
                .is(in -> in
                  .setParameter("myProperty")))
              .and(p2 -> p2
                .setNodeRef("shaclProperty")
                .or(p3 -> p3
                  .setNodeRef("shaclProperty")
                  .setIri(SHACL.CLASS)
                  .setNode("propType"))
                .or(p3 -> p3
                  .setNodeRef("shaclProperty")
                  .setIri(SHACL.NODE)
                  .setNode("propType"))
                .or(p3 -> p3
                  .setNodeRef("shaclProperty")
                  .setIri(SHACL.DATATYPE)
                  .setNode("propType")))))
          .return_(r -> r
            .setNodeRef("propType")
            .setIri(RDFS.LABEL))));

    document.addEntity(query);
  }

  private void dataPropertyRangeSuggestions() throws JsonProcessingException {
    TTEntity query = getFormValidationEntity("dataPropertyRangeSuggestions", "Range suggestions for data property", "takes account of the data model shape that the property is part of")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        getRangeSuggestion()));
    document.addEntity(query);

  }




  private void getAncestors() throws JsonProcessingException {
    TTEntity query = getFormValidationEntity("GetAncestors", "Get active supertypes of concept", "returns transitive closure of an entity and its supertypes, usually used with a text search filter to narrow results");
    query.getPredicateMap().remove(TTIriRef.iri(NAMESPACE.IM + "query"));
    query.set(iri(IM.DEFINITION),
      TTLiteral.literal(new Query()
        .setName("All supert types of an entity, active only")
        .setActiveOnly(true)
        .setDescription("All super types of an entity (where the entity 'is a' $this)")
        .setNode("isa")
        .addIs(new Node()
          .setParameter("this")
          .setAncestorsOf(true))
        .return_(p -> p.setNodeRef("isa").setIri(RDFS.LABEL))
        .return_(p -> p.setNodeRef("isa").setIri(IM.CODE))));
  }

  private FunctionClause assignTimeDiff(String property,TTIriRef units) {
    FunctionClause function = new FunctionClause();
    function.setIri(NAMESPACE.IM+"DateTimeDifference");
    function.addArgument(new Argument()
      .setParameter("firstDate")
      .setValuePath(new Path().setIri(property)));
    function.addArgument(new Argument()
      .setParameter("secondDate")
      .setParameter("$searchDate"));
    function.addArgument(new Argument()
      .setParameter("units")
      .setValueIri(units));
    return function;
  }

  private void testQuery() throws IOException, EQDException {
    Where ageWhere = new Where();
    Value fromAge = new Value();
    fromAge.setOperator(Operator.gte)
      .setUnits(iri(IM.YEARS))
      .setValue("65");
    Value toAge = new Value();
    toAge
      .setOperator(Operator.lt)
      .setUnits(iri(IM.YEARS))
      .setValue("70");
    ageWhere
      .setIri(NAMESPACE.IM + "age")
      .setRange(new Range().setFrom(fromAge).setTo(toAge));
    Query query = new Query()
      .setTypeOf(NAMESPACE.IM + "Patient")
      .setIri(NAMESPACE.IM + "Q_TestQuery")
      .setName("Patients 65-70, or pre-diabetes that need invitations for blood pressure measuring");
    query
      .and(q -> q
        .is(is->is.setIri(NAMESPACE.IM + "Q_RegisteredGMS")
          .setIsCohort(true)
          .setName("Registered for GMS services on reference date")))
      .and(q -> q
        .or(m -> m
          .setTypeOf(NAMESPACE.IM + "Patient")
          .setDescription("aged between 65 and 70")
          .setWhere(ageWhere))
        .or(m -> m
          .setTypeOf(NAMESPACE.IM + "Condition")
          .setDescription("has pre-diabetes")
          .where(w -> w
            .setIri(IM.DATA_MODEL_PROPERTY_CONCEPT)
            .addIs(new Node().setIri(NAMESPACE.SNOMED + "714628002").setDescendantsOrSelfOf(true)))))
      .and(q -> q
        .setDescription("Latest systolic within 12 months of the search date is high")
        .setTypeOf(NAMESPACE.IM + "Observation")
        .where(and -> and
          .and(ww -> ww
            .setIri(IM.DATA_MODEL_PROPERTY_CONCEPT)
            .setName("concept")
            .addIs(new Node()
              .setIri(NAMESPACE.SNOMED + "271649006")
              .setDescendantsOrSelfOf(true)
              .setName("Systolic blood pressure"))
            .addIs(new Node()
              .setIri(NAMESPACE.EMIS + "1994021000006115")
              .setDescendantsOrSelfOf(true)
              .setName("Home systolic blood pressure")))
          .and(ww->ww
            .compare(c->c
              .left(l->l
                .setIri(NAMESPACE.IM + "effectiveDate"))
              .right(r->r
                .setParameter("$searchDate"))
              .setUnits(iri(IM.MONTHS)))
            .setOperator(Operator.gte)
            .setValue("-12")))
        .setOrderBy(new OrderLimit()
          .addProperty(new OrderDirection()
            .setIri(NAMESPACE.IM + "effectiveDate")
            .setDirection(Order.descending))
          .setLimit(1))
        .then(then->then
          .or(whereEither -> whereEither
            .and(w1 -> w1
              .setIri(NAMESPACE.IM + "concept")
              .addIs(new Node()
                .setIri(NAMESPACE.SNOMED + "271649006")
                .setDescendantsOrSelfOf(true)
                .setName("Systolic blood pressure")))
            .and(w1 -> w1
              .setIri(NAMESPACE.IM+"value")
              .setOperator(Operator.gt)
              .setValue("140")))
          .or(whereOr -> whereOr
            .and(w1 -> w1
              .setIri(NAMESPACE.IM+"concept")
              .addIs(new Node()
                .setIri(NAMESPACE.EMIS + "1994021000006115")
                .setDescendantsOrSelfOf(true)
                .setName("Home systolic blood pressure")))
            .and(w1 -> w1
              .setIri(NAMESPACE.IM+"value")
              .setOperator(Operator.gt)
              .setValue("130"))))
        .setNode("HighBPReading")
        .return_(r->r
          .as("date")
          .setIri(NAMESPACE.IM + "effectiveDate")))
      .and(q ->q
        .setName("Invited for screening after high BP reading")
        .setNotExists(true)
        .setDescription("Already invited for screening with an effective date after the effective date of the high BP reading")
        .setNodeRef("HighBPReading")
        .setTypeOf(NAMESPACE.IM + "Procedure")
        .where(and -> and
          .and(inv -> inv
            .setIri(IM.DATA_MODEL_PROPERTY_CONCEPT)
            .addIs(new Node().setIri("http://snomed.info/sct#310422005").setName("invited for screening").setMemberOf(true)))
          .and(inv->inv
            .setOperator(Operator.gte)
            .compare(c->c
              .left(l->l
                .setIri(NAMESPACE.IM + "effectiveDate"))
              .right (r->r
                .setNodeRef("HighBPReading")
                .setIri(NAMESPACE.IM + "effectiveDate").setPropertyRef("date"))))))
      .and(q -> q
        .setNotExists(true)
        .setName("on hypertension register")
        .setDescription("is registered on the hypertensives register")
        .is(is->is.setIri("http://endhealth.info/qof#37d6ee71-b642-407c-be92-cbc924013387")
          .setName("Hypertensives")));

    TTEntity qry = new TTEntity().addType(iri(IM.QUERY))
      .set(iri(IM.RETURN_TYPE), TTIriRef.iri(NAMESPACE.IM + "Patient"))
      .setIri(NAMESPACE.IM + "Q_TestQuery")
      .setName("Patients 65-70, or diabetes or prediabetes that need invitations for blood pressure measuring")
      .setDescription("Test for patients either aged between 65 and 70 or with diabetes with the most recent systolic in the last 12 months either home >130 or office >140, not followed by a screening invite, excluding hypertensives")
      .setScheme(NAMESPACE.IM.asIri())
      .set(iri(IM.DEFINITION), TTLiteral.literal(query))
      .addObject(iri(IM.DEPENDENT_ON), TTIriRef.iri(NAMESPACE.IM + "Q_RegisteredGMS"))
      .addObject(iri(IM.DEPENDENT_ON), TTIriRef.iri("http://endhealth.info/qof#37d6ee71-b642-407c-be92-cbc924013387"))
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(NAMESPACE.IM + "Q_StandardCohorts"));

    document.addEntity(qry);
  }

  private void deleteSets() throws JsonProcessingException {
    TTEntity entity = new TTEntity()
      .setIri(NAMESPACE.IM + "DeleteSets")
      .setName("Delete all concept sets in a scheme")
      .setDescription("Pass in the graph name as a 'this' argument and it deletes all sets")
      .setScheme(NAMESPACE.IM.asIri())
      .set(iri(IM.UPDATE_PROCEDURE), TTLiteral.literal(new Update()
        .match(m -> m
          .where(w->w
            .and(w1->w1
              .setIri(IM.HAS_SCHEME.toString())
              .is(is->is.setParameter("this")))
            .and(w1->w1
              .setIri(RDF.TYPE.toString())
              .is(is->is.setIri(IM.CONCEPT_SET.toString())))))
        .addDelete(new Delete())));

    document.addEntity(entity);
  }

  private void currentGMS() throws JsonProcessingException {


    TTEntity qry = new TTEntity()
      .addType(iri(IM.QUERY))
      .setScheme(NAMESPACE.IM.asIri())
      .set(iri(IM.RETURN_TYPE), TTIriRef.iri(NAMESPACE.IM + "Patient"))
      .set(iri(IM.USAGE_TOTAL), TTLiteral.literal(10000))
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(NAMESPACE.IM + "Q_StandardCohorts"))
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(NAMESPACE.IM + "Q_DefaultCohorts"))
      .set(iri(SHACL.ORDER), TTLiteral.literal(1))
      .setIri(NAMESPACE.IM + "Q_RegisteredGMS")
      .set(iri(IM.ALTERNATIVE_CODE), TTLiteral.literal("RegisteredAsGMS"))
      .setName("Patients registered for GMS services on the reference date")
      .setDescription("For any gpRegistration period,a gpRegistration start date before the reference date and no end date, or an end date after the reference date.");

    qry.set(iri(IM.DEFINITION), TTLiteral.literal(getGmsIsRegistered()));
    document.addEntity(qry);
  }


  private void getSearchAll() throws JsonProcessingException {
    getFormValidationEntity("SearchmainTypes", "Search for entities of the main types", "used to filter free text searches excluding queries and concept sets")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setActiveOnly(true)
          .setName("Search for all main types")
          .setDescription("Search for Concepts, Concept Sets, Folders, Value Sets or Data Model Properties")
          .and(f -> f
            .or(w -> w
              .setName("Concepts")
              .setDescription("Type is Concept")
              .setTypeOf(IM.CONCEPT.toString()))
            .or(w -> w
              .setName("Concept sets")
              .setDescription("Type is Concept Set")
              .setTypeOf(IM.CONCEPT_SET.toString()))
            .or(w -> w
              .setName("Folders")
              .setDescription("Type is Folder")
              .setTypeOf(IM.FOLDER.toString()))
            .or(w -> w
              .setName("Value Sets")
              .setDescription("Type is Value Set")
              .setTypeOf(IM.VALUESET.toString()))
            .or(w -> w
              .setName("Data model property")
              .setDescription("Type is Data Model Property")
              .setTypeOf(NAMESPACE.IM + "dataModelProperty")))
          .return_(p -> p.setIri(RDFS.LABEL))
          .return_(p -> p.setIri(RDFS.COMMENT))
          .return_(p -> p.setIri(IM.CODE))
          .return_(p -> p.setIri(IM.HAS_STATUS)
            .return_(rp -> rp.setIri(RDFS.LABEL)))
          .return_(p -> p.setIri(IM.HAS_SCHEME)
            .return_(rp -> rp.setIri(RDFS.LABEL)))
          .return_(p -> p.setIri(RDF.TYPE)
            .return_(rp -> rp.setIri(RDFS.LABEL)))
          .return_(p -> p.setIri(IM.USAGE_TOTAL))
      ));
  }
  private void allowableSubTypes() throws JsonProcessingException {
    Query query = new Query()
      .setName("Allowable subtypes for a particular entity")
      .setDescription("pass 'this' as the iri for the selected entity e.g. {'iri': 'http://sometype'}")
      .or(m -> m
        .where(w -> w
          .setInverse(true)
          .setIri(RDF.TYPE)
          .is(is -> is.setParameter("this"))))
      .or(m -> m
        .where(w -> w
          .setInverse(true)
          .setIri(IM.CONTENT_TYPE)
          .is(is -> is.setParameter("this"))))
      .or(m->m
        .and(m1->m1
          .where(w->w
            .setSubjectParameter("this")
            .setIsNull(true)
            .setIri(IM.CONTENT_TYPE)))
        .and(m1->m1
          .where(w->w
            .setIri(IM.IS_CONTAINED_IN)
            .is(is->is.setIri(NAMESPACE.IM+"EntityTypes")))))
      .return_(p -> p
        .setIri(RDFS.LABEL))
      .return_(p -> p
        .setIri(SHACL.PROPERTY)
        .return_(p1 -> p1
          .setIri(SHACL.PATH)));
    getFormValidationEntity("AllowableChildTypes", "Allowable child types for editor", "used in the editor to select the type of entity being created as a subtype")
      .set(iri(IM.DEFINITION), TTLiteral.literal(query));
  }

  private void isAllowableRange() throws JsonProcessingException {
    getFormValidationEntity("IsAllowableRange", "Is an entity an allowable range a particular property", "uses inverse range property to check the ranges of the property as authored. Should be used with another ")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Is an entity an allowable range a particular property")
          .setActiveOnly(true)
          .is(ins -> ins
            .setDescendantsOrSelfOf(true)
            .setParameter("ranges"))
          .return_(p -> p.setIri(RDFS.LABEL))
          .return_(p -> p.setIri(RDF.TYPE))
          .return_(p -> p.setIri(IM.HAS_SCHEME))
          .return_(p -> p.setIri(IM.HAS_TERM_CODE)
            .return_(p1 -> p1.setIri(RDFS.LABEL)))));
  }

  private void isValidProperty() throws JsonProcessingException {
    getFormValidationEntity("IsValidProperty", "is a valid property", "is the property a valid value for the concept(s)")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Is it a valid property")
          .setDescription("is the property 'property' a valid value for the concept(s) 'concepts")
          .setActiveOnly(true)
          .is(i -> i.setParameter("concept").setDescendantsOrSelfOf(true))
          .path(p->p
            .setIri(IM.ROLE_GROUP)
            .setNode("roleGroup"))
          .setWhere(new Where()
            .setNodeRef("roleGroup")
            .setParameter("entity")
            .setNode("value")
          )));
  }

  private void isValidType() throws JsonProcessingException {
    getFormValidationEntity("IsValidType", "is a valid type", "is the entity a valid type")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Is '$entity' a valid $type")
          .setDescription("is the type of the selecvted entity a type of allowed types")
          .setParameter("entity")
          .setWhere(new Where()
            .setIri(RDF.TYPE)
            .is(is -> is.setParameter("type"))
          )));
  }
  private void isValidDescendant() throws JsonProcessingException {
    getFormValidationEntity("IsValidDescendant", "is a valid descendant", "is the concept a valid descendant of some parent")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Is it a valid property")
          .setDescription("is the property 'property' a valid value for the concept(s) 'concepts")
          .setActiveOnly(true)
          .where(w->w
            .setIri(IM.IS_A.toString())
            .is(i -> i.setParameter("parent"))
          )));
  }

  private void entityFilter() throws JsonProcessingException {
    getFormValidationEntity("EntityFilter", "Entity filter", "Parameterised list of entities to filter on a text search")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Entity filter")
          .where(w->w
            .setIri(IM.IRI)
            .is(is->is
              .setParameter("entities")))));

  }

  private void getAllowableProperties() throws JsonProcessingException {
    getFormValidationEntity("AllowableProperties", "Properties that have been used for subtypes of a terminology concept", "Returns a list of properties for a particular term concept")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Properties that can be used for a concept {this}")
          .addIs(new Node().setParameter("this").setAncestorsOf(true))
          .where(w->w
            .and(w1->w1
              .setInverse(true)
              .setIri(RDFS.DOMAIN.toString())
              .setNode("property"))
            .and(w1->w1
              .setExists(true)
              .setSubjectVariable("roleGroup")
              .setPropertyVariable("property")
              .setNode("value")))
          .return_(p -> p.setNodeRef("property"))));
  }


  private void getAllowablePropertyAncestors() throws JsonProcessingException {
    getFormValidationEntity("AllowablePropertyAncestors", "Allowable properties for a terminology concept", "Returns a list of properties for a particular term concept, used in value set definitions with RCL")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setTypeOf(RDF.PROPERTY.toString())
          .setName("Allowable Properties for a terminology concept")
          .setDescription("Allowable Properties for a terminology concept")
          .setActiveOnly(true)
          .setName("property that has $this (or supertype) as a domain")
          .setDescription("property that has $this (or supertype) as a domain")
          .setNode("concept")
          .setWhere(new Where()
            .setIri(RDFS.DOMAIN)
            .addIs(new Node().setParameter("this").setAncestorsOf(true))
          )
          .return_(p -> p.setIri(RDFS.LABEL))
          .return_(p -> p.setIri(RDF.TYPE))));
  }


  private void getConcepts() throws JsonProcessingException {
    getFormValidationEntity("SearchEntities", "Search for entities of a certain type", "parameter 'this' set to the list of type iris, Normally used with a text search entry to filter the list")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setActiveOnly(true)
          .setName("Search for concepts of $this type")
          .setDescription("Search for concepts")
          .setDescription("of type $this")
          .setTypeOf(new Node()
            .setParameter("this"))
          .return_(p -> p.setIri(RDFS.LABEL))
          .return_(p -> p.setIri(RDF.TYPE))))
      .getPredicateMap().remove(TTIriRef.iri(NAMESPACE.IM + "query"));
  }

  private void searchFolders() throws JsonProcessingException {
    getFormValidationEntity("SearchFolders", "Search for folder by name", "Returns a list of folder using a text search")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Search for folders by name")
          .setDescription("Search for folders by name")
          .setActiveOnly(true)
          .setName("of type Folder")
          .setDescription("of type Folder")
          .setNode("folder")
          .setTypeOf(IM.FOLDER.toString())
          .return_(p -> p.setNodeRef("folder").setIri(RDFS.LABEL))
          .return_(p -> p.setNodeRef("folder").setIri(RDF.TYPE))));
  }

  private void searchContainedIn() throws JsonProcessingException {
    getFormValidationEntity("SearchContainedIn", "Search for entities contained in parent folder", "parameter 'value' needs to be set to the parent folder")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
          new Query()
            .setName("Search for entities contained in parent folder")
            .setDescription("Search for entities contained in parent folder $value")
            .setActiveOnly(true)
            .setName("Contained in folder $value")
            .setDescription("Contained in $value")
            .setWhere(new Where()
              .setIri(IM.IS_CONTAINED_IN)
              .is(i -> i
                .setParameter("value")
              )
            )
        )
      );
  }

  private void searchAllowableSubclass() throws JsonProcessingException {
    getFormValidationEntity("SearchAllowableSubclass", "Search for allowable subclasses", "parameter 'value' needs to be set to current entity type")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
          new Query()
            .setName("Search for allowable subclasses")
            .setDescription("Search for allowable subclasses")
            .setActiveOnly(true)
            .setName("Subtypes of $value")
            .setDescription("Subtypes (i.e. 'Is a') of $value")
            .setWhere(new Where()
              .setIri(RDF.TYPE)
              .is(i -> i
                .setParameter("value")
              )
            )
        )
      );
  }

  private void searchAllowableChildOf() throws JsonProcessingException {
    getFormValidationEntity("SearchAllowableChildOf", "Search for allowable parents of a child query", "parameter 'value' needs to be set to current entity type")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
          new Query()
            .setName("Search for allowable shild of")
            .setDescription("Search for allowable child of parents")
            .setActiveOnly(true)
            .setName("Child of of $value")
            .setDescription("Same type of  $value")
            .setWhere(new Where()
              .setIri(RDF.TYPE)
              .is(i -> i
                .setParameter("value")
              )
            )
        )
      );
  }

  private void searchAllowableContainedIn() throws JsonProcessingException {
    getFormValidationEntity("SearchAllowableContainedIn", "Search for allowable parent folder", "parameter 'value' needs to be set to the current entity type")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Search for allowable contained in")
          .setDescription("Search for allowable contained in")
          .setActiveOnly(true)
          .setDescription("Folders with no content type, or content type $value")
          .setNode("folder")
          .setTypeOf(IM.FOLDER.toString())
          .setWhere(new Where()
            .or(p -> p
              .and(p1->p1
                .setIri(IM.CONTENT_TYPE)
                .setIsNull(true))
              .and(p1->p1
                .setIri(RDF.TYPE)
                .is(is->is.setIri(IM.FOLDER.toString()))))
            .or(p -> p
              .setIri(IM.CONTENT_TYPE)
              .is(i -> i.setParameter("value")))

          )
          .return_(p -> p.setNodeRef("folder").setIri(RDFS.LABEL))
          .return_(p -> p.setNodeRef("folder").setIri(RDF.TYPE))));
  }

  private void getDescendants() throws JsonProcessingException {
    getFormValidationEntity("GetDescendants", "Get active subtypes of concept", "returns transitive closure of an entity and its subtypes, usually used with a text search filter to narrow results")
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(new Query()
          .setName("All subtypes of an entity $this , active only")
          .setDescription("All subtypes of an entity, active only")
          .setActiveOnly(true)
          .setDescription("Is a descendant of, or $this")
          .setNode("isa")
          .addIs(new Node()
            .setParameter("this")
            .setDescendantsOrSelfOf(true))
          .return_(p -> p.setNodeRef("isa").setIri(RDFS.LABEL))
          .return_(p -> p.setNodeRef("isa").setIri(IM.CODE))))
      .getPredicateMap().remove(TTIriRef.iri(NAMESPACE.IM + "query"));
  }

  private void getSubclasses() throws JsonProcessingException {
    getFormValidationEntity("GetSubClasses", "Get active subclasses of entity", "returns all subclasses of an entity, active only, used with Creator/Editor to get Status subclasses")
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(new Query()
          .setName("All subclasses of an entity $this, active only")
          .setDescription("All subclasses of an entity, active only")
          .setActiveOnly(true)
          .setDescription("Is a subclass of")
          .setNode("subclass")
          .where(w -> w
            .setIri(RDFS.SUBCLASS_OF)
            .is(i -> i
              .setParameter("this")))
          .return_(p -> p.setNodeRef("subclass").setIri(RDFS.LABEL))
          .return_(p -> p.setNodeRef("subclass").setIri(IM.CODE))))
      .getPredicateMap().remove(TTIriRef.iri(NAMESPACE.IM + "query"));
  }

  private void getSubsets() throws JsonProcessingException {
    getFormValidationEntity("GetSubsets", "Get subsets using superset iri", "return items which have a isSubsetOf predicate linked to the iri provided")
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(new Query()
          .setName("All subsets of an entity, active only")
          .setActiveOnly(true)
          .return_(s -> s.setIri(IM.CODE))
          .return_(s -> s.setIri(RDFS.LABEL))
          .setDescription("Is a subset of $this")
          .setWhere(new Where()
            .setIri(IM.IS_SUBSET_OF)
            .addIs(new Node().setParameter("this")
            )
          )
        )
      )
      .getPredicateMap().remove(TTIriRef.iri(NAMESPACE.IM + "query"));
  }

  private TTEntity getFormValidationEntity(String iri, String name, String comment) {
    TTEntity entity = new TTEntity()
      .setIri(NAMESPACE.IM + "Query_" + iri)
      .setName(name)
      .setDescription(comment)
      .addType(iri(IM.QUERY))
      .setScheme(NAMESPACE.IM.asIri())
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(NAMESPACE.IM + "IMFormValidationQueries"));
    document.addEntity(entity);
    return entity;
  }
  private TTEntity getColumnGroupEntity(String iri, String name, String comment) {
    TTEntity entity = new TTEntity()
      .setIri(NAMESPACE.IM + "CG_" + iri)
      .setName(name)
      .setDescription(comment)
      .addType(iri(IM.QUERY))
      .setScheme(NAMESPACE.IM.asIri())
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(NAMESPACE.IM + "ColumnGroups"));
    document.addEntity(entity);
    return entity;
  }

  @Override
  public void validateFiles(String inFolder) throws TTFilerException {
    // No files to validate
  }


  @Override
  public void close() throws Exception {

  }
}
