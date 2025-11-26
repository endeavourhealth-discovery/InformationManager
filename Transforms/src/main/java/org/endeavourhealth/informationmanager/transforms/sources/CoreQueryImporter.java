package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.logic.reasoner.IndicatorGenerator;
import org.endeavourhealth.imapi.model.customexceptions.EQDException;
import org.endeavourhealth.imapi.queryengine.ClauseUtils;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;

import java.io.IOException;
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


      age();
      gmsRegistration();
      gmsRegistrationStatus();
      gmsRegisteredPractice();
      getDescendants();
      getSubclasses();
      getConcepts();
      getAllowableProperties();
      getAllowablePropertyAncestors();
      isValidProperty();
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
      searchProperties();
      dataModelPropertyRange();
      dataModelPropertyByShape();
      searchFolders();
      searchContainedIn();
      searchAllowableSubclass();
      searchAllowableContainedIn();
      generateDefaultCohorts(manager);
      generateDefaultIndicators(manager);
      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
        filer.fileDocument(document);
      }
    } catch (Exception e) {
      throw new ImportException(e.getMessage(), e);
    }
  }
  private void generateDefaultIndicators(TTManager manager) throws JsonProcessingException {
    TTEntity defaults= new TTEntity()
      .setIri(Namespace.IM+"StandardIndicators")
      .setName("Standard indicators")
      .addType(iri(IM.FOLDER))
      .setScheme(iri(Namespace.IM))
      .addObject(iri(IM.IS_CONTAINED_IN), iri(Namespace.IM+"Indicators"));
    defaults.set(IM.ORDER,TTLiteral.literal(1));
    manager.getDocument().addEntity(defaults);
    IndicatorGenerator generator= new IndicatorGenerator();
    TTEntity GMSIndicator= generator.createIndicator(Namespace.IM+"GMSIndicator"
    ,"GMS Registered patients (indicator)"
        ,"The indicator for GMS registered patients used by most patient indicators"
    ,Namespace.IM,
      null,
     Namespace.IM+"Q_RegisteredGMS",
      null);
    GMSIndicator.set(iri(IM.IS_CONTAINED_IN), iri(Namespace.IM+"StandardIndicators"));
    manager.getDocument().addEntity(GMSIndicator);
  }

  private void generateDefaultCohorts(TTManager manager) throws JsonProcessingException {
    TTEntity gms = manager.getEntity(Namespace.IM + "Q_RegisteredGMS");
    gms.addObject(TTIriRef.iri(IM.IS_CONTAINED_IN), TTVariable.iri(Namespace.IM + "Q_DefaultCohorts"));
    gms.addObject(iri(IM.CONTEXT_ORDER), new TTNode().set(SHACL.ORDER, TTLiteral.literal(1))
      .set(IM.CONTEXT, TTIriRef.iri(Namespace.IM + "Q_DefaultCohorts")));
    int order = 1;
    for (String defaultFolder : List.of("Patient", "PeopleAndThings", "ClinicalInformation", "PersonalHealthManagement", "ProcessOfCare", "Q_Queries")) {
      order++;
      addToDefaults(defaultFolder, manager, order);
    }
  }

  private void addToDefaults(String defaultEntity, TTManager manager, int order) {
    TTEntity entity = new TTEntity()
      .setIri(Namespace.IM + defaultEntity)
      .setScheme(Namespace.IM.asIri())
      .setCrud(iri(IM.ADD_QUADS));
    entity.addObject(TTVariable.iri(IM.IS_CONTAINED_IN), TTVariable.iri(Namespace.IM + "Q_DefaultCohorts"));
    entity.addObject(iri(IM.CONTEXT_ORDER), new TTNode().set(SHACL.ORDER, TTLiteral.literal(order)).set(IM.CONTEXT, TTIriRef.iri(Namespace.IM + "Q_DefaultCohorts")));
    manager.getDocument().addEntity(entity);
  }


  private void gmsRegisteredPractice() throws JsonProcessingException {
    TTEntity gms = new TTEntity()
      .setIri(Namespace.IM + "gmsRegisteredPractice")
      .setDescription("Returns the practice if the patient is registered as a GMS patient on the reference date")
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .setScheme(Namespace.IM.asIri())
      .addObject(iri(SHACL.PARAMETER), new TTNode()
        .set(iri(RDFS.LABEL), TTLiteral.literal("searchDate"))
        .set(iri(SHACL.DATATYPE), iri(Namespace.IM + "DateTime")));
    Query query = getGmsIsRegistered();
    query
      .orderBy(o -> o.addProperty(new OrderDirection().setNodeRef("RegistrationEpisode").setIri(Namespace.IM + "effectiveDate").setDirection(Order.descending)).setLimit(1));
    query.return_(r -> r.property(p -> p
      .setNodeRef("RegistrationEpisode")
      .setIri(Namespace.IM + "provider")));
    query.setName("GMS registered practice");
    gms.set(iri(IM.DEFINITION), TTLiteral.literal(query));
    document.addEntity(gms);
  }

  private Query getGmsIsRegistered() {
    return new Query()
      .setName("Patient registered as GMS on the reference date")
      .setDescription("Is the patient registered as a GMS patient on the reference date?")
      .setTypeOf(Namespace.IM + "Patient")
      .and(m -> m
        .path(p -> p
          .setIri(Namespace.IM + "episodeOfCare")
          .setTypeOf(Namespace.IM + "EpisodeOfCare")
          .setVariable("RegistrationEpisode"))
        .where(w -> w
          .and(pv -> pv
            .setNodeRef("RegistrationEpisode")
            .setIri(Namespace.IM + "gmsPatientType")
            .addIs(new Node().setIri("http://hl7.org/fhir/registration-type/r").setName("Regular GMS patient")))
          .and(pv -> pv
            .setNodeRef("RegistrationEpisode")
            .setIri(Namespace.IM + "effectiveDate")
            .setOperator(Operator.lte)
            .setRelativeTo(new RelativeTo().setParameter("$searchDate")))
          .and(pv -> pv
            .setNodeRef("RegistrationEpisode")
            .or(pv1 -> pv1
              .setNodeRef("RegistrationEpisode")
              .setIri(Namespace.IM + "endDate")
              .setIsNull(true))
            .or(pv1 -> pv1
              .setNodeRef("RegistrationEpisode")
              .setIri(Namespace.IM + "endDate")
              .setOperator(Operator.gt)
              .setRelativeTo(new RelativeTo().setParameter("$searchDate"))))));


  }


  private void gmsRegistration() throws JsonProcessingException {
    TTEntity gms = new TTEntity()
      .setIri(Namespace.IM + "gmsRegistrationAtEvent")
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .setScheme(Namespace.IM.asIri())
      .addObject(iri(SHACL.PARAMETER), new TTNode()
        .set(iri(RDFS.LABEL), TTLiteral.literal("searchDate"))
        .set(iri(SHACL.DATATYPE), iri(Namespace.IM + "DateTime")))
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(getGmsQuery()));

    document.addEntity(gms);
  }

  private Query getGmsQuery() {
    return new Query()
      .setName("GP GMS registration at a reference date")
      .setDescription("Retrieves the Registration status of active, left or died")
      .setTypeOf(Namespace.IM + "Patient")
      .and(m -> m
        .where(w -> w
          .setIri(Namespace.IM + "gmsRegistrationStatus")
          .is(is -> is
            .setIri(Namespace.IM + "CaseloadStatusActive"))));
  }

  private void gmsRegistrationStatus() throws JsonProcessingException {
    Query query = getGmsQuery();
    query.setName("Returns the gpRegistration status of a patient if they are currently registered as a regular GMS patient, or if died");
    query.setVariable("currentEpisode");
    Return ret = new Return();
    query.setReturn(ret);
    ReturnProperty returnProperty = new ReturnProperty();
    ret.addProperty(returnProperty);
    returnProperty.case_(c -> c
      .when(when -> when
        .where(w -> w
          .or(w1 -> w1
            .setIri(Namespace.IM + "dateOfDeath")
            .setIsNull(true))
          .or(w1 -> w1
            .setIri(Namespace.IM + "dateOfDeath")
            .setOperator(Operator.lt)
            .relativeTo(r -> r.setParameter("searchDate"))))
        .setThen(Namespace.IM + "CaseloadStatusDead"))
      .when(when -> when
        .where(pv -> pv
          .or(pv1 -> pv1
            .setNodeRef("currentEpisode")
            .setIri(Namespace.IM + "endDate")
            .setIsNull(true))
          .or(pv1 -> pv1
            .setNodeRef("currentEpisode")
            .setIri(Namespace.IM + "endDate")
            .setOperator(Operator.gt)
            .setRelativeTo(new RelativeTo().setParameter("$searchDate"))))
        .setThen(Namespace.IM + "CaseloadStatusActive"))
      .setElse(Namespace.IM + "CaseloadStatusLeft"));

    TTEntity gms = new TTEntity()
      .setIri(Namespace.IM + "gmsRegistrationStatus")
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .setScheme(Namespace.IM.asIri())
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(query));
    document.addEntity(gms);

  }


  private void addressProperty(String propertyName, String value) throws JsonProcessingException {
    TTEntity address = new TTEntity()
      .setIri(Namespace.IM + propertyName)
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .setScheme(Namespace.IM.asIri())
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName(value + " address property definition")
          .path(p -> p.setIri(Namespace.IM + propertyName)
            .setVariable("Address")
            .setTypeOf(Namespace.IM + "AssignedAddress"))
          .where(and -> and
            .and(w -> w
              .setNodeRef("Address")
              .setIri(Namespace.IM + "effectiveDate")
              .setOperator(Operator.lte)
              .relativeTo(r -> r.setParameter("$indexDate")))
            .and(w -> w
              .or(or->or
                .setNodeRef("Address")
                .setIri(Namespace.IM + "endDate")
                .setIsNull(true))
              .or(or -> or
                .setNodeRef("Address")
                .setIri(Namespace.IM + "endDate")
                .setOperator(Operator.gt)
                .relativeTo(r -> r.setParameter("$indexDate"))))
            .and(w -> w
              .setNodeRef("Address")
              .setIri(Namespace.IM + "addressUse")
              .is(is -> is.setIri("http://hl7.org/fhir/fhir-address-use/" + value))))
          .orderBy(ob -> ob.addProperty(new OrderDirection().setNodeRef("Address").setIri(Namespace.IM + "effectiveDate").setDirection(Order.descending)).setLimit(1))));
    document.addEntity(address);


  }

  private void telephoneProperty(String propertyName, String value) throws JsonProcessingException {
    TTEntity address = new TTEntity()
      .setIri(Namespace.IM + propertyName)
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .setScheme(Namespace.IM.asIri())
      .set(iri(IM.DEFINITION), TTLiteral.literal(new Query()
        .setName(value + " telephone property definition")
        .path(p -> p.setIri(Namespace.IM + propertyName)
          .setVariable("Telephone")
          .setTypeOf(Namespace.IM + "TelephoneNumber"))
        .where(and -> and
          .and(w -> w
            .setNodeRef("Telephone")
            .setIri(IM.STATUS)
            .is(is -> is.setIri(IM.ACTIVE.toString())))
          .and(w -> w
            .setNodeRef("Telephone")
            .setIri(Namespace.IM + "use")
            .is(is -> is.setIri("http://hl7.org/fhir/contact-point-use/" + value))))
        .orderBy(o -> o.addProperty(new OrderDirection().setNodeRef("Telephone").setIri(Namespace.IM + "effectiveDate").setDirection(Order.descending)).setLimit(1))));
    document.addEntity(address);

  }

  private void age() throws JsonProcessingException {
    TTEntity age = new TTEntity()
      .setIri(Namespace.IM + "age")
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .setScheme(Namespace.IM.asIri())
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(new Query()
          .setName("Age function")
          .where(w -> w
            .setIri(Namespace.IM + "dateOfBirth")
            .setIsNotNull(true)
            .setValueVariable("dateOfBirth"))
        ));
    document.addEntity(age);
  }


  private void objectPropertyRangeSuggestions() throws JsonProcessingException {
    TTEntity query = getQuery("ObjectPropertyRangeSuggestions", "Range suggestions for object property", "takes account of the data model shape that the property is part of")
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
            .setValueVariable("range"))
          .or(p -> p
            .setIri(SHACL.DATATYPE)
            .setValueVariable("range"))
          .or(p -> p
            .setIri(SHACL.CLASS)
            .setValueVariable("range"))))
      .and(m -> m
        .setName("Path is $this")
        .setDescription("have $this as their path")
        .setWhere(new Where()
          .setIri(SHACL.PATH)
          .addIs(new Node().setParameter("this"))))
      .return_(r -> r.property(p -> p.setValueRef("range").setIri(RDFS.LABEL)));

  }

  private void dataModelPropertyByShape() throws JsonProcessingException {
    TTEntity query = getQuery("DataModelPropertyByShape", "Data model property", "takes account of the data model shape that the property is part of")
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
              .setVariable("shaclProperty"))
            .setWhere(new Where()
              .setNodeRef("shaclProperty")
              .setIri(SHACL.PATH)
              .addIs(new Node().setParameter("myProperty"))))
          .return_(r -> r.
            setNodeRef("shaclProperty")
            .setProperty(List.of(
              new ReturnProperty()
                .setIri(SHACL.CLASS)
                .setReturn(new Return().setProperty(List.of(new ReturnProperty()
                  .setIri(RDFS.LABEL)))),
              new ReturnProperty()
                .setIri(SHACL.NODE)
                .setReturn(new Return().setProperty(List.of(new ReturnProperty()
                  .setIri(RDFS.LABEL)))),
              new ReturnProperty()
                .setIri(SHACL.DATATYPE)
                .setReturn(new Return().setProperty(List.of(new ReturnProperty()
                  .setIri(RDFS.LABEL)))),
              new ReturnProperty()
                .setIri(SHACL.GROUP)
                .setReturn(new Return().setProperty(List.of(new ReturnProperty()
                  .setIri(RDFS.LABEL)))),
              new ReturnProperty()
                .setIri(SHACL.FUNCTION)
                .setReturn(new Return().setProperty(List.of(new ReturnProperty()
                  .setIri(RDFS.LABEL)))),
              new ReturnProperty()
                .setIri(SHACL.INVERSEPATH)
                .setReturn(new Return().setProperty(List.of(new ReturnProperty()
                  .setIri(RDFS.LABEL)))),
              new ReturnProperty()
                .setIri(SHACL.ORDER),
              new ReturnProperty()
                .setIri(SHACL.MAXCOUNT),
              new ReturnProperty()
                .setIri(SHACL.MINCOUNT)
            ))
          )));

    document.addEntity(query);
  }

  private void dataModelPropertyRange() throws JsonProcessingException {
    TTEntity query = getQuery("DataModelPropertyRange", "Data model property range", "returns a flat list of data model property ranges based on input data model and property")
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
              .setVariable("shaclProperty"))
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
                  .setValueVariable("propType"))
                .or(p3 -> p3
                  .setNodeRef("shaclProperty")
                  .setIri(SHACL.NODE)
                  .setValueVariable("propType"))
                .or(p3 -> p3
                  .setNodeRef("shaclProperty")
                  .setIri(SHACL.DATATYPE)
                  .setValueVariable("propType")))))
          .return_(r -> r
            .setNodeRef("propType")
            .property(p -> p
              .setIri(RDFS.LABEL)))));

    document.addEntity(query);
  }

  private void dataPropertyRangeSuggestions() throws JsonProcessingException {
    TTEntity query = getQuery("dataPropertyRangeSuggestions", "Range suggestions for data property", "takes account of the data model shape that the property is part of")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        getRangeSuggestion()));
    document.addEntity(query);

  }


  private void getAncestors() throws JsonProcessingException {
    TTEntity query = getQuery("GetAncestors", "Get active supertypes of concept", "returns transitive closure of an entity and its supertypes, usually used with a text search filter to narrow results");
    query.getPredicateMap().remove(TTIriRef.iri(Namespace.IM + "query"));
    query.set(iri(IM.DEFINITION),
      TTLiteral.literal(new Query()
        .setName("All supert types of an entity, active only")
        .setActiveOnly(true)
        .setDescription("All super types of an entity (where the entity 'is a' $this)")
        .setVariable("isa")
        .addIs(new Node()
          .setParameter("this")
          .setAncestorsOf(true))
        .return_(s -> s.setNodeRef("isa")
          .property(p -> p.setIri(RDFS.LABEL))
          .property(p -> p.setIri(IM.CODE)))));
  }


  private void testQuery() throws IOException, EQDException {
    Where ageWhere = new Where();
    Value fromAge = new Value();
    Where bpLast6Months= new Where();
    fromAge.setOperator(Operator.gte)
      .setValue("65");
    Value toAge = new Value();
    toAge
      .setOperator(Operator.lt)
      .setValue("70");
    ageWhere
      .setIri(Namespace.IM + "age")
      .setUnits(iri(IM.YEARS))
      .range(r -> r
        .setFrom(fromAge)
        .setTo(toAge));
    bpLast6Months.setNodeRef("Observation")
      .setIri(Namespace.IM + "effectiveDate")
      .setOperator(Operator.gte)
      .setValue("-12")
      .setUnits(iri(IM.MONTHS))
      .relativeTo(r -> r.setParameter("$searchDate"))
      .setValueLabel("last 12 months");

    Where relativeWhere = new Where();
    relativeWhere.setNodeRef("Observation")
      .setIri(Namespace.IM + "effectiveDate")
      .setOperator(Operator.gte)
      .relativeTo(r -> r.setNodeRef("highBPReading").setIri(Namespace.IM + "effectiveDate"));
    ClauseUtils.assignFunction(ageWhere);
    ClauseUtils.assignFunction(relativeWhere);
    ClauseUtils.assignFunction(bpLast6Months);
    Query query = new Query()
      .setIri(Namespace.IM + "Q_TestQuery")
      .setName("Patients 65-70, or diabetes or prediabetes that need invitations for blood pressure measuring");
    query
      .return_(r->r.property(p -> p.setIri(Namespace.IM+"patient")))
      .is(is->is.setIri(Namespace.IM + "Q_RegisteredGMS")
        .setIsCohort(true)
        .setName("Registered for GMS services on reference date"))
      .and(q -> q
        .or(m -> m
          .setDescription("aged between 65 and 70")
          .setWhere(ageWhere))
        .or(m -> m
          .setDescription("has pre-diabetes")
          .setTypeOf(Namespace.IM + "Observation")
          .where(w -> w
            .setIri(IM.DATA_MODEL_PROPERTY_CONCEPT)
            .addIs(new Node().setIri(Namespace.SNOMED + "714628002").setDescendantsOf(true))
            .setValueLabel("Prediabetes"))))
      .and(q -> q
          .setDescription("Latest systolic within 12 months of the search date")
          .setTypeOf(Namespace.IM + "Observation")
          .setKeepAs("latestBPL12M")
          .path(p->p.setIri(Namespace.IM+"observation").setVariable("obs").setTypeOf(Namespace.IM+"Observation"))
         .where(and -> and
          .and(ww -> ww
            .setIri(IM.DATA_MODEL_PROPERTY_CONCEPT)
            .setName("concept")
            .addIs(new Node()
              .setIri(Namespace.SNOMED + "271649006")
              .setDescendantsOrSelfOf(true)
              .setName("Systolic blood pressure"))
            .addIs(new Node()
              .setIri(Namespace.EMIS + "1994021000006115")
              .setDescendantsOrSelfOf(true)
              .setName("Home systolic blood pressure"))
            .setValueLabel("Office or home systolic blood pressure"))
          .addAnd(bpLast6Months))
        .setOrderBy(new OrderLimit()
          .addProperty(new OrderDirection()
            .setNodeRef("Observation")
            .setIri(Namespace.IM + "effectiveDate")
            .setDirection(Order.descending))
          .setLimit(1))
        .return_(r -> r
          .property(p->p
            .setIri(Namespace.IM+"concept"))))
      .and(then->then
        .setName("Have high blood pressure in the last year")
        .setNodeRef("latestBPL12M")
        .setKeepAs("HighBPReading")
        .return_(r->r
          .property(p ->p
            .setIri(Namespace.IM+"effectiveDate")))
        .setDescription("is either an office systolic >140 or a home systolic >130")
        .where(w -> w
            .or(whereEither -> whereEither
              .and(w1 -> w1
                .setIri(IM.DATA_MODEL_PROPERTY_CONCEPT)
                .addIs(new Node()
                  .setIri(Namespace.SNOMED + "271649006")
                  .setDescendantsOrSelfOf(true)
                  .setName("Systolic blood pressure"))
                .setValueLabel("Office blood pressure"))
              .and(w1 -> w1
                .setIri(Namespace.IM + "value")
                .setOperator(Operator.gt)
                .setValue("140")))
            .or(whereOr -> whereOr
              .and(w1 -> w1
                .setIri(IM.DATA_MODEL_PROPERTY_CONCEPT)
                .addIs(new Node()
                  .setIri(Namespace.EMIS + "1994021000006115")
                  .setDescendantsOrSelfOf(true)
                  .setName("Home systolic blood pressure"))
                .setValueLabel("Home blood pressure"))
              .and(w1 -> w1
                .setIri(Namespace.IM + "value")
                .setOperator(Operator.gt)
                .setValue("130")))))
      .not(q -> q
        .setKeepAs("InvitedAfterHighBP")
        .setName("Invited for screening after high BP reading")
        .setDescription("invited for screening with an effective date after then effective date of the high BP reading")
        .path(p->p.setIri(Namespace.IM+"observation").setVariable("obs").setTypeOf(Namespace.IM+"Observation"))
        .setTypeOf(Namespace.IM + "Observation")
        .where(and -> and
          .and(inv -> inv
            .setIri(IM.DATA_MODEL_PROPERTY_CONCEPT)
            .addIs(new Node().setIri("http://snomed.info/sct#310422005").setName("invited for screening").setMemberOf(true)))
          .addAnd(relativeWhere)))
      .not(q -> q
        .setKeepAs("OnHypertensionRegister")
        .setName("on hypertension register")
        .setDescription("is registered on the hypertensives register")
        .is(is->is.setIri("http://endhealth.info/qof#37d6ee71-b642-407c-be92-cbc924013387")
          .setName("Hypertensives")));

    TTEntity qry = new TTEntity().addType(iri(IM.QUERY))
      .set(iri(IM.RETURN_TYPE), TTIriRef.iri(Namespace.IM + "Patient"))
      .setIri(Namespace.IM + "Q_TestQuery")
      .setName("Patients 65-70, or diabetes or prediabetes that need invitations for blood pressure measuring")
      .setDescription("Test for patients either aged between 65 and 70 or with diabetes with the most recent systolic in the last 12 months either home >130 or office >140, not followed by a screening invite, excluding hypertensives")
      .setScheme(Namespace.IM.asIri())
      .set(iri(IM.DEFINITION), TTLiteral.literal(query))
      .addObject(iri(IM.DEPENDENT_ON), TTIriRef.iri(Namespace.IM + "Q_RegisteredGMS"))
      .addObject(iri(IM.DEPENDENT_ON), TTIriRef.iri("http://endhealth.info/qof#37d6ee71-b642-407c-be92-cbc924013387"))
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(Namespace.IM + "Q_StandardCohorts"));

    document.addEntity(qry);
  }

  private void deleteSets() throws JsonProcessingException {
    TTEntity entity = new TTEntity()
      .setIri(Namespace.IM + "DeleteSets")
      .setName("Delete all concept sets in a graph")
      .setDescription("Pass in the graph name as a 'this' argument and it deletes all sets")
      .setScheme(Namespace.IM.asIri())
      .set(iri(IM.UPDATE_PROCEDURE), TTLiteral.literal(new Update()
        .match(m -> m
          .setGraph(new Node().setParameter("this"))
          .setTypeOf(IM.CONCEPT_SET.toString()))
        .addDelete(new Delete())));

    document.addEntity(entity);
  }

  private void currentGMS() throws JsonProcessingException {


    TTEntity qry = new TTEntity()
      .addType(iri(IM.QUERY))
      .setScheme(Namespace.IM.asIri())
      .set(iri(IM.RETURN_TYPE), TTIriRef.iri(Namespace.IM + "Patient"))
      .set(iri(IM.USAGE_TOTAL), TTLiteral.literal(10000))
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(Namespace.IM + "Q_StandardCohorts"))
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(Namespace.IM + "Q_DefaultCohorts"))
      .set(iri(SHACL.ORDER), TTLiteral.literal(1))
      .setIri(Namespace.IM + "Q_RegisteredGMS")
      .set(iri(IM.ALTERNATIVE_CODE), TTLiteral.literal("RegisteredAsGMS"))
      .setName("Patients registered for GMS services on the reference date")
      .setDescription("For any gpRegistration period,a gpRegistration start date before the reference date and no end date, or an end date after the reference date.");

    qry.set(iri(IM.DEFINITION), TTLiteral.literal(getGmsIsRegistered()));
    document.addEntity(qry);
  }


  private void getSearchAll() throws JsonProcessingException {
    getQuery("SearchmainTypes", "Search for entities of the main types", "used to filter free text searches excluding queries and concept sets")
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
              .setTypeOf(Namespace.IM + "dataModelProperty")))
          .return_(s -> s
            .property(p -> p.setIri(RDFS.LABEL))
            .property(p -> p.setIri(RDFS.COMMENT))
            .property(p -> p.setIri(IM.CODE))
            .property(p -> p.setIri(IM.HAS_STATUS)
              .return_(r -> r.property(rp -> rp.setIri(RDFS.LABEL))))
            .property(p -> p.setIri(IM.HAS_SCHEME)
              .return_(r -> r.property(rp -> rp.setIri(RDFS.LABEL))))
            .property(p -> p.setIri(RDF.TYPE)
              .return_(r -> r.property(rp -> rp.setIri(RDFS.LABEL))))
            .property(p -> p.setIri(IM.USAGE_TOTAL))
          )));
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
            .setIsNull(true)
            .setIri(IM.CONTENT_TYPE)))
        .and(m1->m1
          .where(w->w
            .setIri(IM.IS_CONTAINED_IN)
            .is(is->is.setIri(Namespace.IM+"EntityTypes")))))
      .return_(r -> r
        .property(p -> p
          .setIri(RDFS.LABEL))
        .property(p -> p
          .setIri(SHACL.PROPERTY)
          .return_(s1 -> s1
            .property(p1 -> p1
              .setIri(SHACL.PATH)))));
    getQuery("AllowableChildTypes", "Allowable child types for editor", "used in the editor to select the type of entity being created as a subtype")
      .set(iri(IM.DEFINITION), TTLiteral.literal(query));
  }


  private void isAllowableRange() throws JsonProcessingException {
    getQuery("IsAllowableRange", "Is an entity an allowable range a particular property", "uses inverse range property to check the ranges of the property as authored. Should be used with another ")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Is an entity an allowable range a particular property")
          .setActiveOnly(true)
          .is(ins -> ins
            .setDescendantsOrSelfOf(true)
            .setParameter("ranges"))
          .return_(r -> r
            .property(p -> p.setIri(RDFS.LABEL))
            .property(p -> p.setIri(RDF.TYPE))
            .property(p -> p.setIri(IM.HAS_SCHEME))
            .property(p -> p.setIri(IM.HAS_TERM_CODE)
              .return_(r1 -> r1.property(p1 -> p1.setIri(RDFS.LABEL)))))));
  }

  private void isValidProperty() throws JsonProcessingException {
    getQuery("IsValidProperty", "is a valid property", "is the property a valid value for the concept(s)")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setImQuery(true)
          .setName("Is it a valid property")
          .setDescription("is the property 'property' a valid value for the concept(s) 'concepts")
          .setActiveOnly(true)
          .is(i -> i.setParameter("property").setAncestorsOrSelfOf(true))
          .setWhere(new Where()
            .setIri(RDFS.DOMAIN)
            .addIs(new Node().setParameter("concept").setAncestorsOf(true))
          )));
  }


  private void getAllowableProperties() throws JsonProcessingException {
    getQuery("AllowableProperties", "Allowable properties for a terminology concept", "Returns a list of properties for a particular term concept, used in value set definitions with RCL")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setTypeOf(RDF.PROPERTY.toString())
          .setName("Allowable Properties for a terminology concept")
          .setDescription("Allowable Properties for a terminology concept")
          .setActiveOnly(true)
          .setName("property that has $this (or supertype) as a domain")
          .setDescription("property that has $this (or supertype) as a domain")
          .setVariable("concept")
          .setWhere(new Where()
            .and(w -> w
              .setIri(IM.HAS_SCHEME)
              .is(is -> is.setIri(Namespace.SNOMED.toString())))
            .and(w -> w
              .setIri(RDFS.DOMAIN)
              .addIs(new Node().setParameter("this").setAncestorsOf(true))
            ))
          .setEntailment(Entail.descendantsOrSelfOf)
          .return_(r -> r.setNodeRef("concept")
            .property(p -> p.setIri(RDFS.LABEL))
            .property(p -> p.setIri(RDF.TYPE))
            .property(p -> p.setIri(IM.HAS_SCHEME))
            .property(p -> p.setIri(IM.HAS_TERM_CODE)
              .return_(r1 -> r1.property(p1 -> p1.setIri(RDFS.LABEL)))))));
  }

  private void getAllowablePropertyAncestors() throws JsonProcessingException {
    getQuery("AllowablePropertyAncestors", "Allowable properties for a terminology concept", "Returns a list of properties for a particular term concept, used in value set definitions with RCL")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setTypeOf(RDF.PROPERTY.toString())
          .setName("Allowable Properties for a terminology concept")
          .setDescription("Allowable Properties for a terminology concept")
          .setActiveOnly(true)
          .setName("property that has $this (or supertype) as a domain")
          .setDescription("property that has $this (or supertype) as a domain")
          .setVariable("concept")
          .setWhere(new Where()
            .setIri(RDFS.DOMAIN)
            .addIs(new Node().setParameter("this").setAncestorsOf(true))
          )
          .return_(r -> r.property(p -> p.setIri(RDFS.LABEL)).property(p -> p.setIri(RDF.TYPE)))));
  }

  private void searchProperties() throws JsonProcessingException {
    getQuery("SearchProperties", "Search for properties by name", "Returns a list of properties using a text search to filter the list.")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Search for properties by name")
          .setDescription("Search for properties by name")
          .setActiveOnly(true)
          .setName("Properties")
          .setDescription("Properties")
          .setVariable("concept")
          .setTypeOf(RDF.PROPERTY.toString())
          .return_(r -> r
            .setNodeRef("concept")
            .property(p -> p.setIri(IM.CODE))
            .property(p -> p.setIri(RDFS.LABEL)))
      ));
  }

  private void getConcepts() throws JsonProcessingException {
    getQuery("SearchEntities", "Search for entities of a certain type", "parameter 'this' set to the list of type iris, Normally used with a text search entry to filter the list")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setActiveOnly(true)
          .setName("Search for concepts of $this type")
          .setDescription("Search for concepts")
          .setDescription("of type $this")
          .setTypeOf(new Node()
            .setParameter("this"))
          .return_(r -> r
            .property(p -> p.setIri(RDFS.LABEL))
            .property(p -> p.setIri(RDF.TYPE)))))
      .getPredicateMap().remove(TTIriRef.iri(Namespace.IM + "query"));
  }

  private void searchFolders() throws JsonProcessingException {
    getQuery("SearchFolders", "Search for folder by name", "Returns a list of folder using a text search")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Search for folders by name")
          .setDescription("Search for folders by name")
          .setActiveOnly(true)
          .setName("of type Folder")
          .setDescription("of type Folder")
          .setVariable("folder")
          .setTypeOf(IM.FOLDER.toString())
          .return_(r -> r
            .setNodeRef("folder")
            .property(p -> p.setIri(RDFS.LABEL))
            .property(p -> p.setIri(RDF.TYPE)))
      ));
  }

  private void searchContainedIn() throws JsonProcessingException {
    getQuery("SearchContainedIn", "Search for entities contained in parent folder", "parameter 'value' needs to be set to the parent folder")
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
    getQuery("SearchAllowableSubclass", "Search for allowable subclasses", "parameter 'value' needs to be set to current entity type")
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

  private void searchAllowableContainedIn() throws JsonProcessingException {
    getQuery("SearchAllowableContainedIn", "Search for allowable parent folder", "parameter 'value' needs to be set to the current entity type")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Search for allowable contained in")
          .setDescription("Search for allowable contained in")
          .setActiveOnly(true)
          .setDescription("Folders with no content type, or content type $value")
          .setVariable("folder")
          .setTypeOf(IM.FOLDER.toString())
          .setWhere(new Where()
            .or(p -> p
              .setIri(IM.CONTENT_TYPE)
              .setIsNull(true))
            .or(p -> p
              .setIri(IM.CONTENT_TYPE)
              .is(i -> i.setParameter("value")))

          )
          .return_(r -> r
            .setNodeRef("folder")
            .property(p -> p.setIri(RDFS.LABEL))
            .property(p -> p.setIri(RDF.TYPE)))
      ));
  }

  private void getDescendants() throws JsonProcessingException {
    getQuery("GetDescendants", "Get active subtypes of concept", "returns transitive closure of an entity and its subtypes, usually used with a text search filter to narrow results")
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(new Query()
          .setName("All subtypes of an entity $this , active only")
          .setDescription("All subtypes of an entity, active only")
          .setActiveOnly(true)
          .setDescription("Is a descendant of, or $this")
          .setVariable("isa")
          .addIs(new Node()
            .setParameter("this")
            .setDescendantsOrSelfOf(true))
          .return_(s -> s.setNodeRef("isa")
            .property(p -> p.setIri(RDFS.LABEL))
            .property(p -> p.setIri(IM.CODE)))))
      .getPredicateMap().remove(TTIriRef.iri(Namespace.IM + "query"));
  }

  private void getSubclasses() throws JsonProcessingException {
    getQuery("GetSubClasses", "Get active subclasses of entity", "returns all subclasses of an entity, active only, used with Creator/Editor to get Status subclasses")
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(new Query()
          .setName("All subclasses of an entity $this, active only")
          .setDescription("All subclasses of an entity, active only")
          .setActiveOnly(true)
          .setDescription("Is a subclass of")
          .setVariable("subclass")
          .where(w -> w
            .setIri(RDFS.SUBCLASS_OF)
            .is(i -> i
              .setParameter("this")))
          .return_(s -> s.setNodeRef("subclass")
            .property(p -> p.setIri(RDFS.LABEL))
            .property(p -> p.setIri(IM.CODE)))))
      .getPredicateMap().remove(TTIriRef.iri(Namespace.IM + "query"));
  }

  private void getSubsets() throws JsonProcessingException {
    getQuery("GetSubsets", "Get subsets using superset iri", "return items which have a isSubsetOf predicate linked to the iri provided")
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(new Query()
          .setName("All subsets of an entity, active only")
          .setActiveOnly(true)
          .return_(r -> r
            .property(s -> s.setIri(IM.CODE))
            .property(s -> s.setIri(RDFS.LABEL)))
          .setDescription("Is a subset of $this")
          .setWhere(new Where()
            .setIri(IM.IS_SUBSET_OF)
            .addIs(new Node().setParameter("this")
            )
          )
        )
      )
      .getPredicateMap().remove(TTIriRef.iri(Namespace.IM + "query"));
  }

  private TTEntity getQuery(String iri, String name, String comment) {
    TTEntity entity = new TTEntity()
      .setIri(Namespace.IM + "Query_" + iri)
      .setName(name)
      .setDescription(comment)
      .addType(iri(IM.QUERY))
      .setScheme(Namespace.IM.asIri())
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(Namespace.IM + "IMFormValidationQueries"));
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
