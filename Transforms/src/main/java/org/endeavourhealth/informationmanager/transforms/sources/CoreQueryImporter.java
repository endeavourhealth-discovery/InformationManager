package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class CoreQueryImporter implements TTImport {
  public TTDocument document;

  @Override
  public void importData(TTImportConfig config) throws ImportException {
    try (TTManager manager = new TTManager()) {
      document = manager.createDocument(GRAPH.DISCOVERY);
      addressProperty("homeAddress", "home");
      addressProperty("workAddress", "work");
      addressProperty("temporaryAddress", "temp");
      addressEventProperty("placeOfResidenceAtEvent", "home");
      telephoneProperty("homeTelephoneNumber", "home");
      telephoneProperty("mobileTelephoneNumber", "mobile");
      telephoneProperty("workTelephoneNumber", "mobile");

      age();
      ethnicity();
      gmsRegistration();
      gmsRegistrationStatus();
      gmsRegisteredPractice();
      getDescendants();
      getSubclasses();
      getConcepts();
      getAllowableProperties();
      boundEntities();
      getAllowableRanges();
      getSearchAll();
      allowableSubTypes();
      currentGMS();
      aged18OrOverAsMatch();
      deleteSets();
      getAncestors();
      getSubsets();
      patientsWithActiveCondition(IM.NAMESPACE + "Q_Diabetics", "Patients with active diabetes",
        SNOMED.NAMESPACE + "73211009", "Diabetes mellitus",
        SNOMED.NAMESPACE + "315051004", "Diabetes resolved");
      patientsWithActiveCondition(IM.NAMESPACE + "Q_Hypertensives", "Patients with active hypertension",
        SNOMED.NAMESPACE + "70995007", "Hypertension",
        SNOMED.NAMESPACE + "162659009", "Hypertension resolved");
      testQuery();
      getActiveDiabetes();
      latestBPMatch();
      latestHighBP();
      objectPropertyRangeSuggestions();
      dataPropertyRangeSuggestions();
      searchProperties();
      dataModelPropertyRange();
      dataModelPropertyByShape();
      searchFolders();
      searchContainedIn();
      searchAllowableSubclass();
      searchAllowableContainedIn();
      addTestSets();
      output(document, config.getFolder());
      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
        filer.fileDocument(document);
      }
    } catch (Exception e) {
      throw new ImportException(e.getMessage(), e);
    }
  }


  private void addTestSets() throws JsonProcessingException {
    TTEntity set = new TTEntity()
      .setIri(IM.NAMESPACE + "CSET_Q_Antihypertensives")
      .setCrud(iri(IM.UPDATE_PREDICATES));
    Query query = new Query()
      .setName("Drugs used to treat hypertension (Q group)")
      .setBoolMatch(Bool.and)
      .match(m -> m
        .setBoolMatch(Bool.or)
        .match(m1 -> m1
          .instanceOf(i -> i
            .setIri("http://bnf.info/bnf#BNF_020201")
            .setMemberOf(true))
          .instanceOf(i -> i
            .setIri("http://bnf.info/bnf#BNF_0204")
            .setMemberOf(true))
          .instanceOf(i -> i
            .setIri("http://bnf.info/bnf#BNF_0205051")
            .setMemberOf(true))
          .instanceOf(i -> i
            .setIri("http://bnf.info/bnf#BNF_0205052")
            .setMemberOf(true))
          .instanceOf(i -> i
            .setIri("http://bnf.info/bnf#BNF_020201")
            .setMemberOf(true)))
        .match(m1 -> m1
          .instanceOf(i -> i
            .setIri("http://snomed.info/sct#10363801000001108")
            .setDescendantsOrSelfOf(true))
          .instanceOf(i -> i
            .setIri("http://snomed.info/sct#10363901000001102")
            .setDescendantsOrSelfOf(true))
          .setBoolWhere(Bool.and)
          .where(w -> w
            .setBoolWhere(Bool.or)
            .where(w1 -> w1
              .setIri("http://snomed.info/sct#127489000")
              .setAnyRoleGroup(true)
              .setDescendantsOrSelfOf(true)
              .is(is -> is
                .setIri("http://snomed.info/sct#860766002")
                .setDescendantsOrSelfOf(true))
              .is(is -> is
                .setIri("http://snomed.info/sct#373254001")
                .setDescendantsOrSelfOf(true))
              .is(is -> is
                .setIri("http://snomed.info/sct#372733002")
                .setDescendantsOrSelfOf(true))
              .is(is -> is
                .setIri("http://snomed.info/sct#372913009")
                .setDescendantsOrSelfOf(true)))
            .where(w1 -> w1
              .setIri("http://snomed.info/sct#10363001000001101")
              .setAnyRoleGroup(true)
              .setDescendantsOrSelfOf(true)
              .is(is -> is
                .setIri("http://snomed.info/sct#860766002")
                .setDescendantsOrSelfOf(true))
              .is(is -> is
                .setIri("http://snomed.info/sct#373254001")
                .setDescendantsOrSelfOf(true))
              .is(is -> is
                .setIri("http://snomed.info/sct#372733002")
                .setDescendantsOrSelfOf(true))
              .is(is -> is
                .setIri("http://snomed.info/sct#372913009")
                .setDescendantsOrSelfOf(true))))
          .where(w -> w
            .setBoolWhere(Bool.or)
            .where(w1 -> w1
              .setIri("http://snomed.info/sct#411116001")
              .setAnyRoleGroup(true)
              .setDescendantsOrSelfOf(true)
              .is(i -> i
                .setIri("http://snomed.info/sct#385268001")
                .setDescendantsOrSelfOf(true)))
            .where(w1 -> w1
              .setIri("http://snomed.info/sct#13088501000001100")
              .setAnyRoleGroup(true)
              .setDescendantsOrSelfOf(true)
              .is(i -> i
                .setIri("http://snomed.info/sct#385268001")
                .setDescendantsOrSelfOf(true)))
            .where(w1 -> w1
              .setIri("http://snomed.info/sct#10362901000001105")
              .setAnyRoleGroup(true)
              .setDescendantsOrSelfOf(true)
              .is(i -> i
                .setIri("http://snomed.info/sct#385268001")
                .setDescendantsOrSelfOf(true))))))
      .addPrefix("im", "http://endhealth.info/im#")
      .addPrefix("sct", "http://snomed.info/sct#");
    set.set(iri(IM.DEFINITION), TTLiteral.literal(query));
    document.addEntity(set);

  }


  private void gmsRegisteredPractice() throws JsonProcessingException {
    TTEntity gms = new TTEntity()
      .setIri(IM.NAMESPACE + "gmsRegisteredPractice")
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .addObject(iri(SHACL.PARAMETER), new TTNode()
        .set(iri(RDFS.LABEL), TTLiteral.literal("referenceDate"))
        .set(iri(SHACL.DATATYPE), iri(IM.NAMESPACE + "DateTime")))
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(new Query()
          .setName("GP GMS registeredPractice episode at a reference date")
          .setDescription("Retrieves the entry for the GP practice if gms registered on the date or null")
          .match(m -> m
            .setVariable("RegistrationEpisode")
            .setBoolWhere(Bool.and)
            .path(p -> p
              .setIri(IM.NAMESPACE + "episodeOfCare"))
            .setTypeOf(IM.NAMESPACE + "EpisodeOfCare")
            .where(p1 -> p1
              .setIri(IM.NAMESPACE + "patientType")
              .addIs(new Node().setIri(IM.GMS_PATIENT).setName("Regular GMS patient")))
            .where(pv -> pv
              .setIri(IM.NAMESPACE + "effectiveDate")
              .setOperator(Operator.lte)
              .setRelativeTo(new PropertyRef().setParameter("$referenceDate")))
            .where(pv -> pv
              .setBoolWhere(Bool.or)
              .where(pv1 -> pv1
                .setIri(IM.NAMESPACE + "endDate")
                .setIsNull(true))
              .where(pv1 -> pv1
                .setIri(IM.NAMESPACE + "endDate")
                .setOperator(Operator.gt)
                .setRelativeTo(new PropertyRef().setParameter("$referenceDate"))))
            .orderBy(o -> o
              .setProperty(new OrderDirection().setIri(IM.NAMESPACE + "effectiveDate").setDirection(Order.descending))
              .setLimit(1)))
          .return_(r -> r
            .property(p -> p
              .setNodeRef("RegistrationEpisode")
              .setIri(IM.NAMESPACE + "provider")))));
    document.addEntity(gms);
  }


  private void gmsRegistration() throws JsonProcessingException {
    TTEntity gms = new TTEntity()
      .setIri(IM.NAMESPACE + "gmsRegistrationAtEvent")
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .addObject(iri(SHACL.PARAMETER), new TTNode()
        .set(iri(RDFS.LABEL), TTLiteral.literal("referenceDate"))
        .set(iri(SHACL.DATATYPE), iri(IM.NAMESPACE + "DateTime")))
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(getGmsQuery()));

    document.addEntity(gms);
  }

  private Query getGmsQuery() {
    return new Query()
      .setName("GP GMS registration status at a reference date")
      .setDescription("Retrieves the Registration status of active, left or died")
      .setTypeOf(IM.NAMESPACE + "Patient")
      .match(m -> m
        .where(w -> w
          .setIri(IM.NAMESPACE + "gmsRegistrationStatus")
          .is(is -> is
            .setIri(IM.NAMESPACE + "CaseloadStatusActive"))));
  }

  private void gmsRegistrationStatus() throws JsonProcessingException {
    Query query = getGmsQuery();
    query.setName("Returns the gpRegistration status of a patient if they are currently registered as a regular GMS patient, or if died");
    query.setVariable("currentEpisode");
    Return ret = new Return();
    query.addReturn(ret);
    ReturnProperty returnProperty = new ReturnProperty();
    ret.addProperty(returnProperty);
    returnProperty.case_(c -> c
      .when(when -> when
        .where(w -> w
          .setBoolWhere(Bool.or)
          .where(w1 -> w1
            .setIri(IM.NAMESPACE + "dateOfDeath")
            .setIsNull(true))
          .where(w1 -> w1
            .setIri(IM.NAMESPACE + "dateOfDeath")
            .setOperator(Operator.lt)
            .relativeTo(r -> r.setParameter("referenceDate"))))
        .setThen(IM.NAMESPACE + "CaseloadStatusDead"))
      .when(when -> when
        .where(pv -> pv
          .setBoolWhere(Bool.or)
          .where(pv1 -> pv1
            .setNodeRef("currentEpisode")
            .setIri(IM.NAMESPACE + "endDate")
            .setIsNull(true))
          .where(pv1 -> pv1
            .setNodeRef("currentEpisode")
            .setIri(IM.NAMESPACE + "endDate")
            .setOperator(Operator.gt)
            .setRelativeTo(new PropertyRef().setParameter("$referenceDate"))))
        .setThen(IM.NAMESPACE + "CaseloadStatusActive"))
      .setElse(IM.NAMESPACE + "CaseloadStatusLeft"));

    TTEntity gms = new TTEntity()
      .setIri(IM.NAMESPACE + "gmsRegistrationStatus")
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(query));
    document.addEntity(gms);

  }


  private void ethnicity() throws JsonProcessingException {
    TTEntity ethnicity = new TTEntity()
      .setIri(IM.NAMESPACE + "ethnicity")
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(new Query()
          .setName("Returns the most recent ethnicity for a patient")
          .path(p -> p.setIri("observation"))
          .setTypeOf(IM.NAMESPACE + "Observation")
          .where(w -> w
            .setIri(IM.NAMESPACE + "concept")
            .is(i -> i.setIri("VSET_Ethnicity")))
          .orderBy(o -> o.setProperty(new OrderDirection()
              .setIri(IM.NAMESPACE + "effectiveDate")
              .setDirection(Order.descending))
            .setLimit(1))));
    document.addEntity(ethnicity);
  }

  private void addressProperty(String propertyName, String value) throws JsonProcessingException {
    TTEntity address = new TTEntity()
      .setIri(IM.NAMESPACE + propertyName)
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .set(iri(IM.DEFINITION), TTLiteral.literal(new Query()
        .setName(value + " address property definition")
        .match(m -> m
          .path(p -> p.setIri(IM.NAMESPACE + "address"))
          .setBoolWhere(Bool.and)
          .where(w -> w
            .setIri(IM.NAMESPACE + "effectiveDate")
            .setOperator(Operator.lte)
            .relativeTo(r -> r.setParameter("$now")))
          .where(w -> w
            .setIri(IM.NAMESPACE + "endDate")
            .setIsNull(true))
          .where(w -> w
            .setIri(IM.NAMESPACE + "addressUse")
            .is(is -> is.setIri("http://hl7.org/fhir/fhir-address-use/" + value)))
          .orderBy(ob -> ob.setProperty(new OrderDirection().setIri(IM.NAMESPACE + "effectiveDate").setDirection(Order.descending)).setLimit(1)))));
    document.addEntity(address);

  }

  private void addressEventProperty(String propertyName, String value) throws JsonProcessingException {
    TTEntity address = new TTEntity()
      .setIri(IM.NAMESPACE + propertyName)
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .set(iri(IM.DEFINITION), TTLiteral.literal(new Query()
        .setName(value + " address property definition")
        .match(m -> m
          .path(p -> p.setIri(IM.NAMESPACE + "address"))
          .setBoolWhere(Bool.and)
          .where(w -> w
            .setIri(IM.NAMESPACE + "effectiveDate")
            .setOperator(Operator.lte)
            .relativeTo(r -> r.setParameter("$now")))
          .where(or -> or
            .setBoolWhere(Bool.or)
            .where(w -> w
              .setIri(IM.NAMESPACE + "endDate")
              .setIsNull(true))
            .where(w -> w
              .setIri(IM.NAMESPACE + "endDate")
              .setOperator(Operator.gt)
              .relativeTo(r -> r.setParameter("$referenceDate"))))
          .where(w -> w
            .setIri(IM.NAMESPACE + "addressUse")
            .is(is -> is.setIri("http://hl7.org/fhir/fhir-address-use/" + value)))
          .orderBy(ob -> ob.setProperty(new OrderDirection().setIri(IM.NAMESPACE + "effectiveDate").setDirection(Order.descending)).setLimit(1)))));
    document.addEntity(address);

  }

  private void telephoneProperty(String propertyName, String value) throws JsonProcessingException {
    TTEntity address = new TTEntity()
      .setIri(IM.NAMESPACE + propertyName)
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .set(iri(IM.DEFINITION), TTLiteral.literal(new Query()
        .setName(value + " telephone property definition")
        .match(m -> m
          .path(p -> p.setIri(IM.NAMESPACE + "telephone"))
          .setBoolWhere(Bool.and)
          .where(w -> w
            .setIri(IM.STATUS)
            .is(is -> is.setIri(IM.ACTIVE)))
          .where(w -> w
            .setIri(IM.NAMESPACE + "use")
            .is(is -> is.setIri("http://hl7.org/fhir/contact-point-use/" + value))))));
    document.addEntity(address);

  }

  private void age() throws JsonProcessingException {
    TTEntity age = new TTEntity()
      .setIri(IM.NAMESPACE + "age")
      .setCrud(iri(IM.UPDATE_PREDICATES))
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(new Query()
          .setName("Age function")
          .where(w -> w
            .setIri(IM.NAMESPACE + "dateOfBirth")
            .setIsNotNull(true)
            .setValueVariable("dateOfBirth"))
          .function(f -> f
            .setName(Function.timeDifference)
            .argument(a -> a
              .setParameter("firstDateTime")
              .setValueVariable("dateOfBirth"))
            .argument(a -> a
              .setParameter("secondDateTime")
              .setValueVariable("$referenceDate"))
            .argument(a -> a
              .setParameter("units")
              .setValueVariable("$timeUnits")))));
    document.addEntity(age);
  }


  private void aged18OrOverAsMatch() throws JsonProcessingException {

    Match aged18OrOver = new Match()
      .setName("Aged 18 years or over")
      .setDescription("Aged 18 or more years old")
      .addWhere(new Where()
        .setIri(IM.NAMESPACE + "age")
        .setUnit("YEAR")
        .setOperator(Operator.gte)
        .setValue("18"));

    TTEntity qry = new TTEntity()
      .setIri(IM.NAMESPACE + "M_AgedOverEighteen")
      .setName("Aged 18 or over (feature)")
      .setDescription("Tests whether a person is 18 or more years of age.")
      .addType(iri(IM.MATCH_CLAUSE))
      .set(iri(IM.RETURN_TYPE), TTIriRef.iri(IM.NAMESPACE + "Patient"))
      .set(iri(IM.USAGE_TOTAL), TTLiteral.literal(10000))
      .set(iri(SHACL.ORDER), 3)
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "M_CommonClauses"))
      .set(iri(IM.DEFINITION), TTLiteral.literal(aged18OrOver));

    document.addEntity(qry);
  }

  private void objectPropertyRangeSuggestions() throws JsonProcessingException {
    TTEntity query = getQuery("ObjectPropertyRangeSuggestions", "Range suggestions for object property", "takes account of the data model shape that the property is part of")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Suggested range for an object property")
          .setDescription("get node or class values (ranges) of (object) properties that have $this as their path")
          .match(m -> m
            .setName("Object property range(s)")
            .setDescription("Range(s) (SHACL node or SHACL class) of (implied) object properties")
            .addWhere(new Where()
              .setBoolWhere(Bool.or)
              .where(p -> p
                .setIri(SHACL.NODE)
                .match(n -> n
                  .setVariable("range")))
              .where(p -> p
                .setIri(SHACL.CLASS)
                .match(n -> n
                  .setVariable("range")))))
          .match(m -> m
            .setName("Path is $this")
            .setDescription("have $this as their path")
            .addWhere(new Where()
              .setIri(SHACL.PATH)
              .addIs(new Node().setParameter("this"))))
          .return_(r -> r.setNodeRef("range").property(p -> p.setIri(RDFS.LABEL)))));

    document.addEntity(query);
  }

  private void dataModelPropertyByShape() throws JsonProcessingException {
    TTEntity query = getQuery("DataModelPropertyByShape", "Data model property", "takes account of the data model shape that the property is part of")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Data model property")
          .setDescription("get properties of property objects for specific data model and property")
          .match(m -> m
            .setName("Data model property")
            .setDescription("A given property ($myProperty) of a given data model ($myDataModel)")
            .addInstanceOf(new Node()
              .setParameter("myDataModel"))
            .addWhere(new Where()
              .setIri(SHACL.PROPERTY)
              .match(n -> n
                .setName("Property $myProperty")
                .setDescription("Property $myProperty that exists on a data model (via a path)")
                .setVariable("shaclProperty")
                .addWhere(new Where()
                  .setIri(SHACL.PATH)
                  .addIs(new Node().setParameter("myProperty"))))))
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
          .match(m -> m
            .setName("Data model property ranges")
            .setDescription("The range (node, class or datatype) of $myProperty on $myDataModel")
            .addInstanceOf(new Node()
              .setParameter("myDataModel"))
            .addWhere(new Where()
              .setIri("http://www.w3.org/ns/shacl#property")
              .match(m1 -> m1
                .setName("Property range")
                .setDescription("The range (node, class or datatype) of $myProperty")
                .setVariable("shaclProperty")
                .where(p2 -> p2
                  .setIri(SHACL.PATH)
                  .is(in -> in
                    .setParameter("myProperty")))
                .where(p2 -> p2
                  .setBoolWhere(Bool.or)
                  .where(p3 -> p3
                    .setIri(SHACL.CLASS)
                    .match(m3 -> m3
                      .setVariable("propType")))
                  .where(p3 -> p3
                    .setIri(SHACL.NODE)
                    .match(m3 -> m3
                      .setVariable("propType")))
                  .where(p3 -> p3
                    .setIri(SHACL.DATATYPE)
                    .match(m3 -> m3
                      .setVariable("propType")))))))
          .return_(r -> r
            .setNodeRef("propType")
            .property(p -> p
              .setIri(RDFS.LABEL)))));

    document.addEntity(query);
  }

  private void dataPropertyRangeSuggestions() throws JsonProcessingException {
    TTEntity query = getQuery("dataPropertyRangeSuggestions", "Range suggestions for data property", "takes account of the data model shape that the property is part of")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Suggested range for a data property")
          .setDescription("get datatype values (ranges) of (data) properties that have $this as their path")
          .match(m -> m
            .setName("Suggested range for a data property")
            .setDescription("Range(s) (Datatype) of (implied) data properties")
            .addWhere(new Where()
              .setIri(SHACL.DATATYPE)
              .match(n -> n
                .setVariable("range"))))
          .match(m -> m
            .setName("Path is $this")
            .setDescription("have $this as their path")
            .addWhere(new Where()
              .setIri(SHACL.PATH)
              .addIs(new Node().setParameter("this"))))
          .return_(s -> s.setNodeRef("range").property(p -> p.setIri(RDFS.LABEL)))));

    document.addEntity(query);
  }

  private void getActiveDiabetes() throws JsonProcessingException {
    TTEntity entity = new TTEntity()
      .addType(iri(IM.MATCH_CLAUSE))
      .setName("Active Diabetes (Latest entry for diabetes not followed by a resolution)")
      .setDescription("Entry for diabetes not followed by a diabetes resolved entry")
      .set(iri(IM.RETURN_TYPE), TTIriRef.iri(IM.NAMESPACE + "Patient"))
      .setIri(IM.NAMESPACE + "M_ActiveDiabetes")
      .set(iri(SHACL.ORDER), 2)
      .set(iri(IM.DEFINITION), TTLiteral.literal(getActiveDiabetesMatch()))
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "M_CommonClauses"));

    document.addEntity(entity);
  }

  private void latestHighBP() throws JsonProcessingException {
    Match match = new Match()
      .setName("Latest systolic in the last year is >140 (office) or >130 (home)")
      .setDescription("Latest home or office systolic blood pressure in last 12 months is either an office one over 140, or a home one over 130")
      .path(p -> p
        .setIri(IM.NAMESPACE + "observation"))
      .setTypeOf(IM.NAMESPACE + "Observation")
      .where(ww -> ww
        .setIri(IM.NAMESPACE + "concept")
        .setName("concept")
        .addIs(new Node()
          .setIri(SNOMED.NAMESPACE + "999035921000230109")
          .setDescendantsOrSelfOf(true)
          .setName("Systolic blood pressure recording"))
        .addIs(new Node()
          .setIri(GRAPH.EMIS + "1994021000006104")
          .setDescendantsOrSelfOf(true)
          .setName("Home systolic blood pressure"))
        .setValueLabel("Office home or self recorded systolic blood pressure"))
      .where(ww -> ww
        .setIri(IM.NAMESPACE + "value")
        .setIsNotNull(true))
      .where(ww -> ww
        .setIri(IM.NAMESPACE + "effectiveDate")
        .setOperator(Operator.gte)
        .setValue("-12")
        .setUnit("MONTHS")
        .relativeTo(r -> r.setParameter("$referenceDate"))
        .setValueLabel("last 12 months"))
      .setOrderBy(new OrderLimit()
        .setProperty(new OrderDirection()
          .setIri(IM.NAMESPACE + "effectiveDate")
          .setDirection(Order.descending))
        .setLimit(1))
      .then(t -> t
        .setName("Office systolic >140 or Home systolic >130")
        .setDescription("Systolic blood pressure is either an office one with a value greater than 140, or a home one with a value greater than 130")
        .setVariable("highBPReading")
        .setBoolMatch(Bool.or)
        .match(m4 -> m4
          .setName("Office systolic >140")
          .setDescription("Is an office systolic blood pressure with a value greater than 140")
          .where(w -> w
            .setIri(IM.NAMESPACE + "concept")
            .addIs(new Node()
              .setIri(SNOMED.NAMESPACE + "271649006")
              .setDescendantsOrSelfOf(true)
              .setName("Systolic blood pressure"))
            .setValueLabel("Office blood pressure"))
          .where(w -> w
            .setIri(IM.NAMESPACE + "value")
            .setOperator(Operator.gt)
            .setValue("140")))
        .match(m4 -> m4
          .setName("Home systolic >130")
          .setDescription("Is a home systolic blood pressure with a value greater than 130")
          .addWhere(new Where()
            .setBoolWhere(Bool.and)
            .where(w -> w
              .setIri(IM.NAMESPACE + "concept")
              .addIs(new Node()
                .setIri(GRAPH.EMIS + "1994021000006104")
                .setDescendantsOrSelfOf(true)
                .setName("Home systolic blood pressure"))
              .setValueLabel("Home blood pressure"))
            .where(w -> w
              .setIri(IM.NAMESPACE + "value")
              .setOperator(Operator.gt)
              .setValue("130")))));

    TTEntity entity = new TTEntity().addType(iri(IM.MATCH_CLAUSE))
      .set(iri(IM.RETURN_TYPE), TTIriRef.iri(IM.NAMESPACE + "Patient"))
      .setIri(IM.NAMESPACE + "M_LatestRecentHighSystolic")
      .setName("Latest systolic blood pressure in the last 12 months is high")
      .setDescription("Latest home or office BP within the last 12 months is either >140 if in the office of >130 if done at home or self reported")
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "M_CommonClauses"))
      .set(iri(SHACL.ORDER), 4)
      .set(iri(IM.DEFINITION), TTLiteral.literal(match));

    document.addEntity(entity);
  }

  private void getAncestors() throws JsonProcessingException {
    TTEntity query = getQuery("GetAncestors", "Get active supertypes of concept", "returns transitive closure of an entity and its supertypes, usually used with a text search filter to narrow results");
    query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE + "query"));
    query.set(iri(IM.DEFINITION),
      TTLiteral.literal(new Query()
        .setName("All subtypes of an entity, active only")
        .setActiveOnly(true)
        .match(w -> w
          .setName("All subtypes")
          .setDescription("All subtypes of an entity (where the entity 'is a' $this)")
          .setVariable("isa")
          .addInstanceOf(new Node()
            .setParameter("this")
            .setAncestorsOf(true)))
        .return_(s -> s.setNodeRef("isa")
          .property(p -> p.setIri(RDFS.LABEL))
          .property(p -> p.setIri(IM.CODE)))));
  }

  private void latestBPMatch() throws JsonProcessingException {
    Match match = new Match()
      .setName("Latest systolic blood pressure in the last 12 months")
      .setDescription("Latest systolic blood pressure in the last 12 months")
      .path(p -> p
        .setIri(IM.NAMESPACE + "observation"))
      .where(ww -> ww
        .setIri(IM.NAMESPACE + "concept")
        .setName("concept")
        .addIs(new Node()
          .setIri(SNOMED.NAMESPACE + "999035921000230109")
          .setName("Systolic blood pressure recording")))
      .where(ww -> ww
        .setIri(IM.NAMESPACE + "value")
        .setIsNotNull(true))
      .where(ww -> ww
        .setIri(IM.NAMESPACE + "effectiveDate")
        .setOperator(Operator.gte)
        .setValue("-12")
        .setUnit("MONTHS")
        .relativeTo(r -> r.setParameter("$referenceDate"))
        .setValueLabel("last 12 months"))
      .setOrderBy(new OrderLimit()
        .setProperty(new OrderDirection()
          .setIri(IM.NAMESPACE + "effectiveDate")
          .setDirection(Order.descending))
        .setLimit(1));

    TTEntity entity = new TTEntity().addType(iri(IM.MATCH_CLAUSE))
      .set(iri(IM.RETURN_TYPE), TTIriRef.iri(IM.NAMESPACE + "Patient"))
      .setIri(IM.NAMESPACE + "M_LatestSystolicBP12M")
      .setName("Latest systolic blood pressure in the last 12 months")
      .setDescription("The latest systolic blood pressure that has a value")
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "M_CommonClauses"))
      .set(iri(SHACL.ORDER), 3)
      .set(iri(IM.DEFINITION), TTLiteral.literal(match));

    document.addEntity(entity);
  }

  private Match getActiveDiabetesMatch() {
    return new Match()
      .setName("Active diabetics")
      .setDescription("Diabetes observations, not followed by a resolved observation")
      .path(p -> p.setIri(IM.NAMESPACE + "observation"))
      .setTypeOf(IM.NAMESPACE + "Observation")
      .setVariable("latestDiabetes")
      .where(ww -> ww
        .setIri(IM.NAMESPACE + "concept")
        .setName("concept")
        .addIs(new Node()
          .setIri("http://snomed.info/sct#999004691000230108")
          .setName("Diabetes Mellitus")))
      .setOrderBy(new OrderLimit()
        .setProperty(new OrderDirection()
          .setIri(IM.NAMESPACE + "effectiveDate")
          .setDirection(Order.descending))
        .setLimit(1))
      .then(m1 -> m1
        .setName("Exclude resolved diabetes")
        .setDescription("Exclude where a diabetes resolved observation exists with an effective date after the effective date of the $latestDiabetes observation")
        .setExclude(true)
        .setVariable("ResolvedDiabetes")
        .where(ww -> ww
          .setIri(IM.NAMESPACE + "concept")
          .setName("concept")
          .addIs(new Node()
            .setIri("http://snomed.info/sct#999003371000230102")
            .setName("Diabetes Resolved")))
        .where(ww -> ww
          .setIri(IM.NAMESPACE + "effectiveDate")
          .setOperator(Operator.gte)
          .relativeTo(r -> r.setNodeRef("latestDiabetes").setIri(IM.NAMESPACE + "effectiveDate"))));
  }

  private void patientsWithActiveCondition(String iri, String name, String activeIri, String activeName, String inactiveIri, String inactiveName) throws JsonProcessingException {
    Query definition = new Query()
      .setIri(iri)
      .setName(name)
      .setDescription("The latest " + activeName + " or " + inactiveName + " is " + activeName)
      .setTypeOf(IM.NAMESPACE + "Patient")
      .match(m -> m
        .setName("Active {condition}")
        .setDescription("The latest " + activeName + " or " + inactiveName + " is " + activeName)
        .path(p -> p
          .setIri(IM.NAMESPACE + "observation"))
        .setTypeOf(IM.NAMESPACE + "Observation")
        .where(ww -> ww
          .setIri(IM.NAMESPACE + "concept")
          .setName("concept")
          .addIs(new Node()
            .setIri(activeIri)
            .setName(activeName)
            .setDescendantsOrSelfOf(true))
          .addIs(new Node()
            .setIri(inactiveIri)
            .setName(inactiveName)
            .setDescendantsOrSelfOf(true))
          .setValueLabel(activeName + " or " + inactiveName))
        .setOrderBy(new OrderLimit()
          .setProperty(new OrderDirection()
            .setIri(IM.NAMESPACE + "effectiveDate")
            .setDirection(Order.descending))
          .setLimit(1))
        .then(t -> t
          .setName("Is " + activeName)
          .setDescription("Is " + activeName)
          .addWhere(new Where()
            .setIri(IM.NAMESPACE + "concept")
            .addIs(new Node()
              .setIri(activeIri)
              .setName(activeName)
              .setDescendantsOrSelfOf(true)))));

    TTEntity qry = new TTEntity(iri)
      .addType(iri(IM.COHORT_QUERY))
      .setName(name)
      .set(iri(IM.RETURN_TYPE), TTIriRef.iri(IM.NAMESPACE + "Patient"))
      .set(iri(IM.DEFINITION), TTLiteral.literal(definition))
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "Q_StandardCohorts"));

    document.addEntity(qry);
  }

  private void testQuery() throws IOException {
    Query prof = new Query()
      .setIri(IM.NAMESPACE + "Q_TestQuery")
      .setName("Patients 65-70, or diabetes or prediabetes that need invitations for blood pressure measuring")
      .setDescription("Test for patients either aged between 65 and 70 or with diabetes with the most recent systolic in the last 12 months either home >130 or office >140, not followed by a screening invite, excluding hypertensives")
      .setTypeOf(IM.NAMESPACE + "Patient")
      .match(m -> m
        .setName("Patients registered for GMS services on the reference date")
        .setDescription("For any gpRegistration period, a gpRegistration start date before the reference date and no end date, or an end date after the reference date.")
        .addInstanceOf(new Node().setIri(IM.NAMESPACE + "Q_RegisteredGMS").setMemberOf(true)
          .setName("Registered for GMS services on reference date")))
      .match(m -> m
        .setName("Patients 65-70, or diabetes or prediabetes")
        .setDescription("Patients with an age between 65 and 70, or on the diabetic register, or have prediabetes")
        .setBoolMatch(Bool.or)
        .match(or -> or
          .setName("aged between 65 and 70")
          .addWhere(new Where()
            .setIri(IM.NAMESPACE + "age")
            .range(r -> r
              .from(from -> from
                .setOperator(Operator.gte)
                .setValue("65")
                .argument(a->a
                  .setParameter("units")
                  .setValueIri(iri(IM.NAMESPACE+"years"))))
              .to(to -> to
                .setOperator(Operator.lt)
                .setValue("70")
                .argument(a->a
                  .setParameter("units")
                  .setValueIri(iri(IM.NAMESPACE+"years")))))))
        .match(or -> or
          .setName("Is on diabetic register")
          .addInstanceOf(new Node().setIri(IM.NAMESPACE + "Q_Diabetics").setMemberOf(true)))
        .match(or -> or
          .setName("has pre-diabetes")
          .path(p -> p.setIri(IM.NAMESPACE + "observation"))
          .setTypeOf(IM.NAMESPACE + "Observation")
          .addWhere(new Where()
            .setIri(IM.NAMESPACE + "concept")
            .addIs(new Node().setIri(SNOMED.NAMESPACE + "714628002").setDescendantsOf(true))
            .setValueLabel("Prediabetes"))))
      .match(m -> m
        .setName("Have high blood pressure in the last year")
        .setDescription("Latest systolic within 12 months of the reference date, is either an office systolic >140 or a home systolic >130")
        .path(p -> p
          .setIri(IM.NAMESPACE + "observation"))
        .setTypeOf(IM.NAMESPACE + "Observation")
        .where(ww -> ww
          .setIri(IM.NAMESPACE + "concept")
          .setName("concept")
          .addIs(new Node()
            .setIri(SNOMED.NAMESPACE + "271649006")
            .setDescendantsOrSelfOf(true)
            .setName("Systolic blood pressure"))
          .addIs(new Node()
            .setIri(GRAPH.EMIS + "1994021000006115")
            .setDescendantsOrSelfOf(true)
            .setName("Home systolic blood pressure"))
          .setValueLabel("Office or home systolic blood pressure"))
        .where(ww -> ww
          .setIri(IM.NAMESPACE + "effectiveDate")
          .setOperator(Operator.gte)
          .setValue("-12")
          .setUnit("MONTHS")
          .relativeTo(r -> r.setParameter("$referenceDate"))
          .setValueLabel("last 12 months"))
        .setOrderBy(new OrderLimit()
          .setProperty(new OrderDirection()
            .setIri(IM.NAMESPACE + "effectiveDate")
            .setDirection(Order.descending))
          .setLimit(1))
        .then(t -> t.setVariable("highBPReading")
          .setBoolMatch(Bool.or)
          .match(m4 -> m4
            .setName("Office systolic > 140")
            .setDescription("Office based systolic blood pressure with value greater than 140")
            .where(w -> w
              .setIri(IM.NAMESPACE + "concept")
              .addIs(new Node()
                .setIri(SNOMED.NAMESPACE + "271649006")
                .setDescendantsOrSelfOf(true)
                .setName("Systolic blood pressure"))
              .setValueLabel("Office blood pressure"))
            .where(w -> w
              .setIri(IM.NAMESPACE + "value")
              .setOperator(Operator.gt)
              .setValue("140")))
          .match(m4 -> m4
            .setName("Home systolic > 130")
            .setDescription("Home based systolic blood pressure with value greater than 130")
            .setBoolWhere(Bool.and)
            .where(w -> w
              .setIri(IM.NAMESPACE + "concept")
              .addIs(new Node()
                .setIri(GRAPH.EMIS + "1994021000006115")
                .setDescendantsOrSelfOf(true)
                .setName("Home systolic blood pressure"))
              .setValueLabel("Home blood pressure"))
            .where(w -> w
              .setIri(IM.NAMESPACE + "value")
              .setOperator(Operator.gt)
              .setValue("130")))))
      .match(m -> m
        .setName("Not invited for screening since high BP reading")
        .setDescription("invited for screening with an effective date after then effective date of the high BP reading")
        .setBoolWhere(Bool.and)
        .setExclude(true)
        .path(w -> w.setIri(IM.NAMESPACE + "observation"))
        .setTypeOf(IM.NAMESPACE + "Observation")
        .where(inv -> inv
          .setIri(IM.NAMESPACE + "concept")
          .addIs(new Node().setIri("http://snomed.info/sct#310422005").setName("invited for screening").setMemberOf(true)))
        .where(after -> after
          .setIri(IM.NAMESPACE + "effectiveDate")
          .setOperator(Operator.gte)
          .relativeTo(r -> r.setNodeRef("highBPReading").setIri(IM.NAMESPACE + "effectiveDate"))))
      .match(m -> m
        .setName("not on hypertension register")
        .setDescription("is registered on the hypertensives register")
        .setExclude(true)
        .addInstanceOf(new Node().setIri(IM.NAMESPACE + "Q_Hypertensives").setMemberOf(true)
          .setName("Hypertensives")));

    TTEntity qry = new TTEntity().addType(iri(IM.COHORT_QUERY))
      .set(iri(IM.RETURN_TYPE), TTIriRef.iri(IM.NAMESPACE + "Patient"))
      .setIri(IM.NAMESPACE + "Q_TestQuery")
      .setName("Patients 65-70, or diabetes or prediabetes that need invitations for blood pressure measuring")
      .setDescription("Test for patients either aged between 65 and 70 or with diabetes with the most recent systolic in the last 12 months either home >130 or office >140, not followed by a screening invite, excluding hypertensives")
      .set(iri(IM.DEFINITION), TTLiteral.literal(prof))
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "Q_StandardCohorts"));

    document.addEntity(qry);
  }

  private void deleteSets() throws JsonProcessingException {
    TTEntity entity = new TTEntity()
      .setIri(IM.NAMESPACE + "DeleteSets")
      .setName("Delete all concept sets in a graph")
      .setDescription("Pass in the graph name as a 'this' argument and it deletes all sets")
      .set(iri(IM.UPDATE_PROCEDURE), TTLiteral.literal(new Update()
        .match(m -> m
          .setGraph(new Node().setParameter("this"))
          .setTypeOf(IM.CONCEPT_SET))
        .addDelete(new Delete())));

    document.addEntity(entity);
  }

  private void currentGMS() throws JsonProcessingException {

    TTEntity qry = new TTEntity()
      .addType(iri(IM.COHORT_QUERY))
      .set(iri(IM.RETURN_TYPE), TTIriRef.iri(IM.NAMESPACE + "Patient"))
      .set(iri(IM.USAGE_TOTAL), TTLiteral.literal(10000))
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "Q_StandardCohorts"))
      .setIri(IM.NAMESPACE + "Q_RegisteredGMS")
      .setName("Patients registered for GMS services on the reference date")
      .setDescription("For any gpRegistration period,a gpRegistration start date before the reference date and no end date, or an end date after the reference date.");

    qry.set(iri(IM.DEFINITION), TTLiteral.literal(getGmsQuery()));
    document.addEntity(qry);
  }


  private void getSearchAll() throws JsonProcessingException {
    getQuery("SearchmainTypes", "Search for entities of the main types", "used to filter free text searches excluding queries and concept sets")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setActiveOnly(true)
          .setName("Search for all main types")
          .setDescription("Search for Concepts, Concept Sets, Folders, Value Sets or Data Model Properties")
          .match(f -> f
            .setBoolMatch(Bool.or)
            .match(w -> w
              .setName("Concepts")
              .setDescription("Type is Concept")
              .setTypeOf(IM.CONCEPT))
            .match(w -> w
              .setName("Concept sets")
              .setDescription("Type is Concept Set")
              .setTypeOf(IM.CONCEPT_SET))
            .match(w -> w
              .setName("Folders")
              .setDescription("Type is Folder")
              .setTypeOf(IM.FOLDER))
            .match(w -> w
              .setName("Value Sets")
              .setDescription("Type is Value Set")
              .setTypeOf(IM.VALUESET))
            .match(w -> w
              .setName("Data model property")
              .setDescription("Type is Data Model Property")
              .setTypeOf(IM.NAMESPACE + "dataModelProperty")))
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

  private void allowableSubTypes() throws IOException {
    Query query = new Query()
      .setName("Allowable child types for editor")
      .setDescription("used in the editor to select the type of entity being created as a subtype")
      .match(m -> m
        .setName("Child types")
        .setDescription("Instances of $this")
        .addInstanceOf(new Node()
          .setParameter("$this"))
        .addWhere(new Where()
          .setIri(RDF.TYPE)
          .setValueVariable("thisType")))
      .match(m -> m
        .setName("is an entity type")
        .setDescription("Is contained in the 'Entity Types' folder")
        .setVariable("concept")
        .addWhere(new Where()
          .setIri(IM.IS_CONTAINED_IN)
          .addIs(IM.NAMESPACE + "EntityTypes")))
      .match(m -> m
        .setName("Is of type and is a child")
        .setDescription("Is of the given type and is a child (contained in, subclass of, or subset of)")
        .setNodeRef("concept")
        .addWhere(new Where()
          .setIri(SHACL.PROPERTY)
          .match(n -> n
            .setName("is a child")
            .setDescription("is a child (contained in, subclass of, or subset of)")
            .setVariable("predicate")
            .where(a2 -> a2
              .setIri(SHACL.NODE)
              .addIs(new Node().setNodeRef("thisType")))
            .where(a2 -> a2
              .setIri(SHACL.PATH)
              .setIs(List.of(Node.iri(IM.IS_CONTAINED_IN), Node.iri(RDFS.SUBCLASS_OF), Node.iri(IM.IS_SUBSET_OF)))))))
      .match(m -> m
        .setName("Is instance of $concept, $this (if content type is folder), excluding if $this is a content type itself")
        .setBoolMatch(Bool.or)
        .match(m1 -> m1
          .setName("Instance of $thisType")
          .setDescription("Instance of $thisType")
          .setNodeRef("concept")
          .addInstanceOf(new Node().setNodeRef("thisType")))
        .match(m1 -> m1
          .setName("instance of $this where its content type is $concept or a folder")
          .setDescription("instance of $this where its content type is $concept or a folder")
          .addInstanceOf(new Node()
            .setParameter("$this"))
          .addWhere(new Where()
            .setIri(IM.CONTENT_TYPE)
            .is(in -> in.setNodeRef("concept"))
            .is(in -> in.setIri(IM.FOLDER))))
        .match(m1 -> m1
          .setName("instance of $this is a folder and not content type")
          .setDescription("instance of $this is a folder and not content type")
          .setBoolMatch(Bool.and)
          .match(m2 -> m2
            .setName("instance of $this and a Folder")
            .setDescription("instance of $this and a Folder")
            .addInstanceOf(new Node()
              .setParameter("$this"))
            .addWhere(new Where()
              .setIri(RDF.TYPE)
              .is(in -> in.setIri(IM.FOLDER))))
          .match(m2 -> m2
            .setName("instance of $this is content type")
            .setDescription("instance of $this is content type")
            .addInstanceOf(new Node()
              .setParameter("$this"))
            .setExclude(true)
            .addWhere(new Where()
              .setIri(IM.CONTENT_TYPE)))))
      .return_(s -> s
        .setNodeRef("concept")
        .property(p -> p
          .setIri(RDFS.LABEL))
        .property(p -> p
          .setIri(SHACL.PROPERTY)
          .return_(s1 -> s1
            .setNodeRef("predicate")
            .property(p1 -> p1
              .setIri(SHACL.PATH)))));

    getQuery("AllowableChildTypes", "Allowable child types for editor", "used in the editor to select the type of entity being created as a subtype")
      .set(iri(IM.DEFINITION), TTLiteral.literal(query));
  }

  private void boundEntities() throws JsonProcessingException {
    TTEntity queryEntity = getQuery("BoundEntities", "Data model bound concepts and sets", "For a known data model type and property. Filters by entities that are bound via the value set ");
    queryEntity.set(iri(IM.DEFINITION), TTLiteral.literal(
      new Query()
        .setName("Data model bound concepts and sets")
        .setDescription("For a known data model type and property. Filters by entities that are bound via the value set")
        .match(m -> m
          .where(w -> w
            .setIri(IM.BINDING)
            .match(m1 -> m1
              .where(w1 -> w1
                .setIri(SHACL.PATH)
                .is(is -> is.setIri(IM.NAMESPACE + "concept")))
              .where(w1 -> w1
                .setIri(SHACL.NODE)
                .is(is -> is.setIri(IM.NAMESPACE + "Observation"))))))));

  }


  private void getAllowableRanges() throws JsonProcessingException {
    getQuery("AllowableRanges", "Allowable ranges for a particular property or its ancestors", "uses inverse range property to return the ranges of the property as authored. Should be used with another ")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setImQuery(true)
          .setName("Allowable Ranges for a property and super properties")
          .setDescription("Allowable Ranges for a property and super properties")
          .setActiveOnly(true)
          .match(f -> f
            .setName("Inverse of range is $this or its ancestors")
            .addWhere(new Where()
              .setInverse(true)
              .setIri(RDFS.RANGE)
              .addIs(new Node().setParameter("this")
                .setAncestorsOf(true))))
          .query(q1 -> q1
            .return_(r -> r
              .property(s -> s.setIri(IM.CODE))
              .property(s -> s.setIri(RDFS.LABEL)))
            .match(m -> m
              .addInstanceOf(new Node()
                .setDescendantsOrSelfOf(true))))));
  }

  private void getAllowableProperties() throws JsonProcessingException {
    getQuery("AllowableProperties", "Allowable properties for a terminology concept", "Returns a list of properties for a particular term concept, used in value set definitions with RCL")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setImQuery(true)
          .setName("Allowable Properties for a terminology concept")
          .setDescription("Allowable Properties for a terminology concept")
          .setActiveOnly(true)
          .match(f -> f
            .setName("property that has $this (or supertype) as a domain")
            .setDescription("property that has $this (or supertype) as a domain")
            .setVariable("concept")
            .addWhere(new Where()
              .setIri(RDFS.DOMAIN)
              .addIs(new Node().setParameter("this").setAncestorsOf(true))
            ))
          .query(q -> q
            .match(m -> m
              .addInstanceOf(new Node()
                .setDescendantsOrSelfOf(true))))));
  }

  private void searchProperties() throws JsonProcessingException {
    getQuery("SearchProperties", "Search for properties by name", "Returns a list of properties using a text search to filter the list.")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Search for properties by name")
          .setDescription("Search for properties by name")
          .setActiveOnly(true)
          .match(f -> f
            .setName("Properties")
            .setDescription("Properties")
            .setVariable("concept")
            .setTypeOf(RDF.PROPERTY))
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
          .setName("Search for concepts")
          .setDescription("Search for concepts")
          .match(w -> w
            .setName("of type $this")
            .setDescription("of type $this")
            .setTypeOf(new Node()
              .setParameter("this")))
          .return_(r -> r
            .property(p -> p.setIri(RDFS.LABEL))
            .property(p -> p.setIri(RDF.TYPE)))))
      .getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE + "query"));
  }

  private void searchFolders() throws JsonProcessingException {
    getQuery("SearchFolders", "Search for folder by name", "Returns a list of folder using a text search")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Search for folders by name")
          .setDescription("Search for folders by name")
          .setActiveOnly(true)
          .match(f -> f
            .setName("of type Folder")
            .setDescription("of type Folder")
            .setVariable("folder")
            .setTypeOf(IM.FOLDER))
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
          .match(f -> f
            .setName("Conained in $value")
            .setDescription("Conained in $value")
            .addWhere(new Where()
              .setIri(IM.IS_CONTAINED_IN)
              .is(i -> i
                .setParameter("value")
              )
            )
          )
      ));
  }

  private void searchAllowableSubclass() throws JsonProcessingException {
    getQuery("SearchAllowableSubclass", "Search for allowable subclasses", "parameter 'value' needs to be set to current entity type")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Search for allowable subclasses")
          .setDescription("Search for allowable subclasses")
          .setActiveOnly(true)
          .match(f -> f
            .setName("Subtypes of $value")
            .setDescription("Subtypes (i.e. 'Is a') of $value")
            .addWhere(new Where()
              .setIri(RDF.TYPE)
              .is(i -> i
                .setParameter("value")
              )
            )
          )
      ));
  }

  private void searchAllowableContainedIn() throws JsonProcessingException {
    getQuery("SearchAllowableContainedIn", "Search for allowable parent folder", "parameter 'value' needs to be set to the current entity type")
      .set(iri(IM.DEFINITION), TTLiteral.literal(
        new Query()
          .setName("Search for allowable contained in")
          .setDescription("Search for allowable contained in")
          .setActiveOnly(true)
          .match(m -> m
            .setName("Folders allowing type of content")
            .setDescription("Folders with no content type, or content type $value")
            .setVariable("folder")
            .setTypeOf(IM.FOLDER)
            .addWhere(new Where()
              .setBoolWhere(Bool.or)
              .where(p -> p
                .setIri(IM.CONTENT_TYPE)
                .setIsNull(true))
              .where(p -> p
                .setIri(IM.CONTENT_TYPE)
                .is(i -> i.setParameter("value")))

            )
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
          .setName("All subtypes of an entity, active only")
          .setDescription("All subtypes of an entity, active only")
          .setActiveOnly(true)
          .match(w -> w
            .setName("$this or its descendants")
            .setDescription("Is a descendant of, or $this")
            .setVariable("isa")
            .addInstanceOf(new Node()
              .setParameter("this")
              .setDescendantsOrSelfOf(true)))
          .return_(s -> s.setNodeRef("isa")
            .property(p -> p.setIri(RDFS.LABEL))
            .property(p -> p.setIri(IM.CODE)))))
      .getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE + "query"));
  }

  private void getSubclasses() throws JsonProcessingException {
    getQuery("GetSubClasses", "Get active subclasses of entity", "returns all subclasses of an entity, active only, used with Creator/Editor to get Status subclasses")
      .set(iri(IM.DEFINITION),
        TTLiteral.literal(new Query()
          .setName("All subclasses of an entity, active only")
          .setDescription("All subclasses of an entity, active only")
          .setActiveOnly(true)
          .match(m -> m
            .setName("subclasses of $this")
            .setDescription("Is a subclass of")
            .setVariable("subclass")
            .where(w -> w
              .setIri(RDFS.SUBCLASS_OF)
              .is(i -> i
                .setParameter("this"))))
          .return_(s -> s.setNodeRef("subclass")
            .property(p -> p.setIri(RDFS.LABEL))
            .property(p -> p.setIri(IM.CODE)))))
      .getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE + "query"));
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
          .match(f -> f
            .setName("Subsets")
            .setDescription("Is a subset of $this")
            .addWhere(new Where()
              .setIri(IM.IS_SUBSET_OF)
              .addIs(new Node().setParameter("this")
              )
            )
          )
        ))
      .getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE + "query"));
  }

  private TTEntity getQuery(String iri, String name, String comment) {
    TTEntity entity = new TTEntity()
      .setIri(IM.NAMESPACE + "Query_" + iri)
      .setName(name)
      .setDescription(comment)
      .addType(iri(IM.QUERY))
      .addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "IMFormValidationQueries"));
    document.addEntity(entity);
    return entity;
  }

  @Override
  public void validateFiles(String inFolder) throws TTFilerException {
    // No files to validate
  }

  private void output(TTDocument document, String directory) throws IOException {

    try (FileWriter writer = new FileWriter(directory + "\\DiscoveryCore\\CoreQueries\\CoreQueriesAll.json")) {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
      String doc = objectMapper.writerWithDefaultPrettyPrinter()
        .withAttribute(TTContext.OUTPUT_CONTEXT, true).writeValueAsString(document);
      writer.write(doc);
    }
    for (TTEntity entity : document.getEntities()) {
      if (entity.isType(iri(IM.MATCH_CLAUSE))) {
        Match match = entity.get(iri(IM.DEFINITION)).asLiteral().objectValue(Match.class);
        outputMatch(entity.getName(), match, directory);
      } else {
        if (entity.get(iri(IM.DEFINITION)) != null) {
          Query query = entity.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class);
          outputQuery(query, directory);
        }
      }
    }

  }

  private void outputQuery(Query qry, String directory) throws IOException {
    String name = qry.getName();
    if (name.length() > 20)
      name = name.substring(0, 20);
    try (FileWriter writer = new FileWriter(directory + "\\DiscoveryCore\\CoreQueries\\" + name + "+.json")) {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
      String doc = objectMapper.writerWithDefaultPrettyPrinter()
        .withAttribute(TTContext.OUTPUT_CONTEXT, true).writeValueAsString(qry);
      writer.write(doc);
    }
  }

  private void outputMatch(String name, Match qry, String directory) throws IOException {
    if (name.length() > 20)
      name = name.substring(0, 20);
    try (FileWriter writer = new FileWriter(directory + "\\DiscoveryCore\\CoreQueries\\" + name + "+.json")) {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
      String doc = objectMapper.writerWithDefaultPrettyPrinter()
        .withAttribute(TTContext.OUTPUT_CONTEXT, true).writeValueAsString(qry);
      writer.write(doc);
    }
  }

  @Override
  public void close() throws Exception {

  }
}
