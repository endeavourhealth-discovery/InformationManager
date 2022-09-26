package org.endeavourhealth.informationmanager.transforms.authored;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.endeavourhealth.imapi.model.iml.Query;
import org.endeavourhealth.imapi.model.iml.Value;
import org.endeavourhealth.imapi.model.iml.Where;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SHACL;

import java.io.File;
import java.io.IOException;

public class StandardQueries {

	private TTManager manager;

	public void buildQueries() throws IOException {
		manager= loadForms();
		getIsas();
		getConcepts();
		getAllowableProperties();
		getAllowableRanges();
		saveForms(manager.getDocument());
		gpGMSRegisteredPractice();



	}

	private void gpGMSRegisteredPractice() throws IOException {
		manager= loadCore();
		TTEntity gpRegPractice= manager.getEntity("im:gpGMSRegisteredPractice");
		gpRegPractice.getPredicateMap().remove(SHACL.PARAMETER);
		Query query= new Query();
		query
				.setName("GMS registered practice on reference date")
				.select(s -> s
					.setPath(IM.NAMESPACE+"gpRegistration")
			  .property(p->p
				.setIri(IM.NAMESPACE+"Organisation")
				.setAlias("organisation")))
		  .from(f->f
				.setIri(IM.NAMESPACE+"Patient").setName("Patient"))
			.setWhere(new Where()
				.setProperty(IM.NAMESPACE+"gpRegistration")
				.and(pv->pv
					.setProperty(IM.NAMESPACE+"patientType")
					.setIs(new TTAlias().setIri(IM.GMS_PATIENT.getIri()).setName("Regular GMS patient")))
				.and(pv->pv
					.setProperty(IM.NAMESPACE+"startDate")
					.setValue(new Value()
						.setComparison("<=")
						.relativeTo(c->c
							.setVariable("$ReferenceDate"))))
				.or(pv-> pv
					.notExist(not->not
						.setProperty(IM.NAMESPACE+"endDate")))
				.or(pv->pv
					.setProperty(IM.NAMESPACE+"endDate")
					.setValue(new Value()
						.setComparison(">")
						.relativeTo(c ->c
							.setVariable("$referenceDate")))
				));

		gpRegPractice.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE+"query"));
		gpRegPractice
			.set(IM.DEFINITION,TTLiteral.literal(query));
		saveCore(manager.getDocument());
	}


	/*
	private static void gpGMSRegistrationDate() throws IOException {
		TTManager manager= loadCore();
		TTEntity gpRegDate= manager.getEntity("im:gpGMSRegistrationDate");
		gpRegDate.getPredicateMap().remove(SHACL.PARAMETER);
		Select select= new Select()
			.setName("Latest registration date if registered regular")
			.property(p->p.
				setAlias("registrationDate"))
			.match(m->m
				.addPathTo(new ConceptRef(IM.NAMESPACE+"hasEntry"))
				.setEntityType(TTIriRef.iri(IM.NAMESPACE+"GPRegistration"))
				.property(pv1-> pv1
					.setIri(IM.NAMESPACE + "effectiveDate")
					.setAlias("effectiveDate")
					.setValue(Comparison.LESS_THAN, "$ReferenceDate"))
				.order(o-> o
					.setOrderBy(new IriAlias().setAlias("registrationDate"))
					.setLimit(1)
					.setDirection(Order.DESCENDING))
				.testProperty(pv->pv
					.setIri(IM.NAMESPACE + "patientType")
					.addIsConcept(ConceptRef.iri(IM.GMS_PATIENT.getIri(),"GP GMS Regular patient type")))
				.testProperty(pv->pv
					.setNotExist(true)
					.setName("the registration has not ended ")
					.setIri(IM.NAMESPACE + "endDate")));
		gpRegDate
			.set(IM.QUERY_DEFINITION,TTLiteral.literal(Query.getJson(select)));;
		gpRegDate.getPredicateMap().remove(IM.QUERY_DEFINITION);
		saveCore(manager.getDocument());
		output(gpRegDate);
	}


	private static void gpRegistrationStatus() throws IOException {
		TTManager manager= loadCore();
		TTEntity gpStatus= manager.getEntity("im:gpRegisteredStatus");
		gpStatus.getPredicateMap().remove(SHACL.PARAMETER);
		Select select= new Select()
			.setName("Latest gp patient type")
			.property(p->p
				.setAlias("patientType"))
			.match(m->m
				.addPathTo(new ConceptRef(IM.NAMESPACE+"hasEntry"))
				.setEntityType(TTIriRef.iri(IM.NAMESPACE+"GPRegistration"))
				.property(pv-> pv
					.setIri(IM.NAMESPACE+"effectiveDate")
					.setAlias("effectiveDate")
					.value(c->c
						.setComparison(Comparison.LESS_THAN)
						.setValueVariable("$referenceDate")))
				.property(tp->tp
					.setIri(IM.NAMESPACE+"patientType")
					.setAlias("patientType"))
				.order(o-> o
					.setOrderBy(new IriAlias().setAlias("effectiveDate"))
					.setLimit(1)
					.setDirection(Order.DESCENDING)));

		gpStatus
			.set(IM.QUERY_DEFINITION,TTLiteral.literal(Query.getJson(select)));
		gpStatus.set(IM.FUNCTION_DEFINITION,TTLiteral.literal(Query.getJson(new Function()
			.addToConceptMap(IM.NAMESPACE+"2751000252106",IM.NAMESPACE+"1012571000252108")
			.setDefaultConcept(IM.NAMESPACE+"1012581000252106"))));
		gpStatus.getPredicateMap().remove(IM.QUERY_DEFINITION);
		saveCore(manager.getDocument());
		output(gpStatus);
	}
*/
	private void getAllowableRanges() throws JsonProcessingException {
		TTEntity query= getQuery("AllowableRanges");

		query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE+"query"));
		query.set(IM.DEFINITION,TTLiteral.literal(
			new Query()
				.setName("Allowable Ranges for a property")
				.setDescription("'using property domains get the allowable properties from the supertypes of this concept")
				.setActiveOnly(true)
				.from(f ->f
					.setType(IM.CONCEPT))
				.select(IM.CODE.getIri())
				.select(RDFS.LABEL.getIri())
				.where(w->w
					.setProperty(new TTAlias(RDFS.RANGE).setInverse(true))
					.setIs(new TTAlias().setVariable("$this").setIncludeSupertypes(true).setIncludeSubtypes(true))
				)));
	}

	private void getAllowableProperties() throws JsonProcessingException {
		TTEntity query= getQuery("AllowableProperties");

		query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE+"query"));
		query.set(IM.DEFINITION,TTLiteral.literal(
			new Query()
				.setName("Allowable Properties for a concept")
				.setDescription("'using property domains get the allowable properties from the supertypes of this concept")
				.setActiveOnly(true)
			.from(f ->f
				.setType(IM.CONCEPT).setIncludeSubtypes(true))
			.select(IM.CODE.getIri())
			.select(RDFS.LABEL.getIri())
			.where(w->w
					.setProperty(new TTAlias(RDFS.DOMAIN))
					.setIs(new TTAlias().setVariable("$this").setIncludeSupertypes(true))
				)));
	}

	private void getConcepts() throws JsonProcessingException {
		TTEntity query= getQuery("SearchConcepts");
		query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE+"query"));
		query.set(IM.DEFINITION,TTLiteral.literal(
			new Query()
				.setActiveOnly(true)
				.setName("Search for concepts")
				.from(w->w
					.setType(IM.CONCEPT))));
	}

	private void getIsas() throws JsonProcessingException {
		TTEntity query = getQuery("GetIsas");

		query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE+"query"));
		query.set(IM.DEFINITION,
			TTLiteral.literal(new Query()
				.setName("All subtypes of an entity, active only")
					.setActiveOnly(true)
				.from(w->w
					.setVariable("$this").setIncludeSubtypes(true))
				.select(s->s.setProperty(RDFS.LABEL))));
	}

	private TTEntity getQuery(String shortName) {
		return manager.getEntity(IM.NAMESPACE+"Query_"+shortName);
	}

	public static TTManager loadForms() throws IOException {

		String coreFile="C:\\Users\\david\\CloudStation\\EhealthTrust\\DiscoveryDataService\\ImportData\\DiscoveryCore\\FormQueries.json";
		TTManager manager= new TTManager();
		manager.loadDocument(new File(coreFile));
		return manager;
	}

	public static void saveForms(TTDocument document) throws IOException {
		String coreFile="C:\\Users\\david\\CloudStation\\EhealthTrust\\DiscoveryDataService\\ImportData\\DiscoveryCore\\FormQueries.json";
		TTManager manager= new TTManager();
		manager.setDocument(document);
		File core = new File(coreFile);
		manager.saveDocument(core);

	}

	public static TTManager loadCore() throws IOException {

		String coreFile="C:\\Users\\david\\CloudStation\\EhealthTrust\\DiscoveryDataService\\ImportData\\DiscoveryCore\\CoreOntology.json";
		TTManager manager= new TTManager();
		manager.loadDocument(new File(coreFile));
		return manager;
	}

	public static void saveCore(TTDocument document) throws IOException {
		String coreFile="C:\\Users\\david\\CloudStation\\EhealthTrust\\DiscoveryDataService\\ImportData\\DiscoveryCore\\CoreOntology.json";
		TTManager manager= new TTManager();
		manager.setDocument(document);
		File core = new File(coreFile);
		manager.saveDocument(core);

	}
}
