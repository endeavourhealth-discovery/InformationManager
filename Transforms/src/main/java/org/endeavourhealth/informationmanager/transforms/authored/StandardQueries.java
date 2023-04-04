package org.endeavourhealth.informationmanager.transforms.authored;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTLiteral;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SHACL;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class StandardQueries {

	private TTManager manager;

	public void buildQueries() throws IOException {
		manager= loadForms();
		getIsas();
		getConcepts();
		getAllowableProperties();
		getAllowableRanges();
		allowableSubTypes();
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
						.setIri(IM.NAMESPACE+"Organisation")
					.setVariable("practice"))
		  .match(f->f
				.setType("Patient").setName("Patient")
				.path(p->p
				.setIri("gpRegistration"))
				.setBool(Bool.and)
				.where(pv->pv
					.setIri("patientType")
					.addIn(new Element().setIri(IM.GMS_PATIENT.getIri()).setName("Regular GMS patient")))
				.where(pv->pv
					.setIri("effectiveDate")
					.setOperator(Operator.lte)
					.setRelativeTo(new Property().setParameter("$referenceDate")))
				.where(pv->pv
					.setBool(Bool.or)
					.where(pv1->pv1
						.setNull(true)
						.setIri("endDate"))
					.where(pv1->pv1
					.setIri("endDate")
					.setOperator(Operator.gt)
						.setRelativeTo(new Property().setParameter("$referenceDate")))));

		gpRegPractice.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE+"query"));
		gpRegPractice
			.set(IM.DEFINITION,TTLiteral.literal(query));
		saveCore(manager.getDocument());
	}




	private void allowableSubTypes() throws IOException {
		TTEntity entity= getQuery("AllowableChildTypes");
		if (entity==null) {
			entity = new TTEntity();
			manager.getDocument().addEntity(entity);
		}
		entity
			.setIri(IM.NAMESPACE+"Query_AllowableChildTypes")
			.addType(IM.QUERY)
			.setName("For a parent type, allowable child entity types and their predicates connecting to their parent");
		entity.addObject(IM.IS_CONTAINED_IN, TTIriRef.iri(IM.NAMESPACE+"IMEditorQueries"));

		Query query= new Query();
		query.setName("Allowable child types for editor");
		query
			.match(f->f
				.where(w1->w1.setId(IM.IS_CONTAINED_IN.getIri())
					.addIn(IM.NAMESPACE+"EntityTypes")))
			.match(w1->w1
				.path(p->p
					.setId(SHACL.PROPERTY.getIri()))
				.setBool(Bool.and)
				.where(a2->a2
					.setIri(SHACL.NODE.getIri())
					.addIn(new Match().setParameter("$this")))
				.where(a2->a2
					.setIri(SHACL.PATH.getIri())
					.setIn(List.of(Element.iri(IM.IS_CONTAINED_IN.getIri())
						, Match.iri(RDFS.SUBCLASSOF.getIri()), Match.iri(IM.IS_SUBSET_OF.getIri())))))
			.select(s->s
				.setId(RDFS.LABEL.getIri()))
			.select(s->s
				.setId(SHACL.PROPERTY.getIri())

				.select(s1->s1
					.setIri(SHACL.PATH.getIri())));
		entity.set(IM.DEFINITION, TTLiteral.literal(query));

	}
	private void getAllowableRanges() throws JsonProcessingException {
		TTEntity query= getQuery("AllowableRanges");

		query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE+"query"));
		query.set(IM.DEFINITION,TTLiteral.literal(
			new Query()
				.setName("Allowable Ranges for a property")
				.setDescription("'using property domains get the allowable properties from the supertypes of this concept")
				.setActiveOnly(true)
				.select(s->s.setIri(IM.CODE.getIri()))
				.select(s->s.setIri(RDFS.LABEL.getIri()))
				.match(f ->f
					.setType(IM.CONCEPT.getIri())
				.where(w->w
					.setIri(RDFS.RANGE.getIri())
					.setInverse(true)
					.addIn(new Element().setParameter("this")
						.setAncestorsOf(true)
						.setDescendantsOrSelfOf(true))
				))));
	}

	private void getAllowableProperties() throws JsonProcessingException {
		TTEntity query= getQuery("AllowableProperties");

		query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE+"query"));
		query.set(IM.DEFINITION,TTLiteral.literal(
			new Query()
				.setName("Allowable Properties for a concept")
				.setDescription("'using property domains get the allowable properties from the supertypes of this concept")
				.setActiveOnly(true)
				.select(s->s.setIri(IM.CODE.getIri()))
				.select(s->s.setIri(RDFS.LABEL.getIri()))
			.match(f ->f
				.setType(IM.CONCEPT.getIri())
			.where(w->w
					.setIri(RDFS.DOMAIN.getIri())
					.addIn(new Element().setParameter("this").setAncestorsOf(true))
				))));
	}

	private void getConcepts() throws JsonProcessingException {
		TTEntity query= getQuery("SearchConcepts");
		query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE+"query"));
		query.set(IM.DEFINITION,TTLiteral.literal(
			new Query()
				.setActiveOnly(true)
				.setName("Search for concepts")
				.match(w->w
					.setType(IM.CONCEPT.getIri()))));
	}

	private void getIsas() throws JsonProcessingException {
		TTEntity query = getQuery("GetIsas");

		query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE+"query"));
		query.set(IM.DEFINITION,
			TTLiteral.literal(new Query()
				.setName("All subtypes of an entity, active only")
					.setActiveOnly(true)
				.match(w->w
					.setParameter("this")
					.setDescendantsOrSelfOf(true))
				.select(s->s.setIri(RDFS.LABEL.getIri()))));
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
