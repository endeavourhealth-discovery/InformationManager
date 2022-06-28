package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.cdm.ProvActivity;
import org.endeavourhealth.imapi.model.cdm.ProvAgent;
import org.endeavourhealth.imapi.model.sets.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;


import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;

public class CoreQueryImporter implements TTImport {

	@Override
	public TTImport importData(TTImportConfig config) throws Exception {
		TTManager manager = new TTManager();
		TTDocument document = manager.createDocument(IM.GRAPH_DISCOVERY.getIri());
		output(document);

		addCurrentReg(document,config.getFolder());
		output(document);
		try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
			filer.fileDocument(document);
		}

		return null;
	}

	private void addCurrentReg(TTDocument document,String outFolder) throws IOException {
		TTEntity qry = new TTEntity().addType(IM.QUERY);
		qry
			.setIri(IM.NAMESPACE + "Q_RegisteredGMS")
			.setName("Patients registered for GMS services on the reference date")
			.setDescription("For any registration period,a registration start date before the reference date and no end date," +
				"or an end date after the reference date.");
		Query prof= new Query();
		prof.setMainEntity(TTIriRef.iri(IM.NAMESPACE+"Person"));
		prof.setIri(qry.getIri());
		prof.setName(qry.getName());
		prof.setDescription(qry.getDescription());
		prof.setSelect(new Select()
				.setEntityType(TTIriRef.iri(IM.NAMESPACE+"Person").setName("Person"))
			.addMatch(new Match()
				.addPathTo(new ConceptRef(IM.NAMESPACE+"isSubjectOf").setName("has GP registration"))
				.setName(prof.getName())
			.setEntityType(TTIriRef.iri(IM.NAMESPACE+"GPRegistration"))
			.property(pv-> pv
				.setName("patient type is regular GMS Patient")
				.setIri(IM.NAMESPACE + "patientType")
				.addIsConcept(ConceptRef.iri(IM.GMS_PATIENT.getIri(),"Regular GMS patient")))
			.property(pv->pv
				.setIri(IM.NAMESPACE + "effectiveDate")
				.setName("start of registration is before the reference date")
				.setValue(Comparison.LESS_THAN_OR_EQUAL, "$ReferenceDate"))
			.orProperty(pv-> pv
				.setNotExist(true)
				.setName("the registration has not ended ")
					.setIri(IM.NAMESPACE + "endDate"))
			.orProperty(pv-> pv
				.setIri(IM.NAMESPACE + "endDate")
				.setName("the end of registration is after the reference date")
				.setValue(Comparison.GREATER_THAN, "$ReferenceDate"))));

		qry.set(IM.QUERY_DEFINITION,TTLiteral.literal(prof.getasJson()));
		qry.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"Q_StandardCohorts"));
		document.addEntity(qry);
		document.setContext(TTUtil.getDefaultContext());
		setProvenance(qry,document);
		outputQuery(prof);
	}

	private void setProvenance(TTEntity rdf,TTDocument document) {
		ProvAgent agent= new ProvAgent()
			.setPersonInRole(TTIriRef.iri("http://uir.endhealth.org#Stables1"))
			.setParticipationType(IM.AUTHOR_ROLE);
		agent.setIri("http://agent.endhealth.org#Stables1");
		document.addEntity(agent);
		ProvActivity activity= new ProvActivity()
			.setIri("http://prov.endhealth.info/im#Q_RegisteredGMS")
			.setActivityType(IM.PROV_CREATION)
			.setEffectiveDate(LocalDateTime.now().toString())
			.addAgent(TTIriRef.iri(agent.getIri()))
			.setTargetEntity(TTIriRef.iri(rdf.getIri()));
		document.addEntity(activity);

	}


	@Override
	public TTImport validateFiles(String inFolder) throws TTFilerException {
		return null;
	}


	private void output(TTDocument document) throws IOException {
		if ( ImportApp.testDirectory!=null) {
			String directory = ImportApp.testDirectory.replace("%", " ");
			try (FileWriter writer = new FileWriter(directory + "\\Core-qry-LD.json")) {
				ObjectMapper objectMapper = new ObjectMapper();
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
				String doc = objectMapper.writerWithDefaultPrettyPrinter()
					.withAttribute(TTContext.OUTPUT_CONTEXT, true).writeValueAsString(document);
				writer.write(doc);
			}
		}

	}


	private void outputQuery(Query qry) throws IOException {
		if ( ImportApp.testDirectory!=null) {
			String directory = ImportApp.testDirectory.replace("%", " ");
			try (FileWriter writer = new FileWriter(directory + "\\Core-qry.json")) {
				ObjectMapper objectMapper = new ObjectMapper();
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
				String doc = objectMapper.writerWithDefaultPrettyPrinter()
					.withAttribute(TTContext.OUTPUT_CONTEXT, true).writeValueAsString(qry);
				writer.write(doc);
			}
		}

	}
}
