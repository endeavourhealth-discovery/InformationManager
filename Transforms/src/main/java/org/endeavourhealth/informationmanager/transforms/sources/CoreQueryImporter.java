package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.cdm.ProvActivity;
import org.endeavourhealth.imapi.model.cdm.ProvAgent;
import org.endeavourhealth.imapi.model.hql.Comparison;
import org.endeavourhealth.imapi.model.hql.Match;
import org.endeavourhealth.imapi.model.hql.Profile;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;

public class CoreQueryImporter implements TTImport {
	@Override
	public TTImport importData(TTImportConfig config) throws Exception {
		TTManager manager = new TTManager();
		TTDocument document = manager.createDocument(IM.GRAPH_DISCOVERY.getIri());
		addCurrentReg(document);
		try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
			filer.fileDocument(document);
		}

		return null;
	}

	private void addCurrentReg(TTDocument document) throws JsonProcessingException {
		TTEntity qry = new TTEntity().addType(IM.PROFILE);
		qry
			.setIri(IM.NAMESPACE + "Q_RegisteredGMS")
			.setName("Patients registered for GMS services on the reference date")
			.setDescription("For any registration period,a registration start date before the reference date and no end date," +
				"or an end date after the reference date.");
		Profile prof= new Profile();
		prof.setEntityType(TTIriRef.iri(IM.NAMESPACE+"Person"));
		Match match= prof.setMatch();
		match.setPathTo(TTIriRef.iri(IM.NAMESPACE+"isSubjectOf"));
		match.setEntityType(TTIriRef.iri(IM.NAMESPACE+"GPRegistration"));
		Match regtype= match.addAnd();
		regtype
			.setProperty(TTIriRef.iri(IM.NAMESPACE + "patientType"))
			.addValueIn(IM.GMS_PATIENT);
		Match regdate= match.addAnd();
		regdate
			.setProperty(TTIriRef.iri(IM.NAMESPACE + "effectiveDate"))
			.setValueCompare(Comparison.LESS_THAN_OR_EQUAL, "$ReferenceDate");
		Match ends= match.addAnd();
		ends
			.addOr(new Match()
				.setNotExist(true)
					.setProperty(TTIriRef.iri(IM.NAMESPACE + "endDate")));
		ends
			.addOr(new Match()
				.setProperty(TTIriRef.iri(IM.NAMESPACE + "endDate"))
				.setValueCompare(Comparison.GREATER_THAN, "$ReferenceDate"));

		qry.set(IM.DEFINITION,TTLiteral.literal(prof.getasJson()));
		document.addEntity(qry);
		document.setContext(TTUtil.getDefaultContext());
		setProvenance(qry,document);
	}

	private void setProvenance(TTEntity rdf,TTDocument document) {
		ProvAgent agent= new ProvAgent()
			.setPersonInRole(TTIriRef.iri("http://uir.endhealth.org#Stables1"))
			.setParticipationType(IM.AUTHOR_ROLE);
		agent.setIri("http://agent.endhealth.org#Stables1");
		document.addEntity(agent);
		ProvActivity activity= new ProvActivity()
			.setIri("http://prov.endhealth.info/im#Q_RegisteredGMS")
			.setActivityType(IM.CREATION)
			.setEffectiveDate(LocalDateTime.now().toString())
			.addAgent(TTIriRef.iri(agent.getIri()))
			.setTargetEntity(TTIriRef.iri(rdf.getIri()));
		document.addEntity(activity);

	}


	@Override
	public TTImport validateFiles(String inFolder) throws TTFilerException {
		return null;
	}


	private void output(TTDocument document, String infolder) throws IOException {
		try (FileWriter writer= new FileWriter(infolder + "Core-qry.json")) {
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
