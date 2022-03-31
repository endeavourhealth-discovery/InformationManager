package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.rdf4j.model.IRI;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.cdm.ProvActivity;
import org.endeavourhealth.imapi.model.cdm.ProvAgent;
import org.endeavourhealth.imapi.model.query.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;

public class CoreQueryImporter implements TTImport {
	private Query query;
	@Override
	public TTImport importData(TTImportConfig config) throws Exception {
		TTManager manager = new TTManager();
		TTDocument document = manager.createDocument(IM.GRAPH_DISCOVERY.getIri());

		addCurrentReg(document,config.getFolder());
		try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
			filer.fileDocument(document);
		}

		return null;
	}

	private void addCurrentReg(TTDocument document,String folder) throws IOException {
		TTEntity qry = new TTEntity().addType(IM.QUERY);
		qry
			.setIri(IM.NAMESPACE + "Q_RegisteredGMS")
			.setName("Patients registered for GMS services on the reference date")
			.setDescription("For any registration period,a registration start date before the reference date and no end date," +
				"or an end date after the reference date.");
		Query prof= new Query();
		query= prof;
		prof.setId(TTIriRef.iri(qry.getIri()));
		prof.setName(qry.getName());
		prof.setDescription(qry.getDescription());
		prof.setReturn(new Return().addField(new ReturnField().setPath("id")));
		Match match= new Match();
		prof.setMatch(match);
		match.setEntityType(getIri("Person"));
		match.setProperty(getIri("isSubjectOf"));
		match.setObject(new Match());
		Match reg= match.getObject();
		reg.setEntityType(getIri("GPRegistration"));
		Match regtype= reg.addAnd();
		regtype
			.setProperty(getIri("patientType"))
			.addValueIn(getIri("2751000252106"));
		Match regdate= reg.addAnd();
		regdate
			.setProperty(getIri("effectiveDate"))
			.setCompare(Comparison.LESS_THAN_OR_EQUAL, "$ReferenceDate");
		Match ends= reg.addAnd();
		ends
			.addOr(new Match()
				.setNotExists(true)
					.setProperty(getIri("endDate")));
		ends
			.addOr(new Match()
				.setProperty(getIri("endDate"))
				.setCompare(Comparison.GREATER_THAN, "$ReferenceDate"));

		output(IMQLFactory.getJson(prof),folder);

		qry.set(IM.DEFINITION,TTLiteral.literal(IMQLFactory.getJson(prof)));
		qry.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"Q_StandardCohorts"));
		document.addEntity(qry);
		document.setContext(TTUtil.getDefaultContext());
		setProvenance(qry,document);
	}

	private TTIriRef getIri(String lname) throws IOException {
		TTIriRef result= TTIriRef.iri(IM.NAMESPACE+lname);
		String name= new ImportMaps().getCoreName(result.getIri());
		if (name!=null)
			result.setName(name);
		return result;
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


	private void output(String json, String infolder) throws IOException {
		try (FileWriter writer= new FileWriter(infolder + "\\DiscoveryCore\\Core-qry-V2.json")) {
			writer.write(json);
		}

	}
}
