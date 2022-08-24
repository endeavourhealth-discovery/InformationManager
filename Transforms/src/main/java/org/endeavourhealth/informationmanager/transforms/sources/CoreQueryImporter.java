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
import org.endeavourhealth.imapi.transforms.TTToClassObject;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;


import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.LocalDateTime;

public class CoreQueryImporter implements TTImport {

	@Override
	public void importData(TTImportConfig config) throws Exception {
		TTManager manager = new TTManager();
		TTDocument document = manager.createDocument(IM.GRAPH_DISCOVERY.getIri());
		output(document);

		addCurrentReg(document,config.getFolder());
		output(document);
		try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
			filer.fileDocument(document);
		}
    }

	private void addCurrentReg(TTDocument document,String outFolder) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
		TTEntity qry = new TTEntity().addType(IM.QUERY);
		qry
			.setIri(IM.NAMESPACE + "Q_RegisteredGMS")
			.setName("Patients registered for GMS services on the reference date")
			.setDescription("For any registration period,a registration start date before the reference date and no end date," +
				"or an end date after the reference date.");
		Query prof= new Query();
		prof.setMainEntity(TTIriRef.iri(IM.NAMESPACE+"Patient"));
		prof.setIri(qry.getIri());
		prof.setName(qry.getName());
		prof.setDescription(qry.getDescription());
		prof.setSelect(new Select()
				.setEntityType(TTIriRef.iri(IM.NAMESPACE+"Patient").setName("Patient"))
			.addMatch(new Match()
				.addPathTo(new ConceptRef(IM.NAMESPACE+"hasEntry").setName("has GP registration"))
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

		qry.set(IM.QUERY_DEFINITION,TTLiteral.literal(prof.getJson()));
		qry.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"Q_StandardCohorts"));
		document.addEntity(qry);
		document.setContext(TTUtil.getDefaultContext());
		setProvenance(qry,document);
		outputQuery(new TTToClassObject().getObject(qry,QueryEntity.class));
	}

	private void setProvenance(TTEntity rdf,TTDocument document) {
		ProvActivity activity= new ProvActivity()
			.setIri("http://prov.endhealth.info/im#Q_RegisteredGMS")
			.setActivityType(IM.PROV_CREATION)
			.setEffectiveDate(LocalDateTime.now().toString())
			.setTargetEntity(TTIriRef.iri(rdf.getIri()));
		document.addEntity(activity);

	}


	@Override
	public void validateFiles(String inFolder) throws TTFilerException {
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


	private void outputQuery(QueryEntity qry) throws IOException {
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

    @Override
    public void close() throws Exception {

    }
}
