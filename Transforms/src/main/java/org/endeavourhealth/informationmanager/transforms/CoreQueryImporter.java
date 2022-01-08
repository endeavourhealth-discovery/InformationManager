package org.endeavourhealth.informationmanager.transforms;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTLiteral;
import org.endeavourhealth.imapi.query.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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


	private void addCurrentReg(TTDocument document) throws IOException {

		Query qry = new Query()
			.setType(IM.QUERY);
		qry
			.setIri(IM.NAMESPACE + "Q_RegisteredGMS")
			.setName("Patients registered for GMS services on the reference date")
			.setDescription("For any registration period,a registration start date before the reference date and no end date," +
				"or an end date after the reference date.");
			qry.setMainEntityVar("?patient");
			qry.setMainEntityType(TTIriRef.iri(IM.NAMESPACE+"Patient"));
		TTEntity rdf = new TTEntity()
			.setIri(qry.getIri())
			.setName(qry.getName())
			.setDescription(qry.getDescription())
			.addType(IM.QUERY);
		rdf.addObject(IM.IS_CONTAINED_IN, TTIriRef.iri(IM.NAMESPACE + "Q_StandardCohorts"));

		document.addEntity(rdf);

		qry.addSelect(new Select().setVar("?patient"));
		Clause gpReg = new Clause();
		qry.setOperator(Operator.AND);
		qry.addClause(gpReg);
		gpReg.addWhere(new Where()
			.setEntityVar("?patient")
			.setProperty(TTIriRef.iri("im:isSubjectOf"))
			.setValueEntity(TTIriRef.iri("im:GPRegistration"))
			.setValueVar("?reg"));
		gpReg.addWhere(new Where()
				.setEntityVar("?reg")
			.setProperty(IM.NAMESPACE + "patientType")
			.setValueVar("?patientType")
			.addFilter(new Filter().addIn(IM.GMS_PATIENT)));
		gpReg.addWhere(new Where()
		  .setEntityVar("?reg")
			.setProperty(IM.NAMESPACE + "effectiveDate")
			.setValueVar("?regDate")
			.addFilter(new Filter().setValueTest(Comparison.lessThanOrEqual, "$ReferenceDate")));

		Clause notEnded = new Clause();
		qry.addClause(notEnded);
		notEnded.setOperator(Operator.OR);
		Clause noEndDate= new Clause();
		notEnded.addClause(noEndDate);
		noEndDate.setNotExist(true);
		noEndDate.addWhere(new Where()
			.setEntityVar("?reg")
			.setProperty(IM.NAMESPACE + "endDate"));
		Clause leftAfter= new Clause();
		notEnded.addClause(leftAfter);
		leftAfter.addWhere(new Where()
			.setEntityVar("?reg")
			.setProperty(TTIriRef.iri(IM.NAMESPACE + "endDate"))
			.setValueVar("?endDate")
			.addFilter(new Filter()
				.setValueTest(Comparison.greaterThan, "$ReferenceDate")));

		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
		objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
		String json = objectMapper.writeValueAsString(qry);
		rdf.set(IM.QUERY_DEFINITION, TTLiteral.literal(json));
		output(json);

	}


	@Override
	public TTImport validateFiles(String inFolder) throws TTFilerException {
		return null;
	}


	private void output(String json) throws IOException {
		try (FileWriter writer = new FileWriter("c:/temp/Core-qry.json")) {
			writer.write(json);
		}

	}
}
