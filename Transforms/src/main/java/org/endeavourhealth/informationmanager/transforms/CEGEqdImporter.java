package org.endeavourhealth.informationmanager.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.compress.utils.FileNameUtils;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.query.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.TTFilerException;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.TTImportConfig;
import org.endeavourhealth.informationmanager.transforms.eqd.EnquiryDocument;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.zip.DataFormatException;

public class CEGEqdImporter implements TTImport {
	private TTDocument document;
	private TTEntity owner;

	private static final String[] queries = {".*\\\\CEGQuery"};
	private static final String[] dataMapFile = {".*\\\\EMIS\\\\EqdDataMap.properties"};
	@Override
	public TTImport importData(TTImportConfig config) throws Exception {
		TTManager manager= new TTManager();
		document= manager.createDocument(IM.GRAPH_CEG_QUERY.getIri());
		createOrg();
		addCurrentReg();
		loadAndConvert(config.folder);
		return this;
	}

	private void addCurrentReg() throws JsonProcessingException {
		TTEntity entity= new TTEntity()
			.setIri(IM.NAMESPACE+"Q_RegisteredGMS")
			.setName("Patients registered for GMS services on the reference date")
			.setDescription("For any registration period,a registration start date before the reference date and no end date,"+
				"or an end date after the reference date.")
			.addType(IM.QUERY);
		document.addEntity(entity);
		Query qry= new Query();
		entity.set(IM.QUERY_DEFINITION,qry);
		Clause gpReg= new Clause();
		qry.setOperator(Operator.AND);
		qry.addClause(gpReg);
		gpReg.setOperator(Operator.AND);
		gpReg.addSelect(new Select().setVar("?patient"));
		gpReg.setOperator(Operator.AND);
		Where isRegGms= new Where();
		gpReg.addWhere(isRegGms);
		isRegGms
			.addEntity(new IriVar(TTIriRef.iri("im:Patient")).setVar("?patient"))
			.addEntity(new IriVar(TTIriRef.iri("im:isSubjectOf")))
			.addEntity(new IriVar(TTIriRef.iri("im:GPRegistration")).setVar("?reg"))
			.setProperty(IM.NAMESPACE+"patientType")
			.setValueVar("?patientType")
			.addFilter(new Filter().addIn(IM.GMS_PATIENT));
		Where hasRegDate= new Where();
		gpReg.addWhere(hasRegDate);
		hasRegDate.addEntityVar("?reg")
			.setProperty(IM.NAMESPACE+"effectiveDate")
			.setValueVar("?regDate")
			.addFilter(new Filter().setValueTest(Comparison.lessThanOrEqual,"$ReferenceDate"));

		Clause notEnded= new Clause();
		qry.addClause(notEnded);
		notEnded.setOperator(Operator.OR);
		notEnded.addWhere(new Where()
			.addEntityVar("?reg")
			.setNot(true)
			.setProperty(IM.NAMESPACE+"endDate"));
		notEnded.addWhere(new Where()
			.addEntityVar("?reg")
			.setProperty(TTIriRef.iri(IM.NAMESPACE+"endDate"))
			.setValueVar("?endDate")
			.addFilter(new Filter()
				.setValueTest(Comparison.greaterThan,"$ReferenceDate")));

	}

	private void createOrg() {
		owner= new TTEntity()
			.setIri("http://org.endhealth.info/im#QMUL_CEG")
			.addType(TTIriRef.iri(IM.NAMESPACE+"Organisation"))
			.setName("Clinical Effectiveness Group of Queen Mary Universitly of London - CEG");
		document.addEntity(owner);
	}

	public void loadAndConvert(String folder) throws JAXBException, IOException, DataFormatException {
		Properties dataMap= new Properties();
		dataMap.load(new FileReader((ImportUtils.findFileForId(folder, dataMapFile[0]).toFile())));
		Path directory= ImportUtils.findFileForId(folder,queries[0]);
		for (File fileEntry : Objects.requireNonNull(directory.toFile().listFiles())) {
			if (!fileEntry.isDirectory()) {
				String ext = FileNameUtils.getExtension(fileEntry.getName());
				if (ext.equalsIgnoreCase("xml")) {
					JAXBContext context = JAXBContext.newInstance(EnquiryDocument.class);
					EnquiryDocument eqd = (EnquiryDocument) context.createUnmarshaller()
						.unmarshal(new FileReader(fileEntry));
					EqdToTT converter= new EqdToTT();
					converter.convertDoc(document,eqd,
						IM.GRAPH_CEG_QUERY,
						TTIriRef.iri(owner.getIri()),dataMap);
					output(document);
				}
			}
		}

	}

	private void output(TTDocument document) throws JsonProcessingException {
		TTManager manager= new TTManager();
		manager.setDocument(document);
		manager.saveDocument(new File("c:/temp/CEGQueries.json"));
	}


	@Override
	public TTImport validateFiles(String inFolder) throws TTFilerException {
		ImportUtils.validateFiles(inFolder,queries);
		return this;
	}


}
