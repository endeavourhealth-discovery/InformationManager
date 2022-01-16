package org.endeavourhealth.informationmanager.transforms;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.query.Query;
import org.endeavourhealth.imapi.query.QueryDocument;
import org.endeavourhealth.imapi.transforms.EqdToQuery;
import org.endeavourhealth.imapi.transforms.EqdToTT;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.transforms.eqd.EnquiryDocument;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.*;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.zip.DataFormatException;

public class CEGEqdImporter implements TTImport {
	private TTDocument document;
	private TTEntity owner;

	private static final String[] queries = {".*\\\\CEGQuery"};
	private static final String[] annotations = {".*\\\\QueryAnnotations.properties"};
	private static final String[] dataMapFile = {".*\\\\EMIS\\\\EqdDataMap.properties"};
	private static final String[] duplicates = {".*\\\\CEGQuery\\\\DuplicateOrs.properties"};
	@Override
	public TTImport importData(TTImportConfig config) throws Exception {
		TTManager manager= new TTManager();
		document= manager.createDocument(IM.GRAPH_CEG_QUERY.getIri());
		createOrg();
		createFolders();
		loadAndConvert(config.folder);
		DeDuplicateClauses();
		for (TTEntity entity:document.getEntities()){
			if (entity.isType(IM.CONCEPT_SET))
				entity.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.GRAPH_CEG_QUERY.getIri()+"CSET_CEGConceptSets"));
		}

		try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
			filer.fileDocument(document);
		}
		return this;
	}

	private void DeDuplicateClauses() {
	}

	private void Deduplicate() {
	}

	private void createFolders() {
		TTEntity folder= new TTEntity()
			.setIri(IM.GRAPH_CEG_QUERY.getIri()+"Q_CEGQueries")
			.setName("QMUL CEG query library")
			.addType(IM.FOLDER)
			.set(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"QT_QueryTemplates"));
		document.addEntity(folder);
		folder= new TTEntity()
			.setIri(IM.GRAPH_CEG_QUERY.getIri()+"CSET_CEGConceptSets")
			.setName("QMUL CEG concept set library")
			.addType(IM.FOLDER)
			.set(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"Sets"));
		document.addEntity(folder);
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
		Map<String,String> reportNames= new HashMap<>();
		Properties criteriaLabels= new Properties();
		dataMap.load(new FileReader((ImportUtils.findFileForId(folder, dataMapFile[0]).toFile())));
		Properties duplicateOrs= new Properties();
		duplicateOrs.load(new FileReader((ImportUtils.findFileForId(folder, duplicates[0]).toFile())));
		EqdToQuery.duplicates= duplicateOrs;
		criteriaLabels.load(new FileReader((ImportUtils.findFileForId(folder, annotations[0]).toFile())));
		Path directory= ImportUtils.findFileForId(folder,queries[0]);
		TTIriRef mainFolder= TTIriRef.iri(IM.GRAPH_CEG_QUERY.getIri()+"Q_CEGQueries");
		for (File fileEntry : Objects.requireNonNull(directory.toFile().listFiles())) {
			if (!fileEntry.isDirectory()) {
				String ext= FilenameUtils.getExtension(fileEntry.getName());
				if (ext.equalsIgnoreCase("xml")) {
					JAXBContext context = JAXBContext.newInstance(EnquiryDocument.class);
					EnquiryDocument eqd = (EnquiryDocument) context.createUnmarshaller()
						.unmarshal(new FileReader(fileEntry));
					EqdToTT converter= new EqdToTT();
					converter.convertDoc(document,mainFolder,eqd,
						IM.GRAPH_CEG_QUERY,
						TTIriRef.iri(owner.getIri()),dataMap,
						criteriaLabels,reportNames);
				  output(fileEntry);
				}
			}
		}

	}

	private void output(File fileEntry) throws IOException {
		TTManager manager= new TTManager();
		manager.setDocument(document);
		manager.saveDocument(new File("c:/temp/"+ fileEntry.getName().replace(".xml","")+"-ld.json"));
		QueryDocument qdoc= new QueryDocument();
		for (TTEntity entity:document.getEntities()){
			if (entity.isType(IM.QUERY)) {
				Query qry = new Query();
				qry = qry.fromEntity(entity);
				qdoc.addQuery(qry);
			}
		}

		try (FileWriter writer= new FileWriter("c:/temp/"+ fileEntry.getName().replace(".xml","")+ "-qry.json")) {
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
			String doc = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(EqdToTT.qDocument);
			writer.write(doc);
		}

	}


	@Override
	public TTImport validateFiles(String inFolder) throws TTFilerException {
		ImportUtils.validateFiles(inFolder,queries,annotations,duplicates);
		return this;
	}


}
