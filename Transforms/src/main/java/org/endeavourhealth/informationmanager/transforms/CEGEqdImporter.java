package org.endeavourhealth.informationmanager.transforms;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.transforms.EqdToTT;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.transforms.eqd.EnquiryDocument;
import org.endeavourhealth.imapi.vocabulary.IM;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.DataFormatException;

public class CEGEqdImporter implements TTImport {
	private TTDocument document;
	private TTEntity owner;
	private Set<TTEntity> allEntities = new HashSet<>();

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
		Properties criteriaLabels= new Properties();
		dataMap.load(new FileReader((ImportUtils.findFileForId(folder, dataMapFile[0]).toFile())));
		Properties duplicateOrs= new Properties();
		duplicateOrs.load(new FileReader((ImportUtils.findFileForId(folder, duplicates[0]).toFile())));
		criteriaLabels.load(new FileReader((ImportUtils.findFileForId(folder, annotations[0]).toFile())));
		Path directory= ImportUtils.findFileForId(folder,queries[0]);
		TTIriRef mainFolder= TTIriRef.iri(IM.GRAPH_CEG_QUERY.getIri()+"Q_CEGQueries");
		for (File fileEntry : Objects.requireNonNull(directory.toFile().listFiles())) {
			if (!fileEntry.isDirectory()) {
				String ext= FilenameUtils.getExtension(fileEntry.getName());
				if (ext.equalsIgnoreCase("xml")) {
                    System.out.println("..." + fileEntry.getName());
					JAXBContext context = JAXBContext.newInstance(EnquiryDocument.class);
					EnquiryDocument eqd = (EnquiryDocument) context.createUnmarshaller()
						.unmarshal(new FileReader(fileEntry));
					EqdToTT converter= new EqdToTT();
					converter.convertDoc(document,mainFolder,eqd,
						TTIriRef.iri(owner.getIri()),dataMap,
						criteriaLabels);
				  //output(fileEntry);
				}
			}
		}

	}

	private void output(File fileEntry) throws IOException {

		TTManager manager= new TTManager();
		TTDocument qDocument= manager.createDocument(IM.GRAPH_CEG_QUERY.getIri());
		for (TTEntity entity:document.getEntities()) {
			if (entity.isType(IM.PROFILE)) {
				if (!allEntities.contains(entity)) {
					qDocument.addEntity(entity);
				}
			}
			allEntities.add(entity);
		}
		manager.setDocument(qDocument);
		manager.saveDocument(new File("G:\\Shared drives\\Discovery Data Service\\InformationModel\\ImportData\\CEGQuery\\" + fileEntry.getName().replace(".xml", "") + "-profiles-ld.json"));
		manager.setDocument(document);
		manager.saveDocument(new File("G:\\Shared drives\\Discovery Data Service\\InformationModel\\ImportData\\CEGQuery\\CEG-Queries.json"));;

	}


	@Override
	public TTImport validateFiles(String inFolder) throws TTFilerException {
		ImportUtils.validateFiles(inFolder,queries,annotations,duplicates);
		return this;
	}


}
