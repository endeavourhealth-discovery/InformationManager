package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.xml.bind.JAXBContext;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.iml.ConceptSet;
import org.endeavourhealth.imapi.model.iml.Entity;
import org.endeavourhealth.imapi.model.iml.ModelDocument;
import org.endeavourhealth.imapi.model.imq.QueryEntity;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.EqdToIMQ;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.transforms.eqd.EnquiryDocument;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class SmartLifeImporter implements TTImport {
	private static final Logger LOG = LoggerFactory.getLogger(CEGImporter.class);

	private static final String[] queries = {".*\\\\Smartlife"};
	private static String[] dataMapFile = {".*\\\\EMIS\\\\EqdDataMap.properties"};
	private TTDocument document;
	private String mainFolder;
	private String setFolder;

	@Override
	public void importData(TTImportConfig config) throws ImportException {
		try (TTManager manager = new TTManager()){
			document = manager.createDocument(GRAPH.SMARTLIFE);
			TTEntity graph = new TTEntity()
				.setIri(GRAPH.SMARTLIFE)
				.setName("Smartlife health graph")
				.setDescription("Smartlife library of value sets, queries and profiles")
				.addType(iri(IM.GRAPH));
			graph.addObject(iri(RDFS.SUBCLASS_OF), iri(IM.GRAPH));
			document.addEntity(graph);
			createFolders(document);
			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
				filer.fileDocument(document);

			}
			catch (Exception ex) {
				throw new ImportException(ex.getMessage(), ex);
			}
			try {
				loadAndConvert(config.getFolder());
			}
			catch (Exception ex) {
				throw new ImportException(ex.getMessage(), ex);
			}
		}
	}


	private void createFolders(TTDocument document) {
		TTEntity folder = new TTEntity()
			.setIri(GRAPH.SMARTLIFE + "Q_SmartLifeQueries")
			.setName("SmartLife queries")
			.addType(iri(IM.FOLDER))
			.set(iri(IM.IS_CONTAINED_IN), iri(IM.NAMESPACE + "Q_Queries"));
		folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.QUERY));
		document.addEntity(folder);
		mainFolder= folder.getIri();
		folder = new TTEntity()
			.setIri(GRAPH.SMARTLIFE + "CSET_SmartLifeConceptSets")
			.setName("QMUL CEG value set library")
			.addType(iri(IM.FOLDER))
			.set(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "QueryConceptSets"));
		folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.CONCEPT_SET));
		document.addEntity(folder);
		setFolder= folder.getIri();

	}


	public void loadAndConvert(String folder) throws Exception {
		Properties dataMap = new Properties();
		if (ImportApp.resourceFolder!=null) {
			try (FileReader reader = new FileReader((ImportApp.resourceFolder))) {
				dataMap.load(reader);
			}
		}
			else{
				try (FileReader reader = new FileReader((ImportUtils.findFileForId(folder, dataMapFile[0]).toFile()))) {
					dataMap.load(reader);
				}
			}


		Path directory = ImportUtils.findFileForId(folder, queries[0]);
		try (TTManager manager= new TTManager()) {
			EQDImporter eqdImporter = new EQDImporter(manager,dataMap,mainFolder,setFolder);
			eqdImporter.importEqds(GRAPH.SMARTLIFE, directory);
		}
	}




	@Override
	public void validateFiles(String inFolder) throws TTFilerException {
		ImportUtils.validateFiles(inFolder, queries);
	}


	@Override
	public void close() throws Exception {

	}
}
