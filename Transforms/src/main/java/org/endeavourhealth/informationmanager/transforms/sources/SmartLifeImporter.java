package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.vocabulary.Graph;
import org.endeavourhealth.imapi.vocabulary.Namespace;
import org.endeavourhealth.informationmanager.transforms.ZipUtils;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class SmartLifeImporter implements TTImport {
	private static final Logger LOG = LoggerFactory.getLogger(CEGImporter.class);

	private static final String[] queries = {".*\\\\Smartlife"};
	private static final String[] libraries = {".*\\\\Smartlife\\\\Library\\\\Library.zip"};
	private static final String[] dataMapFile = {".*\\\\EMIS\\\\EqdDataMap.properties"};
	private String mainFolder;
	private String setFolder;
	private TTImportConfig config;

	@Override
	public void importData(TTImportConfig config) throws ImportException {
		this.config=config;
		try {
			Path zip = ImportUtils.findFileForId(config.getFolder(), libraries[0]);
			File file = ZipUtils.unzipFile(zip.getFileName().toString(), zip.getParent().toString());
		} catch (IOException e) {
			throw new ImportException("Unable to unzip smartlife library",e);
		}

		try (TTManager manager = new TTManager()){
			TTDocument document = manager.createDocument();
			TTEntity namespaceEntity = manager.createNamespaceEntity(Namespace.SMARTLIFE, "Smartlife health graph", "Smartlife library of value sets, queries and profiles");
			namespaceEntity.addObject(iri(IM.IS_CONTAINED_IN),iri(IM.CORE_SCHEMES));
			document.addEntity(namespaceEntity);
			createFolders(document);
			try {
				EQDImporter eqdImporter = new EQDImporter();
				eqdImporter.loadAndConvert(config,manager,queries[0],Namespace.SMARTLIFE,dataMapFile[0],
					"criteriaMaps.properties",mainFolder,setFolder);
			}
			catch (Exception ex) {
				throw new ImportException(ex.getMessage(), ex);
			}
			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
				filer.fileDocument(document, Graph.IM);

			}
			catch (Exception ex) {
				throw new ImportException(ex.getMessage(), ex);
			}
		}
	}


	private void createFolders(TTDocument document) {
		TTEntity folder = new TTEntity()
			.setIri(Namespace.SMARTLIFE + "Q_SmartLifeQueries")
			.setName("SmartLife queries")
			.addType(iri(IM.FOLDER))
			.set(iri(IM.IS_CONTAINED_IN), iri(Namespace.IM + "Q_Queries"));
		folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.QUERY));
		document.addEntity(folder);
		mainFolder= folder.getIri();
		folder = new TTEntity()
			.setIri(Namespace.SMARTLIFE + "CSET_SmartLifeConceptSets")
			.setName("Smart Life Health value set library")
			.addType(iri(IM.FOLDER))
			.set(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(Namespace.IM + "QueryConceptSets"));
		folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.CONCEPT_SET));
		document.addEntity(folder);
		setFolder= folder.getIri();

	}
	private void checkAndUnzip(String file) throws IOException {
		LOG.info("Checking for library zip items");
		Path zipFile= Path.of(file);
		ImportUtils.unzipArchive(zipFile.toString(), zipFile.getParent().toString());

	}



	@Override
	public void validateFiles(String inFolder) throws TTFilerException {
		ImportUtils.validateFiles(inFolder, queries);
	}


	@Override
	public void close() throws Exception {

	}
}
