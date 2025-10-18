package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.utility.ThreadContext;
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

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class SmartLifeImporter implements TTImport {
	private static final String[] queries = {".*\\\\Smartlife"};
	private static final String[] libraries = {".*\\\\Smartlife\\\\Library\\\\Library.zip"};
	private static final String[] dataMapFile = {".*\\\\EQD\\\\EqdDataMap.properties"};
	private static final String[] uuidLabels = {".*\\\\EQD\\\\uuidLabels.properties"};
	private static final String[] indicators = {
		".*\\\\Smartlife\\\\Indicator-query.txt"
	};
	private final Graph fileGraph = Graph.IM;
	private String mainFolder;
	private String setFolder;
	private static final Logger LOG = LoggerFactory.getLogger(SnomedImporter.class);

	@Override
	public void importData(TTImportConfig config) throws ImportException {
    ThreadContext.setUserGraphs(List.of(Graph.IM, Graph.SMARTLIFE));


		try {
			Path zip = ImportUtils.findFileForId(config.getFolder(), libraries[0]);
			ZipUtils.unzipFile(zip.getFileName().toString(), zip.getParent().toString());
		} catch (IOException e) {
			throw new ImportException("Unable to unzip smartlife library",e);
		}

		try (TTManager manager = new TTManager()){
			TTDocument document = manager.createDocument();
			createFolders(document);
			TTEntity namespaceEntity = manager.createNamespaceEntity(Namespace.SMARTLIFE, "Smartlife health graph", "Smartlife library of value sets, queries and profiles",true,false);
			namespaceEntity.addObject(iri(IM.IS_CONTAINED_IN),iri(IM.CORE_SCHEMES));
			document.addEntity(namespaceEntity);

			try {
				EQDImporter eqdImporter = new EQDImporter(false);
				eqdImporter.loadAndConvert(config,manager,queries[0],Namespace.SMARTLIFE,dataMapFile[0],
					uuidLabels[0],mainFolder,setFolder);
			}
			catch (Exception ex) {
				throw new ImportException(ex.getMessage(), ex);
			}
			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(fileGraph)) {
				filer.fileDocument(document);
			}
			catch (Exception ex) {
				throw new ImportException(ex.getMessage(), ex);
			}

		}
		try {
			new IndicatorImporter().generate(config.getFolder()+"\\Smartlife",
				"http://smartlifehealth.info/smh#SmartLifeIndicators",
				"http://endhealth.info/im#CarePathways", Namespace.SMARTLIFE);
		} catch (Exception e) {
			throw new ImportException("Unable to generate indicators",e);
		}

	}



	private void createFolders(TTDocument document) {
		TTEntity folder = new TTEntity()
			.setIri(Namespace.SMARTLIFE + "Q_SmartLifeQueries")
			.setName("SmartLife queries")
			.addType(iri(IM.FOLDER))
			.setScheme(iri(Namespace.SMARTLIFE))
			.set(iri(IM.IS_CONTAINED_IN), iri(Namespace.IM + "Q_Queries"));
		folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.QUERY));
		document.addEntity(folder);
		mainFolder= folder.getIri();
		folder = new TTEntity()
			.setIri(Namespace.SMARTLIFE + "CSET_SmartLifeConceptSets")
			.setName("Smart Life Health value set library")
			.addType(iri(IM.FOLDER))
			.setScheme(iri(Namespace.SMARTLIFE))
			.set(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(Namespace.IM + "QueryConceptSets"));
		folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.CONCEPT_SET));
		document.addEntity(folder);
		folder = new TTEntity()
			.setIri(Namespace.SMARTLIFE + "SmartLifeIndicators")
			.setName("Smart Life indicators")
			.addType(iri(IM.FOLDER))
			.setScheme(iri(Namespace.SMARTLIFE))
			.set(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(Namespace.IM + "Indicators"))
				.addObject(iri(IM.CONTENT_TYPE), iri(IM.INDICATOR));
		document.addEntity(folder);
		setFolder= folder.getIri();

	}

	@Override
	public void validateFiles(String inFolder) throws TTFilerException {
		ImportUtils.validateFiles(inFolder, queries,indicators,uuidLabels);
	}


	@Override
	public void close() throws Exception {

	}
}
