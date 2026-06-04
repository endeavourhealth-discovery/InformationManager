package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.utility.ThreadContext;
import org.endeavourhealth.interfacemanager.model.GRAPH;
import org.endeavourhealth.interfacemanager.model.NAMESPACE;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.interfacemanager.model.IM;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class SmartLifeIndicatorImporter implements TTImport {
	private static final String[] queries = {".*\\\\Smartlife"};
	private static final String[] libraries = {".*\\\\Smartlife\\\\Library\\\\Library.zip"};
	private static final String[] dataMapFile = {".*\\\\EQD\\\\EqdDataMap.properties"};
	private static final String[] uuidLabels = {".*\\\\EQD\\\\uuidLabels.properties"};
	private static final String[] autoNamedSets = {".*\\\\EQD\\\\AutoNamedSets.txt"};
	private static final String[] autoNamedClauses = {".*\\\\EQD\\\\AutoNamedClauses.txt"};
	private static final String[] indicators = {
		".*\\\\Smartlife\\\\Indicator-query.txt"
	};
	private final GRAPH fileGraph = GRAPH.IM;
	private String mainFolder;
	private String setFolder;
	private static final Logger LOG = LoggerFactory.getLogger(SnomedImporter.class);

	@Override
	public void importData(TTImportConfig config) throws ImportException {
		ThreadContext.setUserGraphs(List.of(GRAPH.IM, GRAPH.IM.SMARTLIFE));
			try {
				new IndicatorImporter().generate(config.getFolder()+"\\Smartlife\\Indicator-query.txt",
					"http://smartlifehealth.info/smh#SmartLifeIndicators", NAMESPACE.SMARTLIFE);
			} catch (Exception e) {
				throw new ImportException("Unable to generate indicators",e);
			}
	}




	private void createFolders(TTDocument document) {
		TTEntity folder = new TTEntity()
			.setIri(NAMESPACE.SMARTLIFE + "Q_SmartLifeQueries")
			.setName("SmartLife queries")
			.addType(new TTIriRef(IM.FOLDER))
			.setScheme(new TTIriRef(NAMESPACE.SMARTLIFE))
			.set(new TTIriRef(IM.IS_CONTAINED_IN), new TTIriRef(NAMESPACE.IM + "Q_Queries"));
		folder.addObject(new TTIriRef(IM.CONTENT_TYPE), new TTIriRef(IM.QUERY));
		document.addEntity(folder);
		mainFolder= folder.getIri();
		folder = new TTEntity()
			.setIri(NAMESPACE.SMARTLIFE + "CSET_SmartLifeConceptSets")
			.setName("Smart Life Health value set library")
			.addType(new TTIriRef(IM.FOLDER))
			.setScheme(new TTIriRef(NAMESPACE.SMARTLIFE))
			.set(new TTIriRef(IM.IS_CONTAINED_IN), new TTIriRef(NAMESPACE.IM + "QueryConceptSets"));
		folder.addObject(new TTIriRef(IM.CONTENT_TYPE), new TTIriRef(IM.CONCEPT_SET));
		document.addEntity(folder);
		setFolder= folder.getIri();
		folder = new TTEntity()
			.setIri(NAMESPACE.SMARTLIFE + "SmartLifeIndicators")
			.setName("Smart Life indicators")
			.addType(new TTIriRef(IM.FOLDER))
			.setScheme(new TTIriRef(NAMESPACE.SMARTLIFE))
			.set(new TTIriRef(IM.IS_CONTAINED_IN), new TTIriRef(NAMESPACE.IM + "Indicators"))
			.addObject(new TTIriRef(IM.CONTENT_TYPE), new TTIriRef(IM.INDICATOR));
		document.addEntity(folder);
	}

	@Override
	public void validateFiles(String inFolder) throws TTFilerException {
		ImportUtils.validateFiles(inFolder, queries,indicators,uuidLabels);
	}


	@Override
	public void close() throws Exception {

	}
}
