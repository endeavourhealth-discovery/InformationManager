package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.transforms.EqdToIMQ;
import org.endeavourhealth.library.vocabulary.GRAPH;
import org.endeavourhealth.library.vocabulary.NAMESPACE;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.library.model.tripletree.*;
import org.endeavourhealth.library.transforms.TTManager;
import org.endeavourhealth.library.vocabulary.IM;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;

import java.util.*;

import static org.endeavourhealth.library.model.tripletree.TTIriRef.iri;

public class QOFQueryImport implements TTImport {
	private static final String[] queries = {".*\\\\QOF"};
	private static final String[] dataMapFile = {".*\\\\EQD\\\\EqdDataMap.properties"};
	private static final String[] autoNamedSets = {".*\\\\EQD\\\\AutoNamedSets.txt"};
	private static final String[] autoNamedClauses = {".*\\\\EQD\\\\AutoNamedClauses.txt"};
	private static final String[] uuidLabels = {".*\\\\EQD\\\\uuidLabels.properties"};
	private static final String[] qofRefSets = {".*\\\\QOF\\\\Static_Expanded_cluster_lists_Ruleset-level_adhoc_.*\\.zip"};
	private String mainFolder;
	private String setFolder;


	@Override
	public void importData(TTImportConfig config) throws ImportException {

		try (QOFRefSetImport setImporter= new QOFRefSetImport()) {
			setImporter.importData(config);
		} catch (Exception e) {
			throw new ImportException(e.getMessage(),e);
		}


		try (TTManager manager = new TTManager()){
			manager.createDocument();
			List<String> defaultTypes= List.of(IM.CONCEPT_SET.toString(),IM.QUERY.toString());
			manager.getDocument().addEntity(manager.createNamespaceEntity(NAMESPACE.QOF,"QOF Framework", "QOF  library of value sets, queries and profiles"));
			createFolders(manager.getDocument());
			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(GRAPH.IM)) {
				filer.fileDocument(manager.getDocument());
			}
			catch (Exception ex) {
				throw new ImportException(ex.getMessage(), ex);
			}
			manager.createDocument();
			try {
				EQDImporter eqdImporter = new EQDImporter(true);
				eqdImporter.loadAndConvert(config,manager,queries[0], NAMESPACE.QOF,
					dataMapFile[0],uuidLabels[0],mainFolder,setFolder,autoNamedSets[0],autoNamedClauses[0]);
			}
			catch (Exception ex) {
				throw new ImportException(ex.getMessage(), ex);
			}

			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(GRAPH.IM)) {
				try {
					filer.fileDocument(manager.getDocument());
				} catch (Exception e) {
					throw new ImportException(e.getMessage(),e);
				}
			}
		}
	}


	private void createFolders(TTDocument document) {
		TTEntity folder = new TTEntity()
			.setIri(NAMESPACE.QOF + "Q_QOFQueries")
			.setName("QOF  queries")
			.addType(iri(IM.FOLDER))
			.set(iri(IM.IS_CONTAINED_IN), iri(NAMESPACE.IM + "Q_Queries"));
		folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.QUERY));
		document.addEntity(folder);
		mainFolder= folder.getIri();
		folder = new TTEntity()
			.setIri(NAMESPACE.QOF + "CSET_QOFConceptSets")
			.setName("QOF Health value set library")
			.addType(iri(IM.FOLDER))
			.set(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(NAMESPACE.IM + "QueryConceptSets"));
		folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.CONCEPT_SET));
		document.addEntity(folder);
		setFolder= folder.getIri();

	}



	@Override
	public void validateFiles(String inFolder) throws TTFilerException {
		ImportUtils.validateFiles(inFolder, queries,qofRefSets,autoNamedSets);
	}


	@Override
	public void close() throws Exception {
	}
}

