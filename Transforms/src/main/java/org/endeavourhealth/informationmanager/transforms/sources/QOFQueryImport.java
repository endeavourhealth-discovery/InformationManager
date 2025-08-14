package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.model.imq.Match;
import org.endeavourhealth.imapi.model.imq.Node;
import org.endeavourhealth.imapi.model.imq.Query;
import org.endeavourhealth.imapi.transforms.EqdToIMQ;
import org.endeavourhealth.imapi.vocabulary.Graph;
import org.endeavourhealth.imapi.vocabulary.Namespace;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class QOFQueryImport implements TTImport {
	private static final Logger LOG = LoggerFactory.getLogger(CEGImporter.class);

	private static final String[] queries = {".*\\\\QOF"};
	private static final String[] dataMapFile = {".*\\\\EMIS\\\\EqdDataMap.properties"};
	private String mainFolder;
	private String setFolder;


	@Override
	public void importData(TTImportConfig config) throws ImportException {
		try (TTManager manager = new TTManager()){
			manager.createDocument();
			manager.getDocument().addEntity(manager.createNamespaceEntity(Namespace.QOF,"QOF Framework", "QOF  library of value sets, queries and profiles"));
			createFolders(manager.getDocument());
			try {
				EQDImporter eqdImporter = new EQDImporter();
				EqdToIMQ.gmsPatients.add("71154095-0C58-4193-B58F-21F05EA0BE2F");
				EqdToIMQ.gmsPatients.add("DA05DBF2-72AB-41A3-968F-E4A061F411A4");
				EqdToIMQ.gmsPatients.add("591C5738-2F6B-4A6F-A2B3-05FA538A1B3B");
				eqdImporter.loadAndConvert(config,manager,queries[0], Namespace.QOF,
					dataMapFile[0],"criteriaMaps.properties",mainFolder,setFolder);
			}
			catch (Exception ex) {
				throw new ImportException(ex.getMessage(), ex);
			}

			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
				try {
					filer.fileDocument(manager.getDocument(), Graph.IM);
				} catch (Exception e) {
					throw new ImportException(e.getMessage(),e);
				}
			}
		}
	}


	private void createFolders(TTDocument document) {
		TTEntity folder = new TTEntity()
			.setIri(Namespace.QOF + "Q_QOFQueries")
			.setName("QOF  queries")
			.addType(iri(IM.FOLDER))
			.set(iri(IM.IS_CONTAINED_IN), iri(Namespace.IM + "Q_Queries"));
		folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.QUERY));
		document.addEntity(folder);
		mainFolder= folder.getIri();
		folder = new TTEntity()
			.setIri(Namespace.QOF + "CSET_QOFConceptSets")
			.setName("QOF Health value set library")
			.addType(iri(IM.FOLDER))
			.set(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(Namespace.IM + "QueryConceptSets"));
		folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.CONCEPT_SET));
		document.addEntity(folder);
		setFolder= folder.getIri();

	}



	@Override
	public void validateFiles(String inFolder) throws TTFilerException {
		ImportUtils.validateFiles(inFolder, queries);
	}


	@Override
	public void close() throws Exception {

	}
}

