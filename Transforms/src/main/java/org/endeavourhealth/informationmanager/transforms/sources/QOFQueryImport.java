package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.model.imq.Match;
import org.endeavourhealth.imapi.model.imq.Node;
import org.endeavourhealth.imapi.model.imq.Query;
import org.endeavourhealth.imapi.transforms.EqdToIMQ;
import org.endeavourhealth.imapi.vocabulary.SCHEME;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
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
			manager.getDocument().addEntity(manager.createScheme(SCHEME.QOF,"QOF Framework", "QOF  library of value sets, queries and profiles"));
			createFolders(manager.getDocument());
			try {
				EQDImporter eqdImporter = new EQDImporter();
				EqdToIMQ.gmsPatients.add("71154095-0C58-4193-B58F-21F05EA0BE2F");
				EqdToIMQ.gmsPatients.add("DA05DBF2-72AB-41A3-968F-E4A061F411A4");
				eqdImporter.loadAndConvert(config,manager,queries[0],SCHEME.QOF,
					dataMapFile[0],"criteriaMaps.properties",mainFolder,setFolder);
			}
			catch (Exception ex) {
				throw new ImportException(ex.getMessage(), ex);
			}

		 reformIris(manager.getDocument());
			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
				try {
					filer.fileDocument(manager.getDocument());
				} catch (Exception e) {
					throw new ImportException(e.getMessage(),e);
				}
			}
		} catch (JsonProcessingException e) {
			throw new ImportException(e.getMessage(),e);
		}
	}

	private int getHash(String iri){
		return iri.hashCode();
	}

	private void reformIris(TTDocument document) throws JsonProcessingException {
		Map<String, String> iriMap = new HashMap<>();
		createIriMap(document, iriMap);
		replaceIris(document,iriMap);
	}

	private void replaceIris(TTDocument document, Map<String, String> iriMap) throws JsonProcessingException {
		for (TTEntity entity : document.getEntities()) {
				String oldIri= entity.getIri();
				String newIri= iriMap.get(oldIri);
				if(newIri!=null) {
					entity.setIri(newIri);
				}
				if (entity.get(iri(IM.IS_CONTAINED_IN)) != null){
					for (TTValue parent: entity.get(iri(IM.IS_CONTAINED_IN)).getElements()){
						if (iriMap.get(parent.asIriRef().getIri())!=null) {
							parent.asIriRef().setIri(iriMap.get(parent.asIriRef().getIri()));
						}
					}
				}
				if (entity.isType(iri(IM.QUERY))) {
					if (entity.get(iri(IM.IS_SUBSET_OF)) != null){
						for (TTValue superQuery: entity.get(iri(IM.IS_SUBSET_OF)).getElements()){
							if (iriMap.get(superQuery.asIriRef().getIri())!=null) {
								superQuery.asIriRef().setIri(iriMap.get(superQuery.asIriRef().getIri()));
							}
						}
					}
					Query query= entity.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class);
					replaceQueryIris(query,iriMap);
					entity.set(iri(IM.DEFINITION), TTLiteral.literal(query));
			}
		}
	}

	private void replaceQueryIris(Query query, Map<String, String> iriMap) {
		if (query.getInstanceOf()!=null) {
			for (Node instanceOf : query.getInstanceOf()) {
				if (iriMap.get(instanceOf.getIri()) != null) {
					instanceOf.setIri(iriMap.get(instanceOf.getIri()));
				}
			}
		}
		replaceMatchIris(query,iriMap);
	}


	private void replaceMatchIris(Match query, Map<String, String> iriMap) {
		if (iriMap.get(query.getIri())!=null) {
			query.setIri(iriMap.get(query.getIri()));
		}

		if (query.getRule()!=null){
			for (Match match:query.getRule()){
				if (match.getInstanceOf()!=null){
					for (Node instanceOf: match.getInstanceOf()){
						if (iriMap.get(instanceOf.getIri())!=null) {
							instanceOf.setIri(iriMap.get(instanceOf.getIri()));
						}
					}
				}
				replaceMatchIris(match,iriMap);
			}
		}
	}
	private String getIri(String prefix,String iri,String name){
		return SCHEME.QOF + prefix + name.split(" ")[0].replaceAll("[^a-zA-Z0-9-_~.]", "")+ getHash(iri);
	}

	private void createIriMap(TTDocument document, Map<String, String> iriMap) throws JsonProcessingException {
		for (TTEntity entity : document.getEntities()) {
			if (entity.isType(iri(IM.FOLDER))) {
				String oldIri= entity.getIri();
				if (oldIri.substring(oldIri.lastIndexOf("#")+1).startsWith("Q")) {
					iriMap.put(oldIri, oldIri);
				} else if (oldIri.substring(oldIri.lastIndexOf("#")+1).startsWith("CSET_")) {
					iriMap.put(oldIri,oldIri);
				} else if (oldIri.substring(oldIri.lastIndexOf("#")+1).startsWith("SetFolder_")) {
					iriMap.put(oldIri,getIri("SetFolder_",entity.getIri(),entity.getName()));
				} else if (oldIri.substring(oldIri.lastIndexOf("#")+1).startsWith("Folder_")) {
						iriMap.put(oldIri,getIri("Folder_",entity.getIri(),entity.getName()));;
				} else {
					iriMap.put(oldIri,getIri("Folder_",entity.getIri(),entity.getName()));
				}
			} else if (entity.isType(iri(IM.QUERY))) {
				String oldIri= entity.getIri();
				String newIri= getIri("Q_",entity.getIri(),entity.getName());
				iriMap.put(oldIri,newIri );

				Query query= entity.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class);
				mapQueryIris(newIri+"_Rule_",query,iriMap);
				}
			}
	}

	private void mapQueryIris(String prefix, Match outer, Map<String, String> iriMap) {
		if (outer.getRule()==null) return;
		int clause=0;
		for (Match match:outer.getRule()){
			clause++;
			if (match.getIri()!=null) iriMap.put(match.getIri(),prefix+clause);
			mapQueryIris(prefix+clause+"_",match,iriMap);
		}
	}


	private void createFolders(TTDocument document) {
		TTEntity folder = new TTEntity()
			.setIri(SCHEME.QOF + "Q_QOFQueries")
			.setName("QOF  queries")
			.addType(iri(IM.FOLDER))
			.set(iri(IM.IS_CONTAINED_IN), iri(IM.NAMESPACE + "Q_Queries"));
		folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.QUERY));
		document.addEntity(folder);
		mainFolder= folder.getIri();
		folder = new TTEntity()
			.setIri(SCHEME.QOF+ "CSET_QOFConceptSets")
			.setName("QOF Health value set library")
			.addType(iri(IM.FOLDER))
			.set(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "QueryConceptSets"));
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

