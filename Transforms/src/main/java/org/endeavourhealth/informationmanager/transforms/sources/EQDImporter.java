package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.xml.bind.JAXBContext;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.imapi.model.imq.Match;
import org.endeavourhealth.imapi.model.imq.Node;
import org.endeavourhealth.imapi.model.imq.Query;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.EqdToIMQ;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.transforms.eqd.EnquiryDocument;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class EQDImporter {
	private static final Logger LOG = LoggerFactory.getLogger(EQDImporter.class);
	private TTManager manager;
	private Properties dataMap;
	private Properties criteriaMaps;
	private String mainFolder;
	private String setFolder;
	private final EqdToIMQ converter = new EqdToIMQ();
	private String namespace;
	private final Map<String,TTEntity> folderToEntity= new HashMap<>();
	private final Set<TTEntity> newFolders= new HashSet<>();



	public void loadAndConvert(TTImportConfig config, TTManager manager, String queries, String graph,
													String dataMapFile, String criteriaMapFile,String mainFolder, String setFolder) throws Exception {
		String folder=config.getFolder();
		String singleEntity=config.getSingleEntity();
		this.manager= manager;
		this.mainFolder= mainFolder;
		this.setFolder= setFolder;

		converter.setSingleEntity(singleEntity);
		dataMap= new Properties();
		if (ImportApp.resourceFolder!=null) {
			try (FileReader reader = new FileReader((ImportApp.resourceFolder))) {
				dataMap.load(reader);
			}
		}

		else{
			try (FileReader reader = new FileReader((ImportUtils.findFileForId(folder, dataMapFile).toFile()))) {
				dataMap.load(reader);
			}
		}

		criteriaMaps= new Properties();
		try (FileReader reader = new FileReader(folder+"/"+criteriaMapFile)) {
			criteriaMaps.load(reader);
		}
		catch(Exception ignored){
		}


		Path directory = ImportUtils.findFileForId(folder, queries);
		importEqds(graph, directory);

	}



	public void importEqds(String namespace, Path directory) throws Exception  {
		this.namespace=namespace;
		TTDocument document = manager.getDocument();
		for (File fileEntry : Objects.requireNonNull(directory.toFile().listFiles())) {
			if (!fileEntry.isDirectory()) {
				String ext = FilenameUtils.getExtension(fileEntry.getName());
				if (ext.equalsIgnoreCase("xml")) {
					LOG.info("...{}", fileEntry.getName());
					JAXBContext context = JAXBContext.newInstance(EnquiryDocument.class);
					EnquiryDocument eqd = (EnquiryDocument) context.createUnmarshaller()
						.unmarshal(fileEntry);
					converter.convertEQD(document, eqd, dataMap,criteriaMaps);
				}
			}
		}
		manager.createIndex();
		if (converter.getSingleEntity()==null) {
			setRegisteredPatientParent(document);
			cleanFolders(document);
			manager.createIndex();
			addSetsToFolders(document);
			removeRedundantFolders(manager);
			//createReportFolders(document);
			//createSubPopulations(document);
			//manager.createIndex();
			//if (!newFolders.isEmpty()) {
				//document.getEntities().addAll(newFolders);
			//}
		}


	}
	private TTEntity createFieldGroupFolder(TTEntity fieldGroup, Map<String, TTEntity> folderMap, List<TTEntity> toAdd) {
		TTEntity folder = new TTEntity()
			.setIri(namespace + UUID.randomUUID())
			.setName("Sets used for " + fieldGroup.getName())
			.addType(iri(IM.FOLDER));
		toAdd.add(folder);
		folderMap.put(fieldGroup.getIri(), folder);
		for (TTValue r : fieldGroup.get(iri(IM.USED_IN)).getElements()) {
			TTEntity report = manager.getEntity(r.asIriRef().getIri());
			TTEntity reportFolder = folderMap.get(report.getIri());
			if (reportFolder == null) {
				reportFolder = createReportSetFolder(report,folderMap,toAdd);
			}
			folder.addObject(iri(IM.IS_CONTAINED_IN),iri(reportFolder.getIri()));
		}
		return folder;
	}





	private void createReportFolders(TTDocument document) {
		for (TTEntity entity: document.getEntities()){
			if (entity.isType(iri(IM.COHORT_QUERY))||entity.isType(iri(IM.DATASET_QUERY))){
				String reportFolderIri= namespace+ "Folder_"+entity.getIri().split("#")[1];
				TTEntity reportFolder= folderToEntity.get(reportFolderIri);
				if (reportFolder==null){
					 reportFolder = new TTEntity()
						.setIri(reportFolderIri)
						.setName(entity.getName() + " (folder)")
						.addType(iri(IM.FOLDER));
					newFolders.add(reportFolder);
					folderToEntity.put(reportFolderIri,reportFolder);
				}
				if (entity.get(iri(IM.IS_CONTAINED_IN))!=null) {
					for (TTValue currentFolder : entity.get(iri(IM.IS_CONTAINED_IN)).getElements()) {
						reportFolder.addObject(iri(IM.IS_CONTAINED_IN), currentFolder.asIriRef().getIri());
					}
				}
				entity.set(iri(IM.IS_CONTAINED_IN),new TTArray().add(iri(reportFolder.getIri())));
				}
			}

	}

	private void createSubPopulations(TTDocument document) throws JsonProcessingException {
		for (TTEntity entity : document.getEntities()) {
			if (entity.get(iri(IM.DEFINITION))!=null){
				Query query= entity.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class);
				if (query.getMatch()!=null){
					for (Match match:query.getMatch()){
						if (match.getInstanceOf()!=null){
							for (Node node:match.getInstanceOf()){
								if (node.isMemberOf()){
									if (!node.getIri().equals(IM.NAMESPACE+"Q_RegisteredGMS")&&!EqdToIMQ.gmsPatients.contains(node.getIri())) {
										TTEntity parentReport= manager.getEntity(node.getIri());
										TTEntity parentReportFolder= folderToEntity.get(namespace+"Folder_"+ node.getIri().split("#")[1]);
										String subFolderIri=namespace+"SubFolder_"+node.getIri().split("#")[1];
										TTEntity subFolder= folderToEntity.get(subFolderIri);
										if (subFolder==null) {
											subFolder = new TTEntity()
												.setIri(subFolderIri)
												.setName("Child queries of " + parentReport.getName() + " (folder)")
												.addType(iri(IM.FOLDER));
											subFolder.addObject(iri(IM.IS_CONTAINED_IN), iri(parentReportFolder.getIri()));
											folderToEntity.put(subFolderIri, subFolder);
											newFolders.add(subFolder);
										}
										TTEntity reportFolder= folderToEntity.get(namespace+"Folder_"+ entity.getIri().split("#")[1]);
										reportFolder.addObject(iri(IM.IS_CONTAINED_IN),iri(subFolderIri));
										}
									}
								}
							}
						}
					}
				}
			}
	}

	private void cleanFolders(TTDocument document){
		for (TTEntity entity : document.getEntities()) {
			if (!entity.getIri().equals(mainFolder) && !entity.getIri().equals(setFolder)) {
				if (entity.isType(iri(IM.FOLDER)) && entity.get(iri(IM.IS_CONTAINED_IN)) == null) {
					entity.addObject(iri(IM.IS_CONTAINED_IN), iri(mainFolder));
				}
				else if (entity.get(iri(IM.IS_CONTAINED_IN)) != null) {
					boolean hasParentFolder = true;
					TTArray fixedFolders = new TTArray();
					for (TTValue folder : entity.get(IM.IS_CONTAINED_IN).getElements()) {
						if (manager.getEntity(folder.asIriRef().getIri()) == null) {
							String fixedFolder = fixFolder(folder.asIriRef().getIri());
							if (manager.getEntity(fixedFolder) != null) {
								fixedFolders.add(iri(fixedFolder));
							}
							else
								hasParentFolder = false;
						}
					}
					if (!fixedFolders.isEmpty()) {
						entity.set(iri(IM.IS_CONTAINED_IN), fixedFolders);
					}
					else if (!hasParentFolder && entity.isType(iri(IM.FOLDER))) {
						entity.set(iri(IM.IS_CONTAINED_IN), new TTArray().add(iri(mainFolder)));
					}
				}
			}
		}
	}

	private void addSetsToFolders(TTDocument document) {
		Map<String, TTEntity> folderMap= new HashMap<>();
		List<TTEntity> newSetFolders= new ArrayList<>();
		for (TTEntity set : document.getEntities()) {
			if (set.get(iri(IM.USED_IN))!=null) {
				for (TTValue used : set.get(iri(IM.USED_IN)).getElements()) {
					String namespace = used.asIriRef().getIri().substring(0, used.asIriRef().getIri().lastIndexOf("#") + 1);
					TTEntity report = manager.getEntity(used.asIriRef().getIri());
					TTEntity reportFolder=null;
					String reportFolderIri= null;
					if (report.get(iri(IM.IS_CONTAINED_IN)) == null){
						if (report.isType(iri(IM.FIELD_GROUP))){
							reportFolder= createFieldGroupFolder(report,folderMap,newSetFolders);
							reportFolderIri= reportFolder.getIri();
						}
					}
					else {
						reportFolderIri = report.get(iri(IM.IS_CONTAINED_IN)).getElements().get(0).asIriRef().getIri();
						reportFolder = manager.getEntity(reportFolderIri);
						if (reportFolder == null) {
							reportFolder = createReportSetFolder(report,folderMap,newSetFolders);
							reportFolderIri= reportFolder.getIri();
						}
					}
					String setFolderIri = namespace + "SetFolder_" + reportFolderIri.substring(reportFolderIri.lastIndexOf("#") + 1);
					TTEntity setFolder = folderToEntity.get(setFolderIri);
					if (setFolder == null) {
							setFolder = new TTEntity()
								.setIri(setFolderIri)
								.setName("Sets used in " + reportFolder.getName())
								.addType(iri(IM.FOLDER));
							newSetFolders.add(setFolder);
							folderToEntity.put(setFolderIri, setFolder);
							setFolder.addObject(iri(IM.IS_CONTAINED_IN), iri(reportFolderIri));
							;
						}
						set.addObject(iri(IM.IS_CONTAINED_IN), iri(setFolderIri));
						;
					}
			}
		}
		if (!newSetFolders.isEmpty()) {
			document.getEntities().addAll(newSetFolders);
		}
	}


	private void removeRedundantFolders(TTManager manager) {
		manager.createIndex();
		Map<String,TTEntity> usedFolders= new HashMap<>();
		for (TTEntity entity : manager.getDocument().getEntities()) {
			if (entity.getIri().equals(mainFolder)||entity.getIri().equals(setFolder)){
				usedFolders.put(entity.getIri(),entity);
			} else {
				if (entity.get(iri(IM.IS_CONTAINED_IN)) != null) {
					for (TTValue used : entity.get(iri(IM.IS_CONTAINED_IN)).getElements()) {
						usedFolders.put(used.asIriRef().getIri(), manager.getEntity(used.asIriRef().getIri()));
					}
				}
			}
		}
		Set<TTEntity> toRemove= new HashSet<>();
		for (TTEntity entity : manager.getDocument().getEntities()) {
			if (entity.isType(iri(IM.FOLDER))) {
				if (!usedFolders.containsKey(entity.getIri())) {
					toRemove.add(entity);
				}
			}
		}
		for (TTEntity entity:toRemove) {
			manager.getDocument().getEntities().remove(entity);
		}
	}


	private void setRegisteredPatientParent(TTDocument document) throws JsonProcessingException {
		for (TTEntity entity : document.getEntities()) {
			if (entity.get(iri(IM.DEFINITION)) != null) {
				Query qry = entity.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class);
				if (qry.getMatch() != null && qry.getMatch().get(0).getInstanceOf() != null) {
					for (Node parent : qry.getMatch().get(0).getInstanceOf()) {
						if (parent.getIri().equals(GRAPH.SMARTLIFE + "71154095-0C58-4193-B58F-21F05EA0BE2F")) {
							List<Node> parentList = new ArrayList<>();
							parentList.add(new Node().setIri(IM.NAMESPACE + "Q_RegisteredGMS").setMemberOf(true));
							qry.getMatch().get(0).setInstanceOf(parentList);
						}
					}
				}
			}
		}
	}



	private String fixFolder(String folder){
	  return folder.toLowerCase();
}

	private TTEntity createReportSetFolder(TTEntity report, Map<String, TTEntity> folderMap,List<TTEntity> toAdd) {
		TTEntity folder = new TTEntity()
			.setIri(namespace + UUID.randomUUID())
			.setName("Sets used in " + report.getName())
			.addType(iri(IM.FOLDER));
		toAdd.add(folder);
		folderMap.put(report.getIri(), folder);
		if (report.get(iri(IM.IS_CONTAINED_IN))!=null) {
			for (TTValue rf : report.get(iri(IM.IS_CONTAINED_IN)).getElements()) {
				TTIriRef reportFolder = rf.asIriRef();
				TTEntity groupFolder = folderMap.get(reportFolder.getIri());
				if (groupFolder == null) {
					groupFolder = new TTEntity()
						.setIri(namespace + UUID.randomUUID())
						.addType(iri(IM.FOLDER))
						.setName("Sets for " + reportFolder.getName());
					groupFolder.addObject(iri(IM.IS_CONTAINED_IN), iri(setFolder));
					folderMap.put(reportFolder.getIri(), groupFolder);
					toAdd.add(groupFolder);
				}
				folder.addObject(iri(IM.IS_CONTAINED_IN), iri(groupFolder.getIri()));
			}
		}
		else if (report.get(iri(RDFS.SUBCLASS_OF))!=null) {
			for (TTValue rf : report.get(iri(RDFS.SUBCLASS_OF)).getElements()) {
				TTIriRef reportFolder = rf.asIriRef();
				TTEntity groupFolder = folderMap.get(reportFolder.getIri());
				if (groupFolder == null) {
					groupFolder = new TTEntity()
						.setIri(namespace + UUID.randomUUID())
						.addType(iri(IM.FOLDER))
						.setName("Sets for " + reportFolder.getName());
					groupFolder.addObject(iri(IM.IS_CONTAINED_IN), iri(setFolder));
					folderMap.put(reportFolder.getIri(), groupFolder);
					toAdd.add(groupFolder);
				}
				folder.addObject(iri(IM.IS_CONTAINED_IN), iri(groupFolder.getIri()));
			}
		}
		return folder;
	}




}
