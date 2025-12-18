package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.logic.reasoner.LogicOptimizer;
import org.endeavourhealth.imapi.model.imq.Node;
import org.endeavourhealth.imapi.model.imq.Query;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.EqdToIMQ;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.transforms.eqd.*;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class EQDImporter {
	private static final Logger LOG = LoggerFactory.getLogger(EQDImporter.class);
	private TTManager manager;
	private Properties dataMap;
	private String mainFolder;
	private String setFolder;
	private final EqdToIMQ converter;
	private Namespace namespace;
	private final Map<String,TTEntity> folderToEntity= new HashMap<>();
	private final Set<TTEntity> newFolders= new HashSet<>();
	private Properties uuidLabels;
	private final Map<String,EQDOCCriterion> libraryItems= new HashMap<>();

	public EQDImporter(boolean versionIndependent) {
		converter= new EqdToIMQ(versionIndependent);
	}

	public void loadLibraryItems(Path directory) throws JAXBException {
		File libraryDocument= new File(directory.toAbsolutePath() + "/library/Library.xml");
		if (!libraryDocument.exists()) return;
		JAXBContext context = JAXBContext.newInstance(EnquiryDocument.class);
		EnquiryDocument eqd = (EnquiryDocument) context.createUnmarshaller()
			.unmarshal(libraryDocument);
		for (EQDOCReport report:eqd.getReport()){
			if (report.getPopulation()!=null){
				EQDOCPopulation pop= report.getPopulation();
				for (EQDOCCriteriaGroup group:pop.getCriteriaGroup()){
					if (group.getDefinition()!=null){
						if (group.getDefinition().getCriteria()!=null){
							for (EQDOCCriteria criteria:group.getDefinition().getCriteria()){
								if (criteria.getCriterion()!=null){
									libraryItems.put(criteria.getCriterion().getId(),criteria.getCriterion());
								}
							}
						}
					}
				}
			}
		}

	}


	public void loadAndConvert(TTImportConfig config, TTManager manager, String queries, Namespace namespace,
													String dataMapFile, String uuidLabelsFile,String mainFolder, String setFolder,String autoNamedSets) throws Exception {
		String folder=config.getFolder();
		String singleEntity=config.getSingleEntity();
		this.manager= manager;
		this.mainFolder= mainFolder;
		this.setFolder= setFolder;


		converter.setSingleEntity(singleEntity);

		dataMap= new Properties();
			try (FileReader reader = new FileReader((ImportUtils.findFileForId(folder, dataMapFile).toFile()))) {
				dataMap.load(reader);
			}

		uuidLabels= new Properties();
		try (FileReader reader = new FileReader((ImportUtils.findFileForId(folder, uuidLabelsFile).toFile()))) {
			uuidLabels.load(reader);
		}
		catch(Exception ignored){
		}


		Path directory = ImportUtils.findFileForId(folder, queries);
		loadLibraryItems(directory);
		EqdToIMQ.setLibraryItems(libraryItems);
		laodAutoNamedSets(folder,autoNamedSets);
		importEqds(namespace, directory);


	}

	private void laodAutoNamedSets(String folder, String autoNamedSets) throws IOException {
		try (BufferedReader reader = new BufferedReader( new FileReader((ImportUtils.findFileForId(folder, autoNamedSets).toFile())))) {
			String line= reader.readLine();
			 while (line != null && !line.isEmpty()) {
				 String[] fields=line.split("\t");
				 EqdToIMQ.getAutoNamedSets().put(fields[0],fields[1]);
				 line= reader.readLine();
			}
		}
	}

	public void importEqds(Namespace namespace, Path directory) throws Exception  {
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
					converter.convertEQD(document, eqd, dataMap, namespace);
				}
			}
		}
		LOG.info("reports converted. Adding missing folders and creating index");
		manager.createIndex();
		if (converter.getSingleEntity()==null) {
			setRegisteredPatientParent(document);
			cleanFolders(document);
			manager.createIndex();
			addMissingFolders(document);
			removeRedundantFolders(manager);
			setAlternativeCodes(manager.getDocument());
			//createReportFolders(document);
			//createSubPopulations(document);
			//manager.createIndex();
			//if (!newFolders.isEmpty()) {
				//document.getEntities().addAll(newFolders);
			//}
		}


	}

	private void setAlternativeCodes(TTDocument document) throws JsonProcessingException {
		for (TTEntity entity:document.getEntities()) {
			String uuid= entity.getIri().substring(entity.getIri().lastIndexOf("#")+1);
			if (uuidLabels.get(uuid)!=null){
				entity.set(IM.ALTERNATIVE_CODE,TTLiteral.literal((String) uuidLabels.get(uuid)));
			}
		}
	}

	private void addMissingFolders(TTDocument document) {
		for (TTEntity report : document.getEntities()) {
		if (report.isType(iri(IM.QUERY))&&report.get(iri(IM.IS_CONTAINED_IN))!=null){
				for (TTValue folder: report.get(iri(IM.IS_CONTAINED_IN)).getElements()){
					TTEntity folderEntity= manager.getEntity(folder.asIriRef().getIri());
					if (folderEntity==null){
						report.set(iri(IM.IS_CONTAINED_IN), new TTArray().add(iri(mainFolder)));
					}
				}
			}
		}
	}







	private void createReportFolders(TTDocument document) {
		for (TTEntity entity: document.getEntities()){
			if (entity.isType(iri(IM.QUERY))||entity.isType(iri(IM.QUERY))){
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


	private void removeRedundantFolders(TTManager manager) throws TTFilerException {
		manager.createIndex();
		Map<String,TTEntity> usedFolders= new HashMap<>();
		for (TTEntity entity : manager.getDocument().getEntities()) {
			if (entity.getIri().equals(mainFolder)||entity.getIri().equals(setFolder)){
				usedFolders.put(entity.getIri(),entity);
			} else {
				if (entity.get(iri(IM.IS_CONTAINED_IN)) != null) {
					for (TTValue used : entity.get(iri(IM.IS_CONTAINED_IN)).getElements()) {
						try {
							TTEntity aFolder = manager.getEntity(used.asIriRef().getIri());
							if (aFolder != null) usedFolders.put(used.asIriRef().getIri(), aFolder);
						} catch (Exception e){
							throw new TTFilerException("Could not find folder " + used.asIriRef().getIri());
						}
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
			if (entity.isType(iri(IM.QUERY))) {
				if (entity.get(iri(IM.DEFINITION)) != null) {
					Query qry = entity.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class);
					if (qry.getIs()!=null) {
						String parent = qry.getIs().get(0).getIri();
						if (parent.equals(Namespace.SMARTLIFE + "71154095-0C58-4193-B58F-21F05EA0BE2F")) {
							qry.setIs(new ArrayList<>());
							qry.addIs(new Node().setIri(Namespace.IM + "Q_RegisteredPatient"));
							entity.set(iri(IM.DEFINITION), TTLiteral.literal(qry));
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
			.setIri(namespace.toString() + UUID.randomUUID())
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
						.setIri(namespace.toString() + UUID.randomUUID())
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
						.setIri(namespace.toString() + UUID.randomUUID())
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
