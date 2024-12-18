package org.endeavourhealth.informationmanager.transforms.sources;

import jakarta.xml.bind.JAXBContext;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.EqdToIMQ;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.transforms.eqd.EnquiryDocument;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class EQDImporter {
	private static final Logger LOG = LoggerFactory.getLogger(CEGImporter.class);
	private TTManager manager;
	private final Properties dataMap;
	private final String mainFolder;
	private final String setFolder;
	private Map<String,String> badToGoodFolder= new HashMap<>();

	public EQDImporter(TTManager manager, Properties dataMap, String mainFolder, String setFolder) {
		this.manager= manager;
		this.dataMap= dataMap;
		this.mainFolder= mainFolder;
		this.setFolder= setFolder;
	}

	public void importEqds(String graph,Path directory) throws Exception  {
		for (File fileEntry : Objects.requireNonNull(directory.toFile().listFiles())) {
			if (!fileEntry.isDirectory()) {
				String ext = FilenameUtils.getExtension(fileEntry.getName());
				if (ext.equalsIgnoreCase("xml")) {
					LOG.info("...{}", fileEntry.getName());
					TTDocument document = manager.createDocument(graph);
					JAXBContext context = JAXBContext.newInstance(EnquiryDocument.class);
					EnquiryDocument eqd = (EnquiryDocument) context.createUnmarshaller()
						.unmarshal(fileEntry);
					EqdToIMQ converter = new EqdToIMQ();
					converter.convertEQD(document, eqd, dataMap);
					manager.createIndex();
					for (TTEntity entity : document.getEntities()) {
						if (entity.isType(iri(IM.FOLDER))||entity.isType(iri(IM.COHORT_QUERY))||entity.isType(iri(IM.DATASET_QUERY))) {
							if (entity.get(iri(IM.IS_CONTAINED_IN)) == null) {
								entity.addObject(iri(IM.IS_CONTAINED_IN), iri(mainFolder));
							}
							else {
								boolean hasParentFolder = true;
								TTArray fixedFolders = new TTArray();
								for (TTValue folder : entity.get(IM.IS_CONTAINED_IN).getElements()) {
									if (manager.getEntity(folder.asIriRef().getIri()) == null) {
										if (manager.getEntity(folder.asIriRef().getIri().toLowerCase()) != null) {
											fixedFolders.add(iri(folder.asIriRef().getIri().toLowerCase()));
											badToGoodFolder.put(folder.asIriRef().getIri(), folder.asIriRef().getIri().toLowerCase());
										}
										else
											hasParentFolder = false;
									}
								}
								if (!fixedFolders.isEmpty()) {
									entity.set(iri(IM.IS_CONTAINED_IN), fixedFolders);
								}
								else if (!hasParentFolder) {
									entity.set(iri(IM.IS_CONTAINED_IN), new TTArray().add(iri(mainFolder)));
								}
							}
						}
					}
					manager.createIndex();
					addSetsToFolders(manager, document);
					try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
						filer.fileDocument(document);
					}
				}
			}
		}
}



	private void addSetsToFolders(TTManager manager,TTDocument document) {
		Map<String, TTEntity> folderMap = new HashMap<>();
		Set<TTEntity> toAdd = new HashSet<>();
		for (TTEntity set : document.getEntities()) {
			if (set.isType(iri(IM.CONCEPT_SET))) {
				for (TTValue used : set.get(iri(IM.USED_IN)).getElements()) {
					TTEntity usedIn = manager.getEntity(used.asIriRef().getIri());
					TTEntity folder = folderMap.get(usedIn.getIri());
					if (folder == null) {
						if (usedIn.isType(iri(IM.FIELD_GROUP))) {
							folder = createFieldGroupFolder(usedIn, folderMap,toAdd);
						}
						else {
							folder = createReportSetFolder(usedIn, folderMap,toAdd);
						}
					}
					set.addObject(iri(IM.IS_CONTAINED_IN), iri(folder.getIri()));
				}
			}
		}
		if (!toAdd.isEmpty()){
			for (TTEntity folder:toAdd){
				document.addEntity(folder);
			}
		}
	}

	private TTEntity createFieldGroupFolder(TTEntity fieldGroup, Map<String, TTEntity> folderMap, Set<TTEntity> toAdd) {
		TTEntity folder = new TTEntity()
			.setIri("urn:uuid:" + UUID.randomUUID())
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

	private TTEntity createReportSetFolder(TTEntity report, Map<String, TTEntity> folderMap,Set<TTEntity> toAdd) {
		TTEntity folder = new TTEntity()
			.setIri("urn:uuid:" + UUID.randomUUID())
			.setName("Sets used in " + report.getName())
			.addType(iri(IM.FOLDER));
		toAdd.add(folder);
		folderMap.put(report.getIri(), folder);
		for (TTValue rf : report.get(iri(IM.IS_CONTAINED_IN)).getElements()) {
			TTIriRef reportFolder = rf.asIriRef();
			TTEntity groupFolder = folderMap.get(reportFolder.getIri());
			if (groupFolder == null) {
				groupFolder = new TTEntity()
					.setIri("urn:uuid:" + UUID.randomUUID())
					.addType(iri(IM.FOLDER))
					.setName("Sets for " + reportFolder.getName());
				groupFolder.addObject(iri(IM.IS_CONTAINED_IN), iri(setFolder));
				folderMap.put(reportFolder.getIri(), groupFolder);
				toAdd.add(groupFolder);
			}
			folder.addObject(iri(IM.IS_CONTAINED_IN),iri(groupFolder.getIri()));
		}
		return folder;
	}


}
