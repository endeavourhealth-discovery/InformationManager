package org.endeavourhealth.informationmanager.transforms.sources;

import jakarta.xml.bind.JAXBContext;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.model.customexceptions.EQDException;
import org.endeavourhealth.imapi.model.imq.QueryException;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.transforms.EqdToIMQ;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.transforms.eqd.EnquiryDocument;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class EQDImporter {
	private static final Logger LOG = LoggerFactory.getLogger(CEGImporter.class);
	private TTDocument document;


	public void importEqds(String graph,Path directory, Properties dataMap, TTEntity mainFolder, TTEntity setFolder) throws Exception, EQDException, IOException {
		for (File fileEntry : Objects.requireNonNull(directory.toFile().listFiles())) {
			if (!fileEntry.isDirectory()) {
				String ext = FilenameUtils.getExtension(fileEntry.getName());
				if (ext.equalsIgnoreCase("xml")) {
					LOG.info("...{}", fileEntry.getName());
					try (TTManager manager = new TTManager()) {
						document = manager.createDocument(graph);
						JAXBContext context = JAXBContext.newInstance(EnquiryDocument.class);
						EnquiryDocument eqd = (EnquiryDocument) context.createUnmarshaller()
							.unmarshal(fileEntry);
						EqdToIMQ converter = new EqdToIMQ();
						converter.convertEQD(document, eqd, dataMap);
						for (TTEntity entity : document.getEntities()) {
							if (entity.isType(iri(IM.FOLDER))) {
								entity.addObject(iri(IM.IS_CONTAINED_IN), iri(mainFolder.getIri()).setName(mainFolder.getName()));
							}
							if (entity.isType(iri(IM.CONCEPT_SET))) {
								entity.addObject(iri(IM.IS_CONTAINED_IN), iri(setFolder.getIri()).setName(setFolder.getName()));
							}
						}

						try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
							filer.fileDocument(document);
						}
					}
				}
			}
		}
	}
}
