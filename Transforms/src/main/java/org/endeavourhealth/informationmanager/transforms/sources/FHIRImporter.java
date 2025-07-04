package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.imapi.logic.CachedObjectMapper;
import org.endeavourhealth.imapi.model.fhir.CodeSystem;
import org.endeavourhealth.imapi.model.fhir.FHIRDocument;
import org.endeavourhealth.imapi.model.fhir.ValueSet;
import org.endeavourhealth.imapi.model.imq.QueryException;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.transforms.FHIRToIM;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class FHIRImporter implements TTImport {


	private TTDocument document;
	private TTManager manager;
	private static final Logger LOG = LoggerFactory.getLogger(FHIRImporter.class);
	private String valueSetFolder= Namespace.IM+"VSET_FHIR";
	private String codeSystemFolder= Namespace.IM+"FHIRCodeSystems";

	private static final String[] fhirResources = {
		".*\\\\FHIR\\\\FHIRValueSets.json",
		".*\\\\FHIR\\\\FHIRCodeSystems.json"
	};

	@Override
	public void importData(TTImportConfig config) throws ImportException {
		try {
			LOG.info("Importing FHIR Resources");
			manager = new TTManager();
			document = manager.createDocument();
			FHIRToIM converter = new FHIRToIM();
			for (String coreFile : fhirResources) {
				try (TTManager manager = new TTManager()) {
					Path path = ImportUtils.findFileForId(config.getFolder(), coreFile);
					FHIRDocument fhirDocument = loadDocument(path.toFile());
					if (fhirDocument.getValueSets() != null) {
						for (ValueSet fhirSet : fhirDocument.getValueSets()) {
							TTEntity set = converter.convertValueSet(fhirSet, iri(IM.VALUE_SET), valueSetFolder);
							document.addEntity(set);
						}

					}
					if (fhirDocument.getCodeSystems() != null) {
						for (CodeSystem codeSystem : fhirDocument.getCodeSystems()) {
							List<TTEntity> concepts = converter.convertCodeSystem(codeSystem, codeSystemFolder);
							document.getEntities().addAll(concepts);
						}
					}
					LOG.info("Filing {}", SCHEME.FHIR);
					try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
						try {
							filer.fileDocument(document);
						} catch (TTFilerException | QueryException e) {
							throw new IOException(e.getMessage());
						}
					}
				}
			}
		}catch (Exception e) {
			throw new ImportException(e.getMessage(), e);
		}
	}


	public FHIRDocument loadDocument(File inputFile) throws IOException {
		try (CachedObjectMapper om = new CachedObjectMapper()) {
			return om.readValue(inputFile, FHIRDocument.class);

		}
	}


	private void topFolders() {
		TTEntity entity = new TTEntity()
			.setIri(valueSetFolder)
			.addType(iri(IM.FOLDER))
			.setName(" based value set library")
			.setStatus(iri(IM.ACTIVE))
			.setDescription("A library of value sets generated from BNF codes and NHS BNF snomed maps");
		entity.addObject(iri(IM.IS_CONTAINED_IN), iri(Namespace.IM + "QueryConceptSets"));
		document.addEntity(entity);
	}






	@Override
	public void validateFiles(String inFolder) throws TTFilerException {
		ImportUtils.validateFiles(inFolder, fhirResources);

	}

	@Override
	public void close() throws Exception {

	}
}
