package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.imapi.vocabulary.im.GRAPH;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;

public class NHSTfcImport implements TTImport {
	private static final String[] treatmentCodes = {".*\\\\NHSDD\\\\TreatmentFunctionCodes.txt"};
	private TTManager manager= new TTManager();
	private TTDocument document;
	private TTIriRef nhsTfc;

	@Override
	public void importData(TTImportConfig config) throws Exception {
		document = manager.createDocument(GRAPH.NHS_TFC.getIri());
		document.addEntity(manager.createGraph(GRAPH.NHS_TFC.getIri(),
			"NHS Data Dictionary Speciality and Treatment function codes"
				,"NHS Data dictionary concepts that are not snomed"));
		setNHSDD();
		importFunctionCodes(config.getFolder());
		try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
			filer.fileDocument(document);
		}
    }

	private void setNHSDD() {
		nhsTfc= TTIriRef.iri(GRAPH.NHS_TFC.getIri()+"NHSTfc");
		TTEntity nhs= new TTEntity()
			.setIri(nhsTfc.getIri())
			.setName("Main Specialty and Treatment Function Codes")
			.setScheme(GRAPH.NHS_TFC)
			.setCode("0")
			.addType(IM.CONCEPT)
			.setStatus(IM.ACTIVE);
		nhs.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE.iri+"CodeBasedTaxonomies"));
		document.addEntity(nhs);
	}

	private void importFunctionCodes(String folder) throws IOException {

		Path file = ImportUtils.findFileForId(folder, treatmentCodes[0]);
		try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
			reader.readLine();  // NOSONAR - Skipping header
			String line = reader.readLine();
			int count = 0;
			while (line != null && !line.isEmpty()) {
				count++;
				String[] fields = line.split("\t");
				String code= fields[0];
				String term= fields[1];
				String snomed= fields[2];
				TTEntity tfc= new TTEntity()
					.setIri(GRAPH.NHS_TFC.getIri()+code)
					.setName(term)
					.setScheme(GRAPH.NHS_TFC)
					.setCode(code)
					.addType(IM.CONCEPT)
					.setStatus(IM.ACTIVE);
				tfc.addObject(IM.IS_CHILD_OF,nhsTfc);
				tfc.addObject(IM.MATCHED_TO,TTIriRef.iri(SNOMED.NAMESPACE.iri+snomed));
				document.addEntity(tfc);
				line= reader.readLine();
			}
		}
	}

	@Override
	public void validateFiles(String inFolder) throws TTFilerException {
		ImportUtils.validateFiles(inFolder,treatmentCodes);
	}

    @Override
    public void close() throws Exception {
        manager.close();
    }
}
