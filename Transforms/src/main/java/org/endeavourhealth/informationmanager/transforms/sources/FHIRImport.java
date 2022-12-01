package org.endeavourhealth.informationmanager.transforms.sources;

import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.transforms.TTManager;

import java.io.File;
import java.nio.file.Path;
import java.util.Objects;

public class FHIRImport implements TTImport {
	private static final String[] resources = {".*\\\\FHIR"};

	@Override
	public void importData(TTImportConfig ttImportConfig) throws Exception {
		TTManager manager = new TTManager();
		Path directory=  ImportUtils.findFileForId(ttImportConfig.getFolder(),resources[0]);
		for (File fileEntry : Objects.requireNonNull(directory.toFile().listFiles())) {
			if (!fileEntry.isDirectory()) {
				String ext = FilenameUtils.getExtension(fileEntry.getName());
				if (ext.equalsIgnoreCase("json")) {
					System.out.println("..." + fileEntry.getName());
					manager.loadDocument(fileEntry);
					TTDocument document = manager.getDocument();
					System.out.println("Filing  " + document.getGraph().getIri() + " from " + fileEntry.getName());
					if (!TTFilerFactory.isBulk()) {
						TTTransactionFiler filer= new TTTransactionFiler(null);
						filer.fileTransaction(document);
					} else {
						try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
							filer.fileDocument(document);
						}
					}
				}
			}
		}

	}

	@Override
	public void validateFiles(String inFolder) throws TTFilerException {
		ImportUtils.validateFiles(inFolder,resources);
	}

	@Override
	public void close() throws Exception {

	}
}
