package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.filer.TTTransactionFiler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DeltaImporter implements TTImport {
	private static final String[] delta = {".*\\\\Deltas"};

	@Override
	public TTImport importData(TTImportConfig config) throws Exception {
		Path file =  ImportUtils.findFileForId(config.getFolder(), delta[0]);
		TTTransactionFiler filer= new TTTransactionFiler(file.toString());
		filer.fileDeltas();
		return null;
	}

	@Override
	public TTImport validateFiles(String inFolder) throws TTFilerException {
			if(!Files.exists(Paths.get(inFolder+"\\\\Deltas")))
				throw new TTFilerException(("No files found in [" + inFolder + "\\Deltas"+ "]"));
		return this;
	}
}
