package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.transforms.sources.ImportUtils;

import java.io.IOException;
import java.nio.file.Path;

public class ConfigImporter implements TTImport {

    private static final String[] config = {".*\\\\Config.json"};
    private final TTManager manager= new TTManager();

    private TTDocument document;

    @Override
    public TTImport importData(TTImportConfig ttImportConfig) throws Exception {

        document = manager.createDocument(IM.GRAPH_CONFIG.getIri());
        document.addEntity(manager.createGraph(IM.GRAPH_CONFIG.getIri(), "Config", "Config"));

        importConfig(ttImportConfig.getFolder());

        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }

        return this;
    }

    private void importConfig(String folder) throws IOException {
        Path file = ImportUtils.findFileForId(folder, config[0]);
        document= manager.loadDocument(file.toFile());
    }

    @Override
    public TTImport validateFiles(String inFolder) throws TTFilerException {
        ImportUtils.validateFiles(inFolder,config);
        return this;
    }
}
