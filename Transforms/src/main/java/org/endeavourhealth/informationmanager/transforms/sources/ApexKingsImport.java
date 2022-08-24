package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class ApexKingsImport implements TTImport {


	private static final String[] kingsPath = {".*\\\\Kings\\\\KingsPathMap.txt"};
	private static final String KINGS_APEX_CODES = IM.GRAPH_KINGS_APEX.getIri()+"KingsApexCodes";
	private TTDocument document;
	private Map<String, Set<String>> readToSnomed = new HashMap<>();
	private final Map<String, String> apexToRead = new HashMap<>();
	private final ImportMaps importMaps = new ImportMaps();


	@Override
	public void importData(TTImportConfig config) throws Exception {

        try (TTManager manager = new TTManager()) {
            document = manager.createDocument(IM.GRAPH_KINGS_APEX.getIri());
            document.addEntity(manager.createGraph(IM.GRAPH_KINGS_APEX.getIri(), "Kings Apex pathology code scheme and graph",
                "The Kings Apex LIMB local code scheme and graph"));

            importR2Matches();
            setTopLevel();
            importApexKings(config.getFolder());
            if (TTFilerFactory.isTransactional()) {
                new TTTransactionFiler(null).fileTransaction(document);
                return;
            }

            try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
                filer.fileDocument(document);
            }
        }
    }

	private void setTopLevel() {
		TTEntity kings= new TTEntity()
			.setIri(KINGS_APEX_CODES)
			.addType(IM.CONCEPT)
			.setName("Kings College Hospital Apex path codes")
			.setCode("KingsApexCodes")
			.setScheme(IM.GRAPH_KINGS_APEX)
			.setDescription("Local codes for the Apex pathology system in kings")
			.set(IM.IS_CONTAINED_IN,new TTArray().add(TTIriRef.iri(IM.NAMESPACE+"CodeBasedTaxonomies")));
			document.addEntity(kings);
	}


	private void importR2Matches() throws SQLException, TTFilerException, IOException {
		System.out.println("Retrieving read vision 2 snomed map");
		readToSnomed= importMaps.importReadToSnomed();

	}

	private void importApexKings(String folder) throws IOException {
		System.out.println("Importing kings code file");

		Path file =  ImportUtils.findFileForId(folder, kingsPath[0]);
		try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            Stream<String> lines = reader.lines().skip(1);

			AtomicInteger count = new AtomicInteger();
            lines.forEachOrdered(line -> {
				String[] fields = line.split("\t");
				String readCode= fields[0];
				String code= fields[1];
				String iri = IM.CODE_SCHEME_KINGS_APEX.getIri()+ (fields[1].replaceAll("[ .,\"%]",""));
				TTEntity entity= new TTEntity()
					.setIri(iri)
					.addType(IM.CONCEPT)
					.setName(fields[2])
					.setDescription("Local apex Kings trust pathology system entity ")
					.setCode(code)
					.setScheme(IM.CODE_SCHEME_KINGS_APEX)
					.set(IM.IS_CHILD_OF,new TTArray().add(TTIriRef.iri(KINGS_APEX_CODES)));
				document.addEntity(entity);
				apexToRead.put(code,readCode);
				if (readToSnomed.get(readCode)!=null){
					for (String snomed:readToSnomed.get(readCode)){
						entity.addObject(IM.MATCHED_TO,TTIriRef.iri(SNOMED.NAMESPACE+snomed));
					}
				}
				count.getAndIncrement();
				if (count.get() % 500 == 0) {
					System.out.println("Processed " + count + " records");
				}
            });
			System.out.println("Process ended with " + count + " records");
		}

	}


	@Override
	public void validateFiles(String inFolder) {
		 ImportUtils.validateFiles(inFolder,kingsPath);
	}


    @Override
    public void close() throws Exception {
        readToSnomed.clear();
        apexToRead.clear();
        importMaps.close();
    }
}
