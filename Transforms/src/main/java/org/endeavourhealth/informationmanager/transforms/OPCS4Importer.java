package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.TTArray;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.*;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.DataFormatException;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class OPCS4Importer implements TTImport {

    private static final String[] entities = {".*\\\\nhs_opcs4df_9.0.0_.*\\\\OPCS49 CodesAndTitles.*\\.txt"};
    private static final String[] chapters = {".*\\\\nhs_opcs4df_9.0.0_.*\\\\OPCSChapters.*\\.txt"};
    private static final String[] maps = {".*\\\\SNOMED\\\\SnomedCT_UKClinicalRF2_PRODUCTION_.*\\\\Snapshot\\\\Refset\\\\Map\\\\der2_iisssciRefset_ExtendedMapUKCLSnapshot_GB1000000_.*\\.txt"};

    private TTManager manager= new TTManager();
    private TTDocument document;
    private TTDocument mapDocument;

    private Set<String> snomedCodes;
    private final Map<String,TTEntity> codeToEntity= new HashMap<>();

    public TTImport importData(TTImportConfig config) throws Exception {
        System.out.println("Importing OPCS4.....");
        System.out.println("Checking Snomed codes first");
        snomedCodes= ImportUtils.importSnomedCodes();
        document = manager.createDocument(IM.GRAPH_OPCS4.getIri());
        importChapters(config.folder,document);
        importEntities(config.folder,document);


        mapDocument= manager.createDocument(IM.MAP_SNOMED_OPCS.getIri());

        mapDocument.setCrud(IM.UPDATE);
        importMaps(config.folder);
        //Important to file after maps set
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }
        return this;
    }

    public TTDocument importMaps(String folder) throws IOException, DataFormatException {
        Path file = ImportUtils.findFileForId(folder,maps[0]);
        ComplexMapImporter mapImport= new ComplexMapImporter();
        mapImport.importMap(file.toFile(),mapDocument,codeToEntity,"1126441000000105",snomedCodes);
        return document;
    }

    private void importChapters(String inFolder, TTDocument document) throws IOException {
        Path file = ImportUtils.findFileForId(inFolder, chapters[0]);

        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            String line = reader.readLine();
            while (line != null && !line.isEmpty()) {
                String[] fields = line.split("\t");
                String chapter= fields[0];
                String term= fields[1];
                TTEntity c= new TTEntity();
                c.setIri(IM.CODE_SCHEME_OPCS4.getIri()+chapter)
                    .setName(term+" (chapter "+chapter+")")
                    .setCode(chapter)
                  .addType(IM.CONCEPT)
                    .set(IM.IS_CHILD_OF,new TTArray().add(iri(IM.NAMESPACE+"OPCS49Classification")));
                TTManager.addTermCode(c,term,chapter);
                document.addEntity(c);
                line= reader.readLine();
            }
        }
    }

    private void importEntities(String folder, TTDocument document) throws IOException {

        Path file = ImportUtils.findFileForId(folder, entities[0]);

        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            String line = reader.readLine();

            int count = 0;
            while (line != null && !line.isEmpty()) {
                count++;
                if (count % 10000 == 0) {
                    System.out.println("Processed " + count + " records");
                }
                String[] fields = line.split("\t");
                TTEntity c = new TTEntity()
                        .setCode(fields[0])
                  .setScheme(IM.CODE_SCHEME_OPCS4)
                        .setIri(IM.CODE_SCHEME_OPCS4.getIri() + (fields[0].replace(".","")))
                        .addType(IM.CONCEPT)
                    .set(IM.IS_CHILD_OF,new TTArray().add(iri(IM.CODE_SCHEME_OPCS4.getIri()+fields[0].substring(0,1))));
                codeToEntity.put(fields[0].replace(".",""),c);
                    if(fields[1].length()>250){
                        c.setName(fields[1].substring(0,150));
                    }else {
                        c.setName(fields[1]);
                    }

                    document.addEntity(c);
                    line = reader.readLine();
            }
            System.out.println("Imported " + count + " records");
        }
    }

    public OPCS4Importer validateFiles(String inFolder){
        ImportUtils.validateFiles(inFolder,entities,chapters,maps);
        return this;
    }

}
