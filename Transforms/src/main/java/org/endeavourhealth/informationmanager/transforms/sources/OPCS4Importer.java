package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.TTArray;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
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
    private TTIriRef opcscodes= TTIriRef.iri(IM.GRAPH_ICD10.getIri()+"OPCS49Classification");

    private Set<String> snomedCodes;
    private final Map<String,TTEntity> codeToEntity= new HashMap<>();
    private final Map<String,TTEntity> altCodeToEntity= new HashMap<>();
    private ImportMaps importMaps = new ImportMaps();

    public TTImport importData(TTImportConfig config) throws Exception {
        System.out.println("Importing OPCS4.....");
        System.out.println("Checking Snomed codes first");
        snomedCodes= importMaps.importSnomedCodes();
        document = manager.createDocument(IM.GRAPH_OPCS4.getIri());
        document.addEntity(manager.createGraph(IM.GRAPH_OPCS4.getIri(),"OPCS4 code scheme and graph","OPCS4-9 official code scheme and graph"));
        importChapters(config.getFolder(),document);
        importEntities(config.getFolder(),document);

        mapDocument= manager.createDocument(IM.GRAPH_OPCS4.getIri());
        mapDocument.setCrud(IM.UPDATE);
        importMaps(config.getFolder());
        //Important to file after maps set
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(mapDocument);
        }
        return this;
    }

    public TTDocument importMaps(String folder) throws IOException, DataFormatException {
        Path file =  ImportUtils.findFileForId(folder,maps[0]);
         ComplexMapImporter mapImport= new ComplexMapImporter();
        mapImport.importMap(file.toFile(),mapDocument,altCodeToEntity,"1126441000000105",snomedCodes);
        return document;
    }

    private void importChapters(String inFolder, TTDocument document) throws IOException {
        Path file =  ImportUtils.findFileForId(inFolder, chapters[0]);
        TTEntity opcs= new TTEntity()
          .setIri(opcscodes.getIri())
          .addType(IM.CONCEPT)
          .setName("OPCS 4-9 Classification")
          .setCode("OPCS49Classification")
          .setScheme(IM.GRAPH_OPCS4)
          .setDescription("Classification of OPCS4 with chapter headings");
           opcs.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"CodeBasedTaxonomies"));
        document.addEntity(opcs);

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
                  .setScheme(IM.CODE_SCHEME_OPCS4)
                  .addType(IM.CONCEPT)
                    .set(IM.IS_CHILD_OF,new TTArray().add(iri(opcs.getIri())));
                codeToEntity.put(chapter,c);
                document.addEntity(c);
                line= reader.readLine();
            }
        }
        TTEntity c= new TTEntity()
        .setIri(IM.CODE_SCHEME_OPCS4.getIri()+"O")
          .setName("Overflow codes (chapter "+"O"+")")
          .setCode("O")
          .addType(IM.CONCEPT)
          .set(IM.IS_CHILD_OF,new TTArray().add(iri(opcs.getIri())));
        codeToEntity.put("O",c);
        document.addEntity(c);
    }

    private void importEntities(String folder, TTDocument document) throws IOException {

        Path file =  ImportUtils.findFileForId(folder, entities[0]);

        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            String line = reader.readLine();

            int count = 0;
            while (line != null && !line.isEmpty()) {
                count++;
                if (count % 10000 == 0) {
                    System.out.println("Processed " + count + " records");
                }
                String[] fields = line.split("\t");
                String code=fields[0];
                String altCode= fields[1];
                TTEntity c = new TTEntity()
                        .setCode(fields[0])
                  .setScheme(IM.CODE_SCHEME_OPCS4)
                        .setIri(IM.CODE_SCHEME_OPCS4.getIri() + (fields[0].replace(".","_")))
                        .addType(IM.CONCEPT);
                if (code.contains(".")){
                    String qParent= code.substring(0, code.indexOf("."));
                    TTEntity parent=codeToEntity.get(qParent);
                    c.addObject(IM.IS_CHILD_OF,TTIriRef.iri(parent.getIri()));
                } else {
                    String qParent= code.substring(0,1);
                    TTEntity parent=codeToEntity.get(qParent);
                    c.addObject(IM.IS_CHILD_OF,TTIriRef.iri(parent.getIri()));
                }
                codeToEntity.put(fields[0],c);
                altCodeToEntity.put(fields[1],c);
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