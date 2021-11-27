package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTFilerFactory;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.TTImportConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.DataFormatException;


public class ICD10Importer implements TTImport {

    private static final String[] entities = {".*\\\\icd_df_10.5.0_20151102000001\\\\ICD10_Edition5_.*\\\\Content\\\\ICD10_Edition5_CodesAndTitlesAndMetadata_GB_.*\\.txt"};
    private static final String[] maps = {".*\\\\SNOMED\\\\SnomedCT_UKClinicalRF2_PRODUCTION_.*\\\\Snapshot\\\\Refset\\\\Map\\\\der2_iisssciRefset_ExtendedMapUKCLSnapshot_GB1000000_.*\\.txt"};
    private static final String[] chapters = {".*\\\\icd_df_10.5.0_20151102000001\\\\ICD10_Edition5_.*\\\\Content\\\\ICD10-Chapters.txt"};


    private final TTIriRef icd10Codes= TTIriRef.iri(IM.CODE_SCHEME_ICD10.getIri()+"ICD10Codes");
    private final TTManager manager= new TTManager();
    private Set<String> snomedCodes;
    private final Map<String,TTEntity> startChapterMap= new HashMap<>();
    private final List<String> startChapterList= new ArrayList<>();
    private TTDocument document;
    private TTDocument mapDocument;
    private final Map<String,TTEntity> codeToEntity= new HashMap<>();
    private final Map<String,TTEntity> noDotCodeToEntity= new HashMap<>();

    @Override
    public TTImport importData(TTImportConfig config) throws Exception {
        validateFiles(config.folder);
        System.out.println("Importing ICD10....");
        System.out.println("Getting snomed codes");
        snomedCodes= ImportUtils.importSnomedCodes();
        document = manager.createDocument(IM.GRAPH_ICD10.getIri());
        createTaxonomy();
        importChapters(config.folder,document);
        importEntities(config.folder, document);
        createHierarchy();


        mapDocument= manager.createDocument(IM.MAP_SNOMED_ICD10.getIri());
        mapDocument.setCrud(IM.ADD);
        importMaps(config.folder);
        try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(mapDocument);
        }
        return this;

    }

    private void createHierarchy() {
        Collections.sort(startChapterList);
        for (Map.Entry<String, TTEntity> entry : codeToEntity.entrySet()) {
            String code = entry.getKey();
            TTEntity icd10Entity = entry.getValue();
            if (code.contains(".")){
                String qParent= code.substring(0, code.indexOf("."));
                TTEntity parent=codeToEntity.get(qParent);
                icd10Entity.addObject(IM.IS_CHILD_OF,TTIriRef.iri(parent.getIri()));
            } else {
                int insertion = Collections.binarySearch(startChapterList,code);
                int parentIndex;
                if (insertion>-1)
                    parentIndex=insertion;
                else
                    parentIndex=-(insertion+1)-1;
                String qParent= startChapterList.get(parentIndex);
                TTEntity parent= startChapterMap.get(qParent);
               // System.out.println(code+" in "+ parent.getCode() +"?");
                icd10Entity.addObject(IM.IS_CHILD_OF,TTIriRef.iri(parent.getIri()));
            }

        }

    }

    private void createTaxonomy() {
        TTEntity icd10= new TTEntity()
          .setIri(icd10Codes.getIri())
          .setName("ICD10 5th edition classification codes")
          .addType(IM.CONCEPT)
          .setDescription("ICD1O classification used in backward maps from Snomed");
        icd10.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"CodeBasedTaxonomies"));
        document.addEntity(icd10);

    }

    public void importMaps(String folder) throws IOException, DataFormatException {

        validateFiles(folder);
        Path file = ImportUtils.findFileForId(folder,maps[0]);
        ComplexMapImporter mapImport= new ComplexMapImporter();
        mapImport.importMap(file.toFile(),mapDocument,noDotCodeToEntity,"999002271000000101",snomedCodes);
    }


    private void importChapters(String folder, TTDocument document) throws IOException {

        Path file = ImportUtils.findFileForId(folder, chapters[0]);
        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            reader.readLine();
            String line = reader.readLine();
            int count = 0;
            while (line != null && !line.isEmpty()) {
                count++;
                if (count % 10000 == 0) {
                    System.out.println("Processed " + count + " records");
                }
                String[] fields = line.split("\t");
                String iri= IM.CODE_SCHEME_ICD10.getIri()+fields[1];
                String code= fields[1];
                String label = "Chapter " + fields[0]+ ": "+ fields[2];
                TTEntity c = new TTEntity()
                  .setCode(code)
                  .setName(label)
                  .setIri(iri)
                  .addType(IM.CONCEPT);
                c.addObject(IM.IS_CHILD_OF,icd10Codes);
                startChapterMap.put(code.substring(0,code.indexOf("-")),c);
                startChapterList.add(code.substring(0,code.indexOf("-")));
                document.addEntity(c);
                line = reader.readLine();
            }
            System.out.println("Process ended with " + count + " chapter records");
        }

    }


    private void importEntities(String folder, TTDocument document) throws IOException {

        Path file = ImportUtils.findFileForId(folder, entities[0]);
        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            reader.readLine();
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
                  .setScheme(IM.CODE_SCHEME_ICD10)
                  .setIri(IM.CODE_SCHEME_ICD10.getIri() + fields[1])
                  .addType(IM.CONCEPT);
                if(fields[4].length()>250){
                    c.setName(fields[4].substring(0,200));
                    c.setDescription(fields[4]);
                }else {
                    c.setName(fields[4]);
                }


                codeToEntity.put(fields[0],c);
                noDotCodeToEntity.put(fields[0].replace(".",""),c);
                document.addEntity(c);
                line = reader.readLine();
            }
            System.out.println("Process ended with " + count + " entities");

        }

    }




    public TTImport validateFiles(String path){
        ImportUtils.validateFiles(path,entities,maps,chapters);
        return this;
    }

}
