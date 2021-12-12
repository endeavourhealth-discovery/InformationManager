package org.endeavourhealth.informationmanager.transforms;

import com.opencsv.CSVReader;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.*;

import javax.xml.stream.events.Characters;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;


/**
 * Creates the term code entity map for TPP codes
 * Creates new entities for TPP local codes that are unmapped
 */
public class TPPImporter implements TTImport{

    private static final String[] concepts = {".*\\\\TPP\\\\Concept.v3"};
    private static final String[] dcf = {".*\\\\TPP\\\\Dcf.v3"};
    private static final String[] descriptions = {".*\\\\TPP\\\\Descrip.v3"};
    private static final String[] terms = {".*\\\\TPP\\\\Terms.v3"};
    private static final String[] hierarchies = {".*\\\\TPP\\\\V3hier.v3"};
    private static final String[] nhsMap = {".*\\\\TPP\\\\CTV3SCTMAP.txt"};
    private static final String[] vaccineMaps = {".*\\\\TPP\\\\VaccineMaps.json"};
    private static final String[] tppCtv3Lookup = {".*\\\\TPP_Vision_Maps\\\\tpp_ctv3_lookup_2.csv"};
    private static final String[] tppCtv3ToSnomed = {".*\\\\TPP_Vision_Maps\\\\tpp_ctv3_to_snomed.csv"};
    private final TTManager manager= new TTManager();
    private Map<String,Set<String>> emisToSnomed;
    private TTDocument document;
    private TTDocument vDocument;
    private static final Map<String,TTEntity> codeToEntity= new HashMap<>();
    private static final Map<String,String> termCodes= new HashMap<>();


    public TTImport importData(TTImportConfig config) throws Exception {


        System.out.println("Looking for Snomed codes");

        document = manager.createDocument(IM.GRAPH_TPP.getIri());
        document.addEntity(manager.createGraph(IM.GRAPH_TPP.getIri(),"TPP CTV3 code scheme and graph",
          "CTV3 and TPP local code scheme and graph including CTV3 and local codes"));

        //Gets the emis read 2 codes from the IM to use as look up as some are missing
       // importEmis();
        importEMISMaps();

        addTPPTopLevel();
        inportTPPConcepts(config.folder);
        importTPPTerms(config.folder);
        importTPPDescriptions(config.folder);
        importTPPDcf(config.folder);
        importLocals(config.folder);

        importCV3Hierarchy(config.folder);

        //Imports the tpp terms from the tpp look up table
        importTppCtv3ToSnomed(config.folder);
        importnhsMaps(config.folder);
        addEmisMaps();

        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }
        importVaccineMaps(config.folder);
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(vDocument);
        }

        return this;

    }

    private void importVaccineMaps(String folder) throws IOException {
        Path file = ImportUtils.findFileForId(folder, vaccineMaps[0]);
        vDocument= manager.loadDocument(file.toFile());

    }

    private void addEmisMaps() {
        for (TTEntity entity:document.getEntities()){
            String code= entity.getCode();
            if (code!=null){
                if (!code.startsWith(".")) {
                    String scode = code.replace(".", "");
                    if (emisToSnomed.get(scode) != null) {
                        for (String snomed : emisToSnomed.get(scode)) {
                            if (!alreadyMapped(entity, snomed)) {
                                entity.addObject(IM.MATCHED_TO, iri(SNOMED.NAMESPACE + snomed));
                                System.out.println("new map " + code + " to " + snomed);
                            }
                        }
                    }
                }
            }
        }
    }



    private void importEMISMaps() throws SQLException, TTFilerException, ClassNotFoundException {
        System.out.println("Getting EMIS maps");
        emisToSnomed= ImportUtils.importEmisToSnomed();
    }

    private void importLocals(String folder) throws IOException {
        Path file = ImportUtils.findFileForId(folder, tppCtv3Lookup[0]);
        System.out.println("Importing TPP Ctv3 local codes");
        try (CSVReader reader = new CSVReader(new FileReader(file.toFile()))) {
            reader.readNext();
            String[] fields;
            int count = 0;
            while ((fields = reader.readNext()) != null){
                count++;
                if (count % 10000 == 0) {
                    System.out.println("Processed " + count);
                }
                String code = fields[0].replace("\"","");
                String term = fields[1];
                TTEntity tpp=codeToEntity.get(code);
                if (tpp==null){
                    tpp = new TTEntity().setIri(IM.CODE_SCHEME_TPP.getIri()+code.replace(".","_"));
                    tpp.setCode(code);
                    tpp.setName(term);
                    tpp.addType(IM.CONCEPT);
                    codeToEntity.put(code, tpp);
                    document.addEntity(tpp);

                }
            }
            System.out.println("Process ended with " + count);
        }
    }

    private void importnhsMaps(String folder) throws IOException{
            Path file = ImportUtils.findFileForId(folder, nhsMap[0]);
            System.out.println("Retrieving terms from tpp_TPP+lookup2");
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                reader.readLine();
                String line = reader.readLine();
                int count = 0;
                while (line != null && !line.isEmpty()) {
                    String[] fields = line.split("\t");
                    count++;
                    if (count % 10000 == 0) {
                        System.out.println("Processed " + count +" terms");
                    }
                    String code = fields[0];
                    String snomed= fields[2];
                    TTEntity tpp= codeToEntity.get(code);
                    if (tpp!=null) {
                        if (!alreadyMapped(tpp, snomed))
                            tpp.addObject(IM.MATCHED_TO, iri(SNOMED.NAMESPACE + snomed));
                    }

                    line = reader.readLine();
                }
                System.out.println("Process ended with " + count +" entities created");
            }


    }

    private boolean alreadyMapped(TTEntity tpp, String snomed) {
        if (tpp.get(IM.MATCHED_TO)==null)
            return false;
        for (TTValue superClass:tpp.get(IM.MATCHED_TO).asArray().getElements()){
            if (superClass.asIriRef().getIri().split("#")[1].equals(snomed))
                return true;
        }
        return false;
    }

    private void importCV3Hierarchy(String path) throws IOException {
        for (String hierFile : hierarchies) {
            Path file = ImportUtils.findFilesForId(path, hierFile).get(0);
            System.out.println("Processing  hierarchy in " + file.getFileName().toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                String line = reader.readLine();
                int count = 0;
                while (line != null && !line.isEmpty()) {
                    count++;
                    String[] fields = line.split("\\|");
                    String child= fields[0];
                    String parent= fields[1];
                    TTEntity tpp= codeToEntity.get(child);
                    if (tpp!=null) {
                        if (!parent.startsWith(".")) {
                            TTManager.addChildOf(tpp, iri(IM.CODE_SCHEME_TPP.getIri()+ parent));
                        } else {
                            TTManager.addChildOf(tpp, iri(IM.CODE_SCHEME_TPP.getIri()+"TPPCodes"));
                        }
                    }
                    line = reader.readLine();
                }
                System.out.println("Imported " + count + " hierarchy nodes");
            }
        }

    }

    private void importTPPTerms(String path) throws IOException {
        int i = 0;
        for (String termFile : terms) {
            Path file = ImportUtils.findFilesForId(path, termFile).get(0);
            System.out.println("Processing  terms    in " + file.getFileName().toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                reader.readLine(); // Skip header
                String line = reader.readLine();
                while (line != null && !line.isEmpty()) {
                    String[] fields = line.split("\\|");
                    String termCode = fields[0];
                    String term = fields[2].replace("\t", "");
                    if (fields.length > 3 && !fields[3].equals(""))
                        term = fields[3].replace("\t", "");
                    termCodes.put(termCode, term);
                    i++;
                    line = reader.readLine();
                }
            }
        }
        System.out.println("Imported " + i + " term codes");
    }

    private void importTPPDescriptions(String path) throws IOException {
        int i = 0;
        for (String conceptFile : descriptions) {
            Path file = ImportUtils.findFilesForId(path, conceptFile).get(0);
            System.out.println("Processing  descriptions in " + file.getFileName().toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                String line = reader.readLine();
                while (line != null && !line.isEmpty()) {
                    String[] fields = line.split("\\|");
                    String concept= fields[0];
                    String termCode=fields[1];
                    String termType=fields[2];
                    String term= termCodes.get(termCode);
                    if (term!=null){
                        TTEntity tpp= codeToEntity.get(concept);
                        if (tpp!=null) {
                            TTManager.addTermCode(tpp, term,termCode);
                            if (termType.equals("P"))
                                tpp.setName(term);
                        }
                    }
                    i++;
                    line = reader.readLine();
                }
            }
        }
        System.out.println("Imported " + i + " term codes");
    }

    private void importTPPDcf(String path) throws IOException {
        int i = 0;
        for (String conceptFile : dcf) {
            Path file = ImportUtils.findFilesForId(path, conceptFile).get(0);
            System.out.println("Processing  replacememnts in " + file.getFileName().toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                String line = reader.readLine();
                while (line != null && !line.isEmpty()) {
                    String[] fields = line.split("\\|");
                    String concept1= fields[1];
                    String termCode=fields[0];
                    if (termCode.equals("Yag2I"))
                        System.out.println("term code");
                    String concept2=fields[2];
                    String term= termCodes.get(termCode);
                    if (term!=null){
                        TTEntity tpp= codeToEntity.get(concept1);
                        if (tpp!=null) {
                            TTManager.addTermCode(tpp, term,termCode);
                            if (tpp.getName()==null)
                                tpp.setName(term);
                        }
                        TTEntity tpp1= codeToEntity.get(concept2);
                        if (tpp1!=null) {
                            TTManager.addTermCode(tpp1, term,termCode);
                            if (tpp1.getName()==null)
                                tpp1.setName(term);
                        }
                    }
                    i++;
                    line = reader.readLine();
                }
            }
        }
        System.out.println("Imported " + i + " term codes");
    }

    private void inportTPPConcepts(String path) throws IOException {
            int i = 0;
            for (String conceptFile : concepts) {
                Path file = ImportUtils.findFilesForId(path, conceptFile).get(0);
                System.out.println("Processing  concepts in " + file.getFileName().toString());
                try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                    String line = reader.readLine();
                    while (line != null && !line.isEmpty()) {
                        String[] fields = line.split("\\|");
                        String code= fields[0];
                        if (!Character.isLowerCase(code.charAt(0))) {
                            String iri = IM.CODE_SCHEME_TPP.getIri() +
                              (code.replace(".", "_"));
                            TTEntity tpp = new TTEntity().setIri(iri);
                            tpp.setCode(code);
                            tpp.setScheme(IM.CODE_SCHEME_TPP);
                            tpp.setStatus(IM.ACTIVE);
                            tpp.addType(IM.CONCEPT);
                            if (code.startsWith("."))
                                tpp.setStatus(IM.INACTIVE);
                            codeToEntity.put(code, tpp);
                            document.addEntity(tpp);
                        }
                        i++;
                        line = reader.readLine();
                    }
                }
            }
            System.out.println("Imported " + i + " concepts");
    }

    private void addTPPTopLevel(){
        TTEntity c= new TTEntity().setIri("tpp:TPPCodes")
          .addType(IM.CONCEPT)
          .setName("TPP TPP and local codes")
          .setScheme(IM.GRAPH_TPP)
          .setCode("TPPCodes");
        c.set(IM.IS_CONTAINED_IN,new TTArray());
        c.get(IM.IS_CONTAINED_IN).asArray().add(TTIriRef.iri(IM.NAMESPACE+"CodeBasedTaxonomies"));
        document.addEntity(c);
    }

    @Override
    public TTImport validateFiles(String inFolder) {
        ImportUtils.validateFiles(inFolder,concepts,descriptions,dcf,terms,hierarchies,tppCtv3Lookup,tppCtv3ToSnomed,nhsMap,vaccineMaps);
        return this;
    }



    private void importTppCtv3ToSnomed(String folder) throws IOException {
        Path file = ImportUtils.findFileForId(folder, tppCtv3ToSnomed[0]);
        System.out.println("Importing TPP Ctv3 to Snomed");
        try (CSVReader reader = new CSVReader(new FileReader(file.toFile()))) {
            reader.readNext();
            String[] fields;
            int count = 0;
            while ((fields = reader.readNext()) != null) {
                count++;
                if (count % 10000 == 0) {
                    System.out.println("Processed " + count);
                }
                String code = fields[0];
                String snomed = fields[1];
                TTEntity tpp=codeToEntity.get(code);
                if (tpp==null){
                    tpp = new TTEntity().setIri(IM.CODE_SCHEME_TPP.getIri()+code.replace(".","_"));
                    tpp.setCode(code);
                    tpp.setName("TPP local code. name unknown");
                    tpp.addType(IM.CONCEPT);
                    codeToEntity.put(code, tpp);
                    document.addEntity(tpp);

                }
                if (!alreadyMapped(tpp, snomed)) {
                    tpp.addObject(IM.MATCHED_TO, iri(SNOMED.NAMESPACE + snomed));
                }
            }
            System.out.println("Process ended with " + count);
        }
    }


    public String[] readQuotedCSVLine(String line) {
        String[] fields = line.split(",");
        if(fields.length>3){
            for(int i=2;i<fields.length-1;i++){
                fields[1]=fields[1].concat(",").concat(fields[i]);
            }
        }
        return fields;
    }


}
