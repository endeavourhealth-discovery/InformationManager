package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTDocumentFilerJDBC;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.TTImportConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;


/**
 * Creates the term code entity map for TPP codes
 * Creates new entities for TPP local codes that are unmapped
 */
public class TPPImporter implements TTImport{

    private static final String[] concepts = {".*\\\\TPP\\\\Concept.v3"};
    private static final String[] descriptions = {".*\\\\TPP\\\\Descrip.v3"};
    private static final String[] terms = {".*\\\\TPP\\\\Terms.v3"};
    private static final String[] hierarchies = {".*\\\\TPP\\\\V3hier.v3"};
    private static final String[] tppCtv3Lookup = {".*\\\\TPP_Vision_Maps\\\\tpp_ctv3_lookup_2.csv"};
    private static final String[] tppCtv3ToSnomed = {".*\\\\TPP_Vision_Maps\\\\tpp_ctv3_to_snomed.csv"};
    private final TTManager manager= new TTManager();
    private Set<String> snomedCodes;
    private final Map<String,String> emisToSnomed = new HashMap<>();
    private final Map<String,String> tppCtv3ToToSnomed = new HashMap<>();
    private TTDocument document;
    private TTDocument mapDocument;
    private Connection conn;
    private static final Map<String,TTEntity> codeToEntity= new HashMap<>();
    private static final Map<String,String> termCodes= new HashMap<>();




    public TTImport importData(TTImportConfig config) throws Exception {


        conn=ImportUtils.getConnection();
        System.out.println("Looking for Snomed codes");
        //Gets the snomed codes from the IM to use as look up
        snomedCodes= ImportUtils.importSnomedCodes(conn);
        document = manager.createDocument(IM.GRAPH_TPP.getIri());
        mapDocument= manager.createDocument(IM.MAP_SNOMED_TPP.getIri());

        //Gets the emis read 2 codes from the IM to use as look up as some are missing
        importEmis();

        addTPPTopLevel();
        inportTPPConcepts(config.folder);
        importTPPTerms(config.folder);
        importTPPDescriptions(config.folder);

        importCV3Hierarchy(config.folder);

        //Imports the tpp terms from the tpp look up table
        importTppCtv3ToSnomed(config.folder);
        importTPPMaps(config.folder);

        TTDocumentFiler filer = new TTDocumentFilerJDBC();
        filer.fileDocument(document);
        filer = new TTDocumentFilerJDBC();
        filer.fileDocument(mapDocument);
        return this;

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
                            TTManager.addChildOf(tpp, iri("http://endhealth.info/tpp#" + parent));
                        } else {
                            TTManager.addChildOf(tpp, iri("http://endhealth.info/tpp#TPPCodes"));
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
                    String termCode= fields[0];
                    String term=fields[2].replace("\t","");
                    if (fields.length>3)
                     if (!fields[3].equals(""))
                            term=fields[3].replace("\t","");
                     termCodes.put(termCode,term);
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
                        if (!code.startsWith(".")) {
                            TTEntity tpp = new TTEntity().setIri("tpp:" + code);
                            tpp.setCode(code);
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
          .setCode("TPPCodes");
        c.set(IM.IS_CONTAINED_IN,new TTArray());
        c.get(IM.IS_CONTAINED_IN).asArray().add(TTIriRef.iri(IM.NAMESPACE+"CodeBasedTaxonomies"));
        document.addEntity(c);
    }

    @Override
    public TTImport validateFiles(String inFolder) {
        ImportUtils.validateFiles(inFolder,concepts,descriptions,terms,hierarchies,tppCtv3Lookup,tppCtv3ToSnomed);
        return this;
    }

    @Override
    public TTImport validateLookUps(Connection conn) throws SQLException, ClassNotFoundException {
        return this;
    }

    private void importEmis() throws SQLException {
        System.out.println("Importing EMIS/Read from IM for look up....");
        PreparedStatement getEMIS= conn.prepareStatement("SELECT ct.code as code,c.code as snomed\n"
            +"from term_code ct\n"
        +"join entity c on ct.entity = c.dbid\n"
            +"where c.scheme='"+ "http://endhealth.info/emis#'\n"
        +"and ct.code not like '%-%'");
        ResultSet rs= getEMIS.executeQuery();
        while (rs.next()){
            String emis= rs.getString("code");
            String snomed=rs.getString("snomed");
            emisToSnomed.put(emis,snomed);
        }
    }

    private void importTppCtv3ToSnomed(String folder) throws IOException {
        Path file = ImportUtils.findFileForId(folder, tppCtv3ToSnomed[0]);
        System.out.println("Importing TPP Ctv3 to Snomed");
        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            reader.readLine();
            String line = reader.readLine();
            int count = 0;
            while (line != null && !line.isEmpty()) {
                String[] fields = line.split(",");
                count++;
                if (count % 10000 == 0) {
                    System.out.println("Processed " + count);
                }
                String code = fields[0];
                String snomed = fields[1];
                tppCtv3ToToSnomed.put(code,snomed);
                line = reader.readLine();
            }
            System.out.println("Process ended with " + count);
        }
    }

    //Imports the used TPP codes provided by TPP.
    private void importTPPMaps(String folder) throws IOException {
        Path file = ImportUtils.findFileForId(folder, tppCtv3Lookup[0]);
        System.out.println("Retrieving terms from tpp_TPP+lookup2");
        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            reader.readLine();
            String line = reader.readLine();
            int count = 0;
            while (line != null && !line.isEmpty()) {
                String[] fields = readQuotedCSVLine(line);
                count++;
                if (count % 10000 == 0) {
                    System.out.println("Processed " + count +" terms");
                }
                String code = fields[0];
                String term= fields[1];
                code=code.replaceAll("\"","");
                term=term.replace("\t","").replaceAll("\"","");
                String snomed=tppCtv3ToToSnomed.get(code); //"" must be stripped if using file reader.
                TTEntity TPP= codeToEntity.get(code);
                if (TPP==null){
                    TPP = new TTEntity()
                            .setIri("tpp:" + code)
                            .setName(term)
                            .setCode(code)
                            .addType(IM.CONCEPT);
                    TPP.setCrud(IM.REPLACE);
                    TPP.set(IM.IS_CHILD_OF, new TTArray().add(iri(IM.NAMESPACE + "TPPUnlinkedCodes")));
                    document.addEntity(TPP);
                }
                if (snomed!=null){
                    if (isSnomed(snomed)) {
                        TTEntity snomedEntity= new TTEntity();
                        snomedEntity.setIri(SNOMED.NAMESPACE+ snomed);
                        mapDocument.addEntity(snomedEntity);
                        TTManager.addSimpleMap(snomedEntity,TPP.getIri());
                    }
                } else {
                    if (!code.startsWith(".")) {
                        snomed = emisToSnomed.get(code.replace(".", ""));
                        if (snomed != null) {
                            TTEntity snomedEntity = new TTEntity();
                            snomedEntity.setIri(SNOMED.NAMESPACE + snomed);
                            mapDocument.addEntity(snomedEntity);
                            TTManager.addSimpleMap(snomedEntity, TPP.getIri());
                        }
                    }
                }
                line = reader.readLine();
            }
            System.out.println("Process ended with " + count +" entities created");
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

    public Boolean isSnomed(String s){
        return snomedCodes.contains(s);
    }

    @Override
    public void close() throws Exception {
        if (conn!=null)
            if (!conn.isClosed())
                conn.close();
        if (emisToSnomed!=null)
            emisToSnomed.clear();
        if (snomedCodes!=null)
            snomedCodes.clear();
    }
}
