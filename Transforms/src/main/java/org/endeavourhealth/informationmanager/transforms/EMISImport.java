package org.endeavourhealth.informationmanager.transforms;

import com.opencsv.CSVReader;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTDocumentFilerJDBC;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.TTImportConfig;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class EMISImport implements TTImport {

    private static final String[] emisEntities = {".*\\\\EMIS\\\\EMISCodes.csv"};

    private Set<String> snomedCodes;
    private final Map<String,TTEntity> emisToEntity = new HashMap<>();
    private final Map<String,TTEntity> codeIdToEntity= new HashMap<>();
    private final Map<String,String> codeIdToSnomed = new HashMap<>();
    private final Map<String,List<String>> parentMap = new HashMap<>();

    private Connection conn;
    private final TTManager manager= new TTManager();
    private TTDocument document;
    private TTDocument mapDocument;

    public EMISImport(){}


    /**
     * Imports EMIS , Read and EMIS codes and creates term code map to Snomed or local legacy entities
     * Requires vision maps to be populated
     * @param config import configuration data
     * @throws Exception From document filer
     */


    public TTImport importData(TTImportConfig config) throws Exception {
        conn= ImportUtils.getConnection();
        System.out.println("Retrieving filed snomed codes");
        snomedCodes= ImportUtils.importSnomedCodes(conn);
        document = manager.createDocument(IM.GRAPH_EMIS.getIri());
        mapDocument = manager.createDocument(IM.MAP_SNOMED_EMIS.getIri());
        System.out.println("importing emis code file");
        addEMISUnlinked();
        importEMISCodes(config.folder);
        setEmisHierarchy();
        TTDocumentFiler filer = new TTDocumentFilerJDBC();
        filer.fileDocument(document);

        filer = new TTDocumentFilerJDBC();
        filer.fileDocument(mapDocument);


        return this;

    }




    private void setEmisHierarchy() {
        Map<String,TTEntity> backMap= new HashMap<>();
        for (Map.Entry<String,List<String>> entry:parentMap.entrySet()){
            String child= entry.getKey();
            TTEntity childEntity= codeIdToEntity.get(child);
            List<String> parents=entry.getValue();
            for (String parentId:parents) {
                if (codeIdToEntity.get(parentId)!=null) {
                    String parentIri = codeIdToEntity.get(parentId).getIri();
                    TTManager.addChildOf(childEntity, iri(parentIri));
                }
            }
        }
    }

    private void addEMISUnlinked(){
        TTEntity c= new TTEntity().setIri("emis:EMISUnlinkedCodes")
            .set(IM.IS_CHILD_OF,new TTArray().add(iri("emis:"+"EMISNHH2")))
            .setName("EMIS unlinked local codes")
            .setCode("EMISUnlinkedCodes");

        document.addEntity(c);
    }
    private void importEMISCodes(String folder) throws IOException {
        Path file = ImportUtils.findFileForId(folder, emisEntities[0]);
        addEMISUnlinked();  //place holder for unlinked emis codes betlow the emis root code
        try( CSVReader reader = new CSVReader(new FileReader(file.toFile()))){
            reader.readNext();
            int count=0;
            String[] fields;
            while ((fields = reader.readNext()) != null) {
                count++;
                String codeid = fields[0];
                String term = fields[1];
                String emis = fields[2];
                String snomed = fields[3];
                String descid = fields[4];
                String parent = fields[10];

                if (parent.equals(""))
                    parent = null;
                if (descid.equals(""))
                    descid = null;

                String name = (term.length() <= 250)
                  ? term
                  : (term.substring(0, 200) + "...");
                TTEntity emisConcept= emisToEntity.get(emis);
                if (emisConcept==null) {
                    emisConcept = new TTEntity()
                      .setIri("emis:" + emis)
                      .addType(IM.CONCEPT)
                      .setName(name)
                      .setCode(emis);
                    document.addEntity(emisConcept);
                    emisToEntity.put(emis,emisConcept);
                }
                codeIdToEntity.put(codeid, emisConcept);
                if (isSnomed(snomed)) {
                    parent=null;
                    TTEntity snomedConcept= new TTEntity().setIri(SNOMED.NAMESPACE+ snomed);
                    snomedConcept.setCrud(IM.ADD);
                    mapDocument.addEntity(snomedConcept);
                    codeIdToSnomed.put(codeid, snomed);
                    TTManager.addSimpleMap(snomedConcept,emisConcept.getIri());
                }
                else {
                    emisConcept.addObject(IM.ALTERNATIVE_CODE, TTLiteral.literal(snomed));
                }

                emisConcept.addObject(IM.DESCRIPTION_ID,TTLiteral.literal(descid));

                if (emis.equals("EMISNHH2")) {
                    emisConcept.setName("EMIS Read 2 and local codes");
                    emisConcept.set(IM.IS_CONTAINED_IN, new TTArray()
                      .add(iri(IM.NAMESPACE + "CodeBasedTaxonomies")));
                }
                if (parent == null && !emis.equals("EMISNHH2"))
                    emisConcept.set(IM.IS_CHILD_OF, new TTArray().add(iri("emis:EMISUnlinkedCodes")));
                if (parent != null) {
                    parentMap.computeIfAbsent(codeid, k -> new ArrayList<>());
                    parentMap.get(codeid).add(parent);
                }
            }
            System.out.println("Process ended with " + count + " records");
        }

    }



    public Boolean isSnomed(String s){
        return snomedCodes.contains(s);
    }


    public String getNameSpace(String s){
        s = s.substring(s.length()-10, s.length()-3);
        return s;
    }

    public String getEmisCode(String code, String term) {

        int index = code.indexOf(".");
        if (index != -1) {
            code = code.substring(0, index);
        }
        if ("00".equals(term.substring(0, 2))) {
            return code;
        } else if(term.startsWith("1")){
            return code + "-" + term.charAt(1);
        }else {
            return code + "-" + term.substring(0,2);
        }
    }


    public EMISImport validateFiles(String inFolder){
        ImportUtils.validateFiles(inFolder,emisEntities);
        return this;
    }

    @Override
    public TTImport validateLookUps(Connection conn) throws SQLException, ClassNotFoundException {

        return this;
    }


    @Override
    public void close() throws Exception {
        if (conn!=null)
            if (!conn.isClosed())
                conn.close();
        if (snomedCodes!=null)
            snomedCodes.clear();
        codeIdToEntity.clear();;
        codeIdToSnomed.clear();
        parentMap.clear();

        if (document!=null) {
            if (document.getEntities() != null)
                document.getEntities().clear();
            if (document.getEntities() != null)
                document.getEntities().clear();
        }

    }
}
