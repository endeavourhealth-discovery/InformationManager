package org.endeavourhealth.informationmanager.transforms;

import com.opencsv.CSVReader;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTDocumentFilerJDBC;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.common.transform.TTManager;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class R2EMISVisionImport implements TTImport {

    private static final String[] emisEntities = {".*\\\\EMIS\\\\EMISCodes.csv"};

    private final Set<String> visionCodes= new HashSet<>();
    private Set<String> snomedCodes;
    private final Map<String,TTEntity> codeIdToEntity= new HashMap<>();
    private final Map<String,String> codeIdToSnomed = new HashMap<>();
    private final Map<String,List<String>> parentMap = new HashMap<>();

    private Connection conn;
    private final TTManager manager= new TTManager();
    private TTDocument document;

    private final Map<String,TTEntity> entityMap = new HashMap<>();



    public R2EMISVisionImport(){}


    /**
     * Imports EMIS , Read and EMIS codes and creates term code map to Snomed or local legacy entities
     * Requires vision maps to be populated
     * @param inFolder root folder with sub folder of EMIS, READ
     * @throws Exception From document filer
     */


    public TTImport importData(String inFolder,boolean bulkImport,Map<String,Integer> entityMap) throws Exception {
        if (entityMap==null)
            entityMap= new HashMap<>();
        conn= ImportUtils.getConnection();
        System.out.println("Retrieving filed snomed codes");
        snomedCodes= ImportUtils.importSnomedCodes(conn);
        document = manager.createDocument(IM.GRAPH_EMIS.getIri());


        System.out.println("importing emis code file");
        addEMISUnlinked();
        importEMISCodes(inFolder);
        setEmisHierarchy();
        TTDocumentFiler filer = new TTDocumentFilerJDBC();
        filer.fileDocument(document,bulkImport,entityMap);

        System.out.println("importing vision codes");
        document= manager.createDocument(IM.GRAPH_VISION.getIri());
        importVisionCodes();

        addVisionMaps();
        filer = new TTDocumentFilerJDBC();
        filer.fileDocument(document,bulkImport,entityMap);
        return this;

    }



    private void addVisionMaps() throws SQLException {
        Map<String,TTEntity> backMaps= new HashMap<>();
        PreparedStatement getMaps = conn.prepareStatement("SELECT * from vision_read2_to_snomed_map");
        System.out.println("Retrieving Vision snomed maps");
        ResultSet rs = getMaps.executeQuery();
        while (rs.next()) {
            String code = rs.getString("read_code");
            String snomed = rs.getString("snomed_concept_id");
            if (isSnomed(snomed)) {
                if (visionCodes.contains(code)) {
                    TTEntity visionEntity = entityMap.get(code);
                    document.addEntity(TTManager.createTermCode(
                      iri(SNOMED.NAMESPACE+ snomed),IM.ADD,
                      visionEntity.getName(),code,IM.CODE_SCHEME_VISION,null
                    ));
                    TTEntity transaction = backMaps.get(snomed);
                    if (transaction==null) {
                        transaction = new TTEntity();
                        document.addEntity(transaction);
                        transaction.setCrud(IM.ADD);
                        transaction.setIri(SNOMED.NAMESPACE+snomed);
                        backMaps.put(snomed,transaction);
                        transaction.set(IM.HAS_MAP,new TTArray());
                        transaction.get(IM.HAS_MAP).asArray().add(new TTNode());
                    }

                    TTNode someNode= transaction.get(IM.HAS_MAP).asArray().get(0).asNode();
                    if (someNode.get(IM.SOME_OF)==null)
                        someNode.set(IM.SOME_OF,new TTArray());
                    TTNode mapNode= new TTNode();
                    someNode.get(IM.SOME_OF).asArray().add(mapNode);
                    mapNode.set(IM.MATCHED_TO,TTIriRef.iri(visionEntity.getIri()));
                    mapNode.set(IM.ASSURANCE_LEVEL,IM.SUPPLIER_ASSURED);
                }
            }
        }
    }

    private void importVisionCodes() throws SQLException {
        PreparedStatement getTerms= conn.prepareStatement("SELECT * from vision_read2_code");
        System.out.println("Retrieving terms from vision read+lookup2");
        ResultSet rs= getTerms.executeQuery();
        int count=0;
        while (rs.next()){
            count++;
            if(count%10000 == 0){
                System.out.println("Processed " + count +" terms");
            }
            String code= rs.getString("read_code");
            String term= rs.getString("read_term");
            int isVision = rs.getInt("is_vision_code");
            term=term+" ("+code+")";
            if (isVision==1&!visionCodes.contains(code)){
                TTEntity c= new TTEntity()
                    .setIri("vis:"+code)
                    .setName(term)
                    .setCode(code)
                    .setScheme(IM.CODE_SCHEME_VISION);
                c.set(IM.IS_CHILD_OF,iri("im:VisionLocalCodes"));
                visionCodes.add(code);
                entityMap.put(code,c);
                document.addEntity(c);
            }
        }
        System.out.println("Process ended with " + count +" additional Vision read like codes created");
    }


    private void setEmisHierarchy() {
        Map<String,TTEntity> backMap= new HashMap<>();
        for (Map.Entry<String,List<String>> entry:parentMap.entrySet()){
            String childId= entry.getKey();
            TTEntity childEntity= codeIdToEntity.get(childId);
          // if (childEntity.getCode().equals("^ESCT1270257"))
              //  System.out.println("parent is snomed");
            List<String> parents=entry.getValue();
            for (String parentId:parents) {
                if (codeIdToEntity.get(parentId)!=null) {
                    String parentIri = codeIdToEntity.get(parentId).getIri();
                    TTManager.addChildOf(childEntity, iri(parentIri));
                } else {
                    String parentIri= "sn:"+ codeIdToSnomed.get(parentId);
                    TTEntity transaction= backMap.get(parentIri);
                    if (transaction==null) {
                        transaction = new TTEntity();
                        transaction.setCrud(IM.ADD);
                        transaction.setIri(parentIri);
                        transaction.set(IM.HAS_MAP,new TTArray());
                        transaction.get(IM.HAS_MAP).asArray().add(new TTNode());
                        backMap.put(parentIri,transaction);
                        document.addEntity(transaction);
                    }
                    TTNode someNode= transaction.get(IM.HAS_MAP).asArray().get(0).asNode();
                    if (someNode.get(IM.SOME_OF)==null)
                        someNode.set(IM.SOME_OF,new TTArray());
                    TTNode mapNode= new TTNode();
                    someNode.get(IM.SOME_OF).asArray().add(mapNode);
                    mapNode.set(IM.MATCHED_TO,TTIriRef.iri(childEntity.getIri()));
                    mapNode.set(IM.ASSURANCE_LEVEL,IM.SUPPLIER_ASSURED);

                }
            }
        }
    }

    private void addEMISUnlinked(){
        TTEntity c= new TTEntity().setIri("emis:EMISUnlinkedCodes")
            .set(IM.IS_CHILD_OF,new TTArray().add(iri("emis:"+"EMISNHH2")))
            .setName("EMIS unlinked local codes")
            .setCode("^EMISUnlinkedCodes")
            .setScheme(IM.CODE_SCHEME_EMIS);
        c.set(IM.IS_CONTAINED_IN,new TTArray());
        c.get(IM.IS_CONTAINED_IN).asArray().add(TTIriRef.iri(IM.NAMESPACE+"CodeBasedTaxonomies"));
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
                if (count % 10000 == 0) {
                    System.out.println("Processed " + count + " records looking for new entities");
                }
                String codeid= fields[0];
                String term= fields[1];
                String emis= fields[2];
                String snomed= fields[3];
                String descid= fields[4];
                String parent= fields[10];

                if (parent.equals(""))
                    parent=null;
                if (descid.equals(""))
                    descid= null;

                String name = (term.length() <= 250)
                        ? term
                        : (term.substring(0, 200) + "...");
                name=name+" ("+emis+")";
                //is it a snomed code in disguise?
                if (isSnomed(snomed)){
                    document.addEntity(TTManager
                      .createTermCode(TTIriRef.iri(SNOMED.NAMESPACE+snomed),
                        IM.ADD,name,emis,IM.CODE_SCHEME_EMIS,descid));
                    codeIdToSnomed.put(codeid,snomed);
                } else {
                    TTEntity c = setLegacyEntity(IM.CODE_SCHEME_EMIS, name, emis, descid, parent);
                    codeIdToEntity.put(codeid, c);
                    if (!codeid.equals(descid))
                        c.set(IM.ALTERNATIVE_CODE, new TTArray()
                            .add(TTLiteral.literal(codeid)));
                    if (emis.equals("EMISNHH2"))
                        c.set(IM.IS_CONTAINED_IN, new TTArray()
                            .add(iri(IM.NAMESPACE + "CodeBasedTaxonomies")));
                    document.addEntity(c);
                    entityMap.put(emis, c);
                    if (parent == null && !emis.equals("EMISNHH2"))
                        c.set(IM.IS_CHILD_OF, new TTArray().add(iri("emis:EMISUnlinkedCodes")));
                    if (parent != null) {
                        parentMap.computeIfAbsent(codeid, k -> new ArrayList<>());
                        parentMap.get(codeid).add(parent);
                    }
                }
            }
            System.out.println("Process ended with " + count + " records");
        }

    }

    private TTEntity setLegacyEntity(TTIriRef codeScheme, String name, String code, String descid, String parent) {
        TTEntity c;
        String prefix="emis:";
        if (codeScheme.equals(IM.CODE_SCHEME_VISION))
            prefix="r2:";
        c = new TTEntity()
            .setName(name)
            .setCode(code)
            .setIri(prefix + code)
            .setScheme(codeScheme)
            .addType(IM.LEGACY);
        TTManager.addTermCode(c,name,descid,IM.CODE_SCHEME_EMIS_DESCRIPTION,descid);
        return c;
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



    public R2EMISVisionImport validateVisionTables(Connection conn) throws SQLException {
        PreparedStatement getVision = conn.prepareStatement("Select read_code from vision_read2_code limit 1");
        ResultSet rs= getVision.executeQuery();
        if (!rs.next()) {
            System.err.println("No Vision read look up table (vision_read2_code)");
            System.exit(-1);
        }
        PreparedStatement getVisions = conn.prepareStatement("Select read_code from vision_read2_to_snomed_map limit 1");
        rs= getVisions.executeQuery();
        if (!rs.next()) {
            System.err.println("No Vision Snomed look up table (vision_read2_to_snomed_map)");
            System.exit(-1);
        }
        return this;
    }

    public R2EMISVisionImport validateFiles(String inFolder){
        ImportUtils.validateFiles(inFolder,emisEntities);
        return this;
    }

    @Override
    public TTImport validateLookUps(Connection conn) throws SQLException, ClassNotFoundException {
        validateVisionTables(conn);
        return this;
    }


    @Override
    public void close() throws Exception {
        if (conn!=null)
            if (!conn.isClosed())
                conn.close();
        visionCodes.clear();
        if (snomedCodes!=null)
            snomedCodes.clear();
        codeIdToEntity.clear();;
        codeIdToSnomed.clear();
        parentMap.clear();

        entityMap.clear();
        if (document!=null) {
            if (document.getEntities() != null)
                document.getEntities().clear();
            if (document.getEntities() != null)
                document.getEntities().clear();
        }

    }
}
