package org.endeavourhealth.informationmanager.transforms;

import com.opencsv.CSVReader;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTFilerFactory;
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
    private final Map<String,String> remaps= new HashMap<>();

    private Connection conn;
    private final TTManager manager= new TTManager();
    private TTDocument document;


    public EMISImport(){}


    /**
     * Imports EMIS , Read and EMIS codes and creates term code map to Snomed or local legacy entities
     * Requires vision maps to be populated
     * @param config import configuration data
     * @throws Exception From document filer
     */


    public TTImport importData(TTImportConfig config) throws Exception {
        System.out.println("Retrieving filed snomed codes");
        snomedCodes= ImportUtils.importSnomedCodes();
        document = manager.createDocument(IM.GRAPH_EMIS.getIri());
        System.out.println("importing emis code file");
        populateRemaps();
        addEMISUnlinked();
        importEMISCodes(config.folder);
        setEmisHierarchy();
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }


        return this;

    }

    private void populateRemaps() {
        remaps.put("65O2","116813009");
        remaps.put("65O3","268504008");
        remaps.put("65O4","271498007");
        remaps.put("65O5","384702009");
        remaps.put("65OZ","709562004");
    }


    private void setEmisHierarchy() {
        for (Map.Entry<String,List<String>> entry:parentMap.entrySet()){
            String child= entry.getKey();
            TTEntity childEntity= codeIdToEntity.get(child);
            //if (childEntity.getIri().contains("1B85-1"))
              //  System.out.println("1B85");
            List<String> parents=entry.getValue();
            for (String parentId:parents) {
                TTEntity parentEntity= codeIdToEntity.get(parentId);
                if (parentEntity!=null) {
                    String parentIri = codeIdToEntity.get(parentId).getIri();
                    if (isEMIS(childEntity.getCode())) {
                        if (!isCoreSublass(childEntity))
                            if (!isBlackList(parentIri.substring(parentIri.lastIndexOf("=")+1))) {
                                childEntity.addObject(RDFS.SUBCLASSOF, TTIriRef.iri(parentIri));
                            } else
                                TTManager.addChildOf(childEntity,iri(parentIri));
                        else
                            TTManager.addChildOf(childEntity, iri(parentIri));
                    }
                    else
                        TTManager.addChildOf(childEntity, iri(parentIri));
                }
            }
        }
    }

    private boolean isCoreSublass(TTEntity subclass) {
        if (subclass.get(RDFS.SUBCLASSOF)==null)
            return false;
        for (TTValue value:subclass.get(RDFS.SUBCLASSOF).asArray().getElements()) {
            if (value.asIriRef().getIri().contains(SNOMED.NAMESPACE))
                return true;
            if (value.asIriRef().getIri().contains(IM.NAMESPACE))
                return true;
        }
        return false;
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
                      .setIri(IM.CODE_SCHEME_EMIS.getIri() + emis.replace("^","%"))
                      .addType(IM.CONCEPT)
                      .setScheme(IM.CODE_SCHEME_EMIS)
                      .setName(name)
                      .setCode(emis);
                    document.addEntity(emisConcept);
                    emisToEntity.put(emis,emisConcept);
                }
                codeIdToEntity.put(codeid, emisConcept);
                if (isSnomed(snomed)) {
                    if (remaps.get(emis)!=null)
                        snomed=remaps.get(emis);
                    if (!isBlackList(snomed)) {
                        emisConcept.addObject(RDFS.SUBCLASSOF,
                          TTIriRef.iri(SNOMED.NAMESPACE + snomed));
                    } else
                        emisConcept.addObject(IM.IS_CHILD_OF,iri("emis:EMISUnlinkedCodes"));
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

    public boolean isBlackList(String code){
        String[] blacklist= {"373873005"};
        if (Arrays.asList(blacklist).contains(code))
            return true;
        else return false;

    }



    public Boolean isSnomed(String s){
        return snomedCodes.contains(s);
    }
    public Boolean isEMIS(String s){
     if (s.length()>5)
            return true;
     else if (s.contains("DRG")|s.contains("SHAPT")|s.contains("EMIS"))
         return true;
     else
        return false;
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



}
