package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class EMISImport implements TTImport {
    private static final Logger LOG = LoggerFactory.getLogger(EMISImport.class);
    public static final String EMIS = "http://endhealth.info/emis#";

    private static final String[] emisCodes = {".*\\\\EMIS\\\\emis_codes.txt"};
    private static final String[] allergies = {".*\\\\EMIS\\\\Allergies.json"};
    private static final String[] drugIds = {".*\\\\EMIS\\\\EMISDrugs.txt"};
    private final Map<String, TTEntity> codeIdToEntity = new HashMap<>();
    private final Map<String,TTEntity> oldCodeToEntity = new HashMap<>();
    private final Map<String, TTEntity> conceptIdToEntity = new HashMap<>();
    private final Map<String, TTEntity> snomedToEmis = new HashMap<>();
    private final Map<String, TTEntity> termToEmis = new HashMap<>();
    private final Map<String, List<String>> parentMap = new HashMap<>();
    private final Map<String, String> remaps = new HashMap<>();
    List<String> emisNs=Arrays.asList("1000006","1000033","1000034","1000035");

    private final TTManager manager = new TTManager();
    private TTDocument document;


    public EMISImport() {
    }


    /**
     * Imports EMIS , Read and EMIS codes and creates term code map to Snomed or local legacy entities
     * Requires vision maps to be populated
     *
     * @param config import configuration data
     * @throws Exception From document filer
     */


    public void importData(TTImportConfig config) throws Exception {
        System.out.println("Retrieving filed snomed codes");
        document = manager.createDocument(IM.GRAPH_EMIS.getIri());
        document.addEntity(manager.createGraph(IM.GRAPH_EMIS.getIri(), "EMIS (including Read) codes",
            "The EMIS local code scheme and graph including Read 2 and EMIS local codes."));
        System.out.println("importing emis code file");
        populateRemaps(remaps);
        addEMISUnlinked();
        importEMISCodes(config.getFolder());
        importDrugs(config.getFolder());
        manager.createIndex();
        allergyMaps(config.getFolder());
        setEmisHierarchy();
        supplementary();
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }
    }

    private void importDrugs(String folder) throws IOException {
        Path file =  ImportUtils.findFileForId(folder, drugIds[0]);
        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            reader.readLine();
            String line = reader.readLine();
            while (line != null && !line.isEmpty()) {
                String[] fields = line.split("\t");
                String descid = fields[0];
                String snomed = fields[1];
                String term = fields[2];
                if (!snomed.equals("NULL")) {
                    TTEntity emisConcept = snomedToEmis.get(snomed);
                    if (emisConcept == null)
                        emisConcept = termToEmis.get(term);
                    if (emisConcept != null)
                        if (notFoundValue(emisConcept,IM.HAS_TERM_CODE,IM.CODE,descid))
                            TTManager.addTermCode(emisConcept, null, descid);
                }
                line = reader.readLine();
            }
        }
    }

    private void supplementary() {
        addSub("EMISNQDT1", "310551000000106");
        addSub("EMISNQHA21", "428975001");
        addSub("TRISHE2", "16584000");
        addSub("EMISNQRO5", "415354003");
        addSub("EMISNQ1S1", "414259000");
        addSub("EMISNQ2N1", "415507003");
        addSub("EMISNQ3R1", "415712004");
    }

    private void addSub(String child, String parent) {
        TTEntity childEntity = oldCodeToEntity.get(child);
        childEntity.addObject(IM.MATCHED_TO, iri(SNOMED.NAMESPACE + parent));
    }

    private void allergyMaps(String folder) throws IOException {
        Path path =  ImportUtils.findFileForId(folder, allergies[0]);
         TTManager allMgr = new TTManager();
           TTDocument allDoc = allMgr.loadDocument(path.toFile());
           for (TTEntity all : allDoc.getEntities()) {
               String oldCode= all.getIri().substring(all.getIri().lastIndexOf("#")+1);
               oldCode= oldCode.replaceAll("_",".");
               TTEntity emisEntity = oldCodeToEntity.get(oldCode);
               for (TTValue superClass : all.get(IM.MATCHED_TO).getElements()) {
                   emisEntity.addObject(IM.MATCHED_TO, superClass);
               }
           }
    }

    public static void populateRemaps(Map<String,String> remaps) {
        remaps.put("65O2", "116813009");
        remaps.put("65O3", "268504008");
        remaps.put("65O4", "271498007");
        remaps.put("65O5", "384702009");
        remaps.put("65OZ", "709562004");
    }


    private void setEmisHierarchy() {
        for (Map.Entry<String, List<String>> entry : parentMap.entrySet()) {
            String child = entry.getKey();
            TTEntity childEntity = codeIdToEntity.get(child);
            List<String> parents = entry.getValue();
            for (String parentId : parents) {
                TTEntity parentEntity = codeIdToEntity.get(parentId);
                    TTManager.addChildOf(childEntity, TTIriRef.iri(parentEntity.getIri()));
                }
        }
    }


    private void addEMISUnlinked() {
        TTEntity c = new TTEntity().setIri(EMIS + "EMISUnlinkedCodes")
          .set(IM.IS_CHILD_OF, new TTArray().add(iri(EMIS + "EMISNHH2")))
          .setName("EMIS unlinked local codes")
          .setCode("EMISUnlinkedCodes");

        document.addEntity(c);
    }

    private void importEMISCodes(String folder) throws IOException {
        Path file =  ImportUtils.findFileForId(folder, emisCodes[0]);
        addEMISUnlinked();  //place holder for unlinked emis codes betlow the emis root code
        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            reader.readLine();
            int count = 0;
            String line = reader.readLine();
            while (line != null && !line.isEmpty()) {
                String[] fields = line.split("\t");
                count++;
                if (count % 100000== 0)
                    LOG.info("Written {} entities for "+ document.getGraph().getIri(), count);

                 EmisCode ec = new EmisCode();
                ec.setCodeId(fields[0]);
                ec.setTerm(fields[2]);
                ec.setCode(fields[3]);
                ec.setConceptId(fields[4]);
                if (isBlackList(fields[4])) {
                    ec.setConceptId(fields[3].replaceAll("\\^","").replaceAll("-","_"));
                   // LOG.warn(ec.getConceptId());
                }

                ec.setDescid(fields[5]);
                ec.setSnomedDescripton(fields[6]);
                if (fields.length==14)
                    if (!fields[13].equals(""))
                        ec.setParentId(fields[13]);
                addConcept(ec);
                line = reader.readLine();
            }
            LOG.info("{} codes imported",count);
        }

    }



    private void addConcept( EmisCode ec) {
        String codeId = ec.getCodeId();
        String term = ec.getTerm();
        String oldCode = ec.getCode();
        String conceptId = ec.getConceptId();
        String descid = ec.getDescid();
        String parentId = ec.getParentId();
        String snomedDescription= ec.snomedDescripton;


        if (parentId!=null)
            if (parentId.equals("")|parentId.equals("NULL"))
                parentId = null;
        if (descid.equals("")|descid.equals("NULL"))
            descid = null;


        String name = (term.length() <= 250)
          ? term
          : (term.substring(0, 200) + "...");
        TTEntity emisConcept = conceptIdToEntity.get(conceptId);
        if (emisConcept == null) {
            if (remaps.get(oldCode) != null)
                conceptId = remaps.get(oldCode);
            emisConcept = new TTEntity()
              .setIri(IM.CODE_SCHEME_EMIS.getIri() + conceptId)
              .setCode(conceptId)
              .addType(IM.CONCEPT)
              .setScheme(IM.CODE_SCHEME_EMIS);
            String mainTerm= snomedDescription;
            if (mainTerm.equals("")|mainTerm==null|mainTerm.equals("NULL"))
                mainTerm= name;
            emisConcept
              .setName(mainTerm);

            conceptIdToEntity.put(conceptId,emisConcept);
            document.addEntity(emisConcept);
        }
        emisConcept.addObject(IM.CODE_ID,codeId);
        if (notFoundValue(emisConcept,IM.HAS_TERM_CODE,IM.CODE,descid)){
            TTNode termCode= new TTNode();
            termCode.set(IM.CODE,TTLiteral.literal(descid));
            termCode.set(RDFS.LABEL,TTLiteral.literal(name));
            emisConcept.addObject(IM.HAS_TERM_CODE,termCode);
        }
        if (notFoundValue(emisConcept,IM.HAS_TERM_CODE,IM.OLD_CODE,oldCode)){
            TTNode termCode= new TTNode();
            termCode.set(IM.OLD_CODE,TTLiteral.literal(oldCode));
            termCode.set(RDFS.LABEL,TTLiteral.literal(name));
            emisConcept.addObject(IM.HAS_TERM_CODE,termCode);
        }

        codeIdToEntity.put(codeId,emisConcept);
        oldCodeToEntity.put(oldCode,emisConcept);
        termToEmis.put(term, emisConcept);
        if (isSnomed(conceptId)) {
            if (!isBlackList(conceptId)) {
                snomedToEmis.put(conceptId, emisConcept);
                if (notFound(emisConcept, IM.MATCHED_TO, TTIriRef.iri(SNOMED.NAMESPACE + conceptId)))
                   emisConcept.addObject(IM.MATCHED_TO, TTIriRef.iri(SNOMED.NAMESPACE + conceptId));
            }
            else {
                if (notFound(emisConcept, IM.IS_CHILD_OF, iri(EMIS + "EMISUnlinkedCodes")))
                     emisConcept.addObject(IM.IS_CHILD_OF, iri(EMIS + "EMISUnlinkedCodes"));
            }
        }
        if (oldCode.equals("EMISNHH2")) {
            emisConcept.setName("EMIS Read 2 and local codes");
            emisConcept.set(IM.IS_CONTAINED_IN, new TTArray()
              .add(iri(IM.NAMESPACE + "CodeBasedTaxonomies")));
        }
        if (parentId == null && !oldCode.equals("EMISNHH2"))
            emisConcept.set(IM.IS_CHILD_OF, new TTArray().add(iri(EMIS + "EMISUnlinkedCodes")));
        if (parentId != null) {
            parentMap.computeIfAbsent(codeId, k -> new ArrayList<>());
            parentMap.get(codeId).add(parentId);
        }
    }



    private boolean notFound(TTNode node, TTIriRef predicate, TTValue value){
        if (node.get(predicate)==null)
            return true;
        return !node.get(predicate).getElements().contains(value);
    }

    private boolean notFoundValue(TTNode node, TTIriRef predicate, TTIriRef subPredicate, String code){
        if (node.get(predicate)==null)
            return true;
        for (TTValue subNode:node.get(predicate).getElements()){
            if (subNode.asNode().get(subPredicate)==null)
                return true;
            else {
                for (TTValue already : subNode.asNode().get(subPredicate).getElements()) {
                    if (already.asLiteral().equals(TTLiteral.literal(code)))
                        return false;
                }
            }
        }
        return true;
    }



    public boolean isBlackList(String code){
        String[] blacklist= {"373873005"};
        return Arrays.asList(blacklist).contains(code);

    }



    public Boolean isSnomed(String s) {

        if (getNameSpace(s).equals(""))
            return true;
        return !emisNs.contains(getNameSpace(s));
    }


    public String getNameSpace(String s){
        if (s.length()>10)
         return s.substring(s.length()-10, s.length()-3);
        else
         return "";
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


    public void validateFiles(String inFolder){
         ImportUtils.validateFiles(inFolder,emisCodes,allergies);
    }

    @Override
    public void close() throws Exception {
        conceptIdToEntity.clear();
        codeIdToEntity.clear();
        snomedToEmis.clear();
        termToEmis.clear();
        parentMap.clear();
        remaps.clear();
        manager.close();
    }
}
