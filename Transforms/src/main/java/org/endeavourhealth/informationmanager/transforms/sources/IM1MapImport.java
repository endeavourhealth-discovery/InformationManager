package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class IM1MapImport implements TTImport {
    private static final Logger LOG = LoggerFactory.getLogger(IM1MapImport.class);
    private static final String[] im1Codes = {".*\\\\IMv1\\\\concepts.txt"};
    private static final String[] oldIris= {  ".*\\\\IMv1\\\\oldiris.txt"};
    private static final String[] conceptCounts = {".*\\\\DiscoveryLive\\\\concept_counts.txt",
      ".*\\\\DiscoveryLive\\\\nwlencounter_counts.txt"};
    private static final Map<String,TTEntity> dbidMap= new HashMap<>();
    private static TTDocument document;
    private final Map<String,String> idToDbid= new HashMap<>();
    private final Map<String,String> dbidToId= new HashMap<>();
    private static Map<String,Set<String>> entities;
    private static final Map<String,TTEntity> iriToCore= new HashMap<>();
    private static final  ImportMaps importMaps = new ImportMaps();
    private static Map<String,Integer> used= new HashMap<>();
    private FileWriter writer;
    private String visionNamespace= "1000027";
    private final Map<String,String> oldIriTerm= new HashMap<>();
    private final Map<String,String> oldIriSnomed = new HashMap<>();


    @Override
	public TTImport importData(TTImportConfig config) throws Exception {
        importData(config.getFolder(), config.isSecure());
        return this;
    }

    public TTImport importData(String inFolder, boolean secure) throws Exception {

        importOld(inFolder);
        used= importCounts(inFolder);
        LOG.info("Retrieving all entities and matches...");
        entities= importMaps.getAllPlusMatches();
        TTManager manager = new TTManager();
        document = manager.createDocument(IM.GRAPH_IM1.getIri());
        TTDocument statsDocument= manager.createDocument(IM.GRAPH_STATS.getIri());
        statsDocument.setCrud(IM.UPDATE);
        importv1Codes(inFolder);
        System.out.println("Importing Concept counts");
        importStats(inFolder,statsDocument);
       TTFilerFactory.setSkipDeletes(true);
        try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
           filer.fileDocument(document);
        }

        try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(statsDocument);
        }
        return this;
    }

    private void importv1Codes(String inFolder) throws Exception {
        System.out.println("Importing IMv1");
        Path file =  ImportUtils.findFileForId(inFolder, im1Codes[0]);
        int count = 0;
        try (BufferedReader reader= new BufferedReader(new FileReader(file.toFile()))) {
            writer= new FileWriter(file.toFile().getParentFile().toString()+"\\UnmappedConcepts.txt");
            writer.write("dbid\tterm\tscheme\tcode\n");
            reader.readLine();
            String line = reader.readLine();
            while (line != null && !line.isEmpty()) {
                String[] fields = line.split("\t");
                while (fields.length<6) {
                    line = line + reader.readLine();
                    fields = line.split("\t");
                }
                String dbid= fields[2];
                String id = fields[1];
                idToDbid.put(id,dbid);
                dbidToId.put(dbid,id);
                String term=fields[2];
                String description= fields[3];
                String im1Scheme= fields[4];
                String code= fields[5];
                String scheme;
                if (!code.contains(",")) {
                    switch (im1Scheme) {
                        case "SNOMED":
                            scheme = SNOMED.NAMESPACE;
                            break;
                        case "READ2":
                            scheme = IM.CODE_SCHEME_EMIS.getIri();
                            break;
                        case "DS_DATE_PREC":
                            scheme = "X";
                            break;
                        case "DM+D":
                            scheme = SNOMED.NAMESPACE;
                            break;
                        case "CTV3":
                            scheme = IM.CODE_SCHEME_TPP.getIri();
                            break;
                        case "ICD10":
                            scheme = IM.CODE_SCHEME_ICD10.getIri();
                            break;
                        case "OPCS4":
                            scheme = IM.CODE_SCHEME_OPCS4.getIri();
                            break;
                        case "BartsCerner":
                            scheme = IM.CODE_SCHEME_BARTS_CERNER.getIri();
                            break;
                        case "FHIR_RFP":
                            scheme = "X";
                            break;
                        case "FHIR_RFT":
                            scheme = "X";
                            break;
                        case "FHIR_AG":
                            scheme = "X";
                            break;
                        case "FHIR_EC":
                            scheme = "X";
                            break;
                        case "FHIR_RT":
                            scheme = "X";
                            break;
                        case "FHIR_RS":
                            scheme = "X";
                            break;
                        case "FHIR_AS":
                            scheme = "X";
                            break;
                        case "FHIR_MSAT":
                            scheme = "X";
                            break;
                        case "FHIR_PRS":
                            scheme = "X";
                            break;
                        case "FHIR_AU":
                            scheme = "X";
                            break;
                        case "FHIR_CPS":
                            scheme = "X";
                            break;
                        case "FHIR_CPU":
                            scheme = "X";
                            break;
                        case "FHIR_CEP":
                            scheme = "X";
                            break;
                        case "EMIS_LOCAL":
                            scheme = IM.CODE_SCHEME_EMIS.getIri();
                            break;
                        case "TPP_LOCAL":
                            scheme = IM.CODE_SCHEME_TPP.getIri();
                            break;
                        case "LE_TYPE":
                            scheme = IM.CODE_SCHEME_ENCOUNTERS.getIri();
                            break;
                        case "VISION_LOCAL":
                            scheme = IM.CODE_SCHEME_VISION.getIri();
                            break;
                        case "CM_DiscoveryCode":
                            scheme = IM.CODE_SCHEME_DISCOVERY.getIri();
                            break;
                        case "FHIR_FPR":
                            scheme = "X";
                            break;
                        case "FHIR_LANG":
                            scheme = "X";
                            break;
                        case "ImperialCerner":
                            scheme = "X";
                            break;
                        case "NULL":
                            scheme = "X";
                            break;
                        default:
                            System.err.println(im1Scheme + " :" + code + " : " + id);
                            scheme = "X";
                    }
                    if (scheme == null)
                        throw new IOException();
                    if (!scheme.equals("X")) {
                        String lname = code;

                        if (scheme.equals(IM.CODE_SCHEME_TPP.getIri())) {
                            lname = lname.replace(".", "_");
                            if (entities.containsKey(scheme+lname)) {
                                checkEntity(scheme, lname, im1Scheme, term, code, id,description);
                            }
                            else {
                                List<String> schemes = Arrays.asList(IM.NAMESPACE,SNOMED.NAMESPACE);
                                TTIriRef core = importMaps.getReferenceFromCoreTerm(term, schemes);
                                if (core != null) {
                                    addNewEntity(scheme+lname,core.getIri(),term,code,scheme,id,description);
                                }
                                else {
                                    checkEntity(scheme,lname,im1Scheme,term,code,id,description);
                                }

                            }
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_EMIS.getIri())) {
                            lname = lname.replaceAll("[&/' |()^]", "_");
                            lname = lname.replace("[", "_").replace("]", "_");
                            if (im1Scheme.equals("READ2"))
                                lname = lname.replace(".", "");
                            else
                                lname = lname.replace(".", "_");
                            if (entities.containsKey(scheme+lname)) {
                                checkEntity(scheme, lname, im1Scheme, term, code, id,description);
                            }
                            else if (term.startsWith("[")) {
                                    String suffix = term.substring(1, term.indexOf("]"));
                                    String realName = lname.replace("_", "") + "-" + suffix;
                                    if (entities.containsKey(scheme + realName)) {
                                        addNewEntity(scheme + lname, scheme + realName, term, code,scheme,id,description);
                                    }
                                    else if (entities.containsKey(scheme + code.replace(".", ""))) {
                                        realName = code.replace(".", "");
                                        addNewEntity(scheme+lname,scheme+realName,term,code,scheme,id,description);
                                    }
                                    else
                                        checkEntity(scheme,lname,im1Scheme,term,code,id,description);
                                }
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_ENCOUNTERS.getIri())) {
                            lname = "LE_" + lname;
                            if (entities.containsKey(scheme+lname))
                                checkEntity(scheme,lname,im1Scheme,term,code,id,description);
                            else {
                                List<String> schemes = Arrays.asList(IM.CODE_SCHEME_ENCOUNTERS.getIri());
                                TTIriRef core = importMaps.getReferenceFromCoreTerm(term, schemes);
                                if (core != null) {
                                    addNewEntity(scheme+lname,core.getIri(),term,code,scheme,id,description);
                                }
                                else {
                                    checkEntity(scheme,lname,im1Scheme,term,code,id,description);
                                }

                            }
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_ICD10.getIri())) {
                            lname = lname.replace(".", "_");
                            if (entities.containsKey(scheme + lname)){
                                checkEntity(scheme,lname,im1Scheme,term,code,id,description);
                            }
                            else if (lname.endsWith("X")) {
                                    String realName = lname.split("_")[0];
                                    if (entities.containsKey(scheme + realName)) {
                                        addNewEntity(scheme + lname, scheme + realName, term, code, scheme,id,description);
                                    }
                                }
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_OPCS4.getIri())) {
                            lname = lname.replace(".", "_");
                            checkEntity(scheme,lname,im1Scheme,term,code,id,description);
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_DISCOVERY.getIri())) {
                            if (!checkOld(scheme, lname, term, code, id, description,im1Scheme)) {

                                if (lname.contains("_"))
                                    lname = lname.substring(lname.indexOf("_") + 1);
                                if (entities.containsKey(scheme + lname)) {
                                    checkEntity(scheme, lname, im1Scheme, term, code, id, description);
                                } else {
                                    List<String> schemes = Arrays.asList(IM.NAMESPACE);
                                    TTIriRef core = importMaps.getReferenceFromCoreTerm(term, schemes);
                                    if (core != null) {
                                        lname = core.getIri().split("#")[1];
                                        checkEntity(scheme, lname, im1Scheme, term, code, id, description);
                                    } else {
                                        checkEntity(scheme, lname, im1Scheme, term, code, id, description);
                                    }
                                }
                            }
                        }
                        else if (scheme.equals(SNOMED.NAMESPACE)){
                            if (getNameSpace(code).equals(visionNamespace)){
                                scheme= IM.CODE_SCHEME_VISION.getIri();
                            }
                            checkEntity(scheme,lname,im1Scheme,term,code,id,description);
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_BARTS_CERNER.getIri())){
                            if (entities.containsKey(scheme+lname)){
                                checkEntity(scheme,lname,im1Scheme,term,code,id,description);
                            }
                            else {
                                List<String> schemes = Arrays.asList(IM.NAMESPACE,SNOMED.NAMESPACE);
                                TTIriRef core = importMaps.getReferenceFromCoreTerm(term, schemes);
                                if (core != null) {
                                    addNewEntity(scheme+lname,core.getIri(),term,code,scheme,id,description);
                                }
                                else {
                                    checkEntity(scheme,lname,im1Scheme,term,code,id,description);
                                }

                            }
                        }
                        else {
                            checkEntity(scheme,lname,im1Scheme,term,code,id,description);
                        }
                    }
                }
                count++;
                if (count %10000== 0){
                    LOG.info("Imported {} im1 concepts",count);
                }
                line= reader.readLine();
            }
            writer.close();
        }
        System.out.println("Process ended with " + count);

	}

    private boolean checkOld(String scheme, String lname, String term, String code, String id, String description,String im1Scheme) throws Exception {
        if (oldIriTerm.containsKey(lname)) {
            if (oldIriSnomed.get(lname) != null) {
                String snomed = oldIriSnomed.get(lname);
                if (entities.containsKey(SNOMED.NAMESPACE + snomed)) {
                    checkEntity(SNOMED.NAMESPACE, snomed, im1Scheme, term, code,
                      id, description);
                    return true;
                } else {
                    throw new Exception("mssing snomed " + lname + " " +
                      oldIriSnomed.get(lname));
                }
            }
            else {
                List<String> schemes = Arrays.asList(IM.NAMESPACE);
                String oldTerm = oldIriTerm.get(lname);
                TTIriRef core = importMaps.getReferenceFromCoreTerm(oldTerm, schemes);
                if (core != null) {
                    lname = core.getIri().split("#")[1];
                    checkEntity(scheme, lname, im1Scheme, term, code, id, description);
                    return true;
                }
            }
        }
        return false;
    }

    public String getNameSpace(String s){
        if (s.length()>10)
            return s.substring(s.length()-10, s.length()-3);
        else
            return "";
    }

    private void checkEntity(String scheme, String lname, String im1Scheme,String term, String code, String id,
                             String description) throws IOException {
        if (entities.containsKey(scheme+lname))
            addIM1id(scheme+lname,id);
        else
            writer.write(id+"\t"+term+"\t"+im1Scheme+"\t" +code + "\t"+ description+"\t"+ (used.getOrDefault(id, 0)) + "\n");

    }

    private void addNewEntity(String newIri, String matchedIri, String term, String code,String scheme,String id,
                              String description) {
        TTEntity entity= new TTEntity()
          .setIri(newIri)
          .setName(term)
          .setScheme(TTIriRef.iri(scheme))
          .setCode(code)
          .setStatus(IM.INACTIVE)
          .setDescription(description)
          .set(IM.IM1ID,TTLiteral.literal(id));
        Set<String> matches= entities.get(matchedIri);
        if (matches!=null) {
            for (String iri : matches)
                entity.addObject(IM.MATCHED_TO, TTIriRef.iri(iri));
        }
        String dbid= idToDbid.get(id);
        if (used.containsKey(dbid))
            entity.set(IM.USAGE_TOTAL,TTLiteral.literal(used.get(dbid)));
        document.addEntity(entity);
    }
    private void addIM1id(String iri,String id){
        TTEntity im1= new TTEntity().setIri(iri);
        String dbid= idToDbid.get(id);
        im1.addObject(IM.IM1ID,TTLiteral.literal(id));
        if (used.containsKey(dbid))
            im1.set(IM.USAGE_TOTAL,TTLiteral.literal(used.get(dbid)));
        dbidMap.put(id, im1);
        document.addEntity(im1);
    }

    private Map<String,Integer> importCounts(String inFolder) throws IOException {
        for (String statsFile : conceptCounts) {
            Path file = ImportUtils.findFilesForId(inFolder, statsFile).get(0);
            System.out.println("Retrieving counts from..."+ file.toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                reader.readLine();  // NOSONAR - Skipping header
                String line = reader.readLine();
                while (line != null && !line.isEmpty()) {
                    String[] fields = line.split("\t");
                    String dbid = fields[0];
                    Integer count = Integer.parseInt(fields[fields.length - 1]);
                    if (count > 0) {
                        if (used.containsKey(dbid)) {
                            used.put(dbid, used.get(dbid) + count);
                        } else
                            used.put(dbid, count);
                    }
                    line = reader.readLine();
                }
            }
        }
        return used;
    }



    private void importOld(String inFolder) throws IOException {
        for (String oldFile : oldIris) {
            Path file = ImportUtils.findFilesForId(inFolder, oldFile).get(0);
            System.out.println("Retrieving old iri maps from..."+ file.toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                reader.readLine();  // NOSONAR - Skipping header
                String line = reader.readLine();
                while (line != null && !line.isEmpty()) {
                    String[] fields = line.split("\t");
                    String oldIri = fields[0];
                    if (oldIri.startsWith(":"))
                        oldIri= oldIri.substring(1);
                    String snomed= fields[1];
                    String term= fields[3];
                    String description= fields[4];
                    if (oldIri.startsWith(":CM"))
                        oldIriSnomed.put(oldIri,snomed);
                    oldIriTerm.put(oldIri,term);
                    line = reader.readLine();
                }
            }
        }
    }



    private void importStats(String inFolder, TTDocument statsDocument) throws IOException {
        for (String statsFile : conceptCounts) {
            Path file = ImportUtils.findFilesForId(inFolder, statsFile).get(0);
            System.out.println("Calculating weightings");
            Map<String, Set<String>> legacyMap = importMaps.getAllMatchedLegacy();
            int i = 0;
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                reader.readLine();  // NOSONAR - Skipping header
                String line = reader.readLine();
                while (line != null && !line.isEmpty()) {
                    String[] fields = line.split("\t");
                    String dbid = fields[0];
                    String id= dbidToId.get(dbid);
                    Integer count = Integer.parseInt(fields[fields.length - 1]);
                    TTEntity im1 = dbidMap.get(id);
                    if (im1 != null) {
                        String im1Iri = im1.getIri();
                        Set<String> coreConcepts;
                        if (im1Iri.contains(SNOMED.NAMESPACE) || im1Iri.contains(IM.NAMESPACE)) {
                            coreConcepts = new HashSet<>();
                            coreConcepts.add(im1Iri);
                        } else {
                            coreConcepts = legacyMap.get(im1Iri);
                        }
                        if (coreConcepts != null) {
                            for (String core : coreConcepts) {
                                TTEntity coreEntity = iriToCore.get(core);
                                if (coreEntity == null) {
                                    coreEntity = new TTEntity().setIri(core);
                                    iriToCore.put(core, coreEntity);
                                    statsDocument.addEntity(coreEntity);
                                }
                                if (coreEntity.get(IM.WEIGHTING) != null)
                                    coreEntity.set(IM.WEIGHTING, TTLiteral.literal(coreEntity.get(IM.WEIGHTING).asLiteral().intValue() + count));
                                else
                                    coreEntity.set(IM.WEIGHTING, TTLiteral.literal(count));

                            }
                        }
                    }

                    i++;
                    line = reader.readLine();
                }
            }
            LOG.info("Imported {} from stats file",i);
        }


    }


    @Override
	public TTImport validateFiles(String inFolder)  {

         ImportUtils.validateFiles(inFolder,conceptCounts,im1Codes,oldIris);
         return this;
	}


}
