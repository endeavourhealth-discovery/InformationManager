package org.endeavourhealth.informationmanager.transforms.sources;

import org.apache.commons.text.CaseUtils;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTLiteral;
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
    private static final String[] conceptCounts = {".*\\\\DiscoveryLive\\\\usage.txt"};
    private static final Map<String,TTEntity> oldIriEntity = new HashMap<>();
    private static TTDocument document;
    private static TTDocument statsDocument;
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
        statsDocument= manager.createDocument(IM.GRAPH_STATS.getIri());
        statsDocument.setCrud(IM.UPDATE);
        importv1Codes(inFolder);
        calculateWeightings();
       TTFilerFactory.setSkipDeletes(true);
        try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
           filer.fileDocument(document);
        }

        try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(statsDocument);
        }

        ImportUnmapped unm= new ImportUnmapped();
        unm.importUnmapped(inFolder);

        return this;

    }

    private void importv1Codes(String inFolder) throws Exception {
        System.out.println("Importing IMv1");
        Path file =  ImportUtils.findFileForId(inFolder, im1Codes[0]);
        int count = 0;
        try (BufferedReader reader= new BufferedReader(new FileReader(file.toFile()))) {
            writer= new FileWriter(file.toFile().getParentFile().toString()+"\\UnmappedConcepts.txt");
            writer.write("oldIri\tterm\tscheme\tcode\n");
            reader.readLine();
            String line = reader.readLine();
            while (line != null && !line.isEmpty()) {
                String[] fields = line.split("\t");
                while (fields.length<6) {
                    line = line + reader.readLine();
                    fields = line.split("\t");
                }
                String oldIri = fields[1];
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
                            System.err.println(im1Scheme + " :" + code + " : " + oldIri);
                            scheme = "X";
                    }
                    if (scheme == null)
                        throw new IOException();
                    if (!scheme.equals("X")) {
                        String lname = code;

                        if (scheme.equals(IM.CODE_SCHEME_TPP.getIri())) {
                            lname = lname.replace(".", "_");
                            if (entities.containsKey(scheme+lname)) {
                                checkEntity(scheme, lname, im1Scheme, term, code, oldIri,description);
                            }
                            else {
                                List<String> schemes = Arrays.asList(IM.NAMESPACE,SNOMED.NAMESPACE);
                                TTIriRef core = importMaps.getReferenceFromCoreTerm(term, schemes);
                                if (core != null) {
                                    addNewEntity(scheme+lname,core.getIri(),term,code,scheme,oldIri,description);
                                }
                                else {
                                    checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
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
                                checkEntity(scheme, lname, im1Scheme, term, code, oldIri,description);
                            }
                            else if (term.startsWith("[")) {
                                    String suffix = term.substring(1, term.indexOf("]"));
                                    String realName = lname.replace("_", "") + "-" + suffix;
                                    if (entities.containsKey(scheme + realName)) {
                                        addNewEntity(scheme + lname, scheme + realName, term, code,scheme,oldIri,description);
                                    }
                                    else if (entities.containsKey(scheme + code.replace(".", ""))) {
                                        realName = code.replace(".", "");
                                        addNewEntity(scheme+lname,scheme+realName,term,code,scheme,oldIri,description);
                                    }
                                    else
                                        checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
                                }
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_ENCOUNTERS.getIri())) {
                            lname = "LE_" + lname;
                            if (entities.containsKey(scheme+lname))
                                checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
                            else {
                                List<String> schemes = List.of(IM.CODE_SCHEME_ENCOUNTERS.getIri());
                                TTIriRef core = importMaps.getReferenceFromCoreTerm(term, schemes);
                                if (core != null) {
                                    addNewEntity(scheme+lname,core.getIri(),term,code,scheme,oldIri,description);
                                }
                                else {
                                    checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
                                }

                            }
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_ICD10.getIri())) {
                            lname = lname.replace(".", "_");
                            if (entities.containsKey(scheme + lname)){
                                checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
                            }
                            else if (lname.endsWith("X")) {
                                    String realName = lname.split("_")[0];
                                    if (entities.containsKey(scheme + realName)) {
                                        addNewEntity(scheme + lname, scheme + realName, term, code, scheme,oldIri,description);
                                    }
                                }
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_OPCS4.getIri())) {
                            lname = lname.replace(".", "_");
                            checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_DISCOVERY.getIri())) {
                            if (!checkOld(scheme, lname, term, code, oldIri, description,im1Scheme)) {
                                String original=lname;
                                if (original.contains("_")) {
                                    lname = lname.substring(lname.indexOf("_") + 1);
                                    if (original.startsWith("DM_"))
                                        lname= CaseUtils.toCamelCase(lname,false);
                                }
                                if (entities.containsKey(scheme + lname)) {
                                    checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
                                } else {
                                    List<String> schemes = List.of(IM.NAMESPACE);
                                    TTIriRef core = importMaps.getReferenceFromCoreTerm(term, schemes);
                                    if (core != null) {
                                        lname = core.getIri().split("#")[1];
                                        checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
                                    } else {
                                            checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
                                        }


                                }
                            }
                        }
                        else if (scheme.equals(SNOMED.NAMESPACE)){
                            if (getNameSpace(code).equals(visionNamespace)){
                                scheme= IM.CODE_SCHEME_VISION.getIri();
                            }
                            checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_BARTS_CERNER.getIri())){
                            if (entities.containsKey(scheme+lname)){
                                checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
                            }
                            else {
                                List<String> schemes = Arrays.asList(IM.NAMESPACE,SNOMED.NAMESPACE);
                                TTIriRef core = importMaps.getReferenceFromCoreTerm(term, schemes);
                                if (core != null) {
                                    addNewEntity(scheme+lname,core.getIri(),term,code,scheme,oldIri,description);
                                }
                                else {
                                    checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
                                }

                            }
                        }
                        else {
                            checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
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

    private boolean checkOld(String scheme, String lname, String term, String code, String oldIri, String description,String im1Scheme) throws Exception {
        if (oldIriTerm.containsKey(lname)) {
            if (oldIriSnomed.get(lname) != null) {
                String newCode= oldIriSnomed.get(lname);
                if (entities.containsKey(IM.NAMESPACE + newCode)) {
                    checkEntity(IM.NAMESPACE, newCode, im1Scheme, term, code,
                      oldIri, description);
                    return true;
                } else {
                    throw new Exception("mssing snomed " + lname + " " +
                      oldIriSnomed.get(lname));
                }
            }
            else {
                List<String> schemes = List.of(IM.NAMESPACE);
                String oldTerm = oldIriTerm.get(lname);
                TTIriRef core = importMaps.getReferenceFromCoreTerm(oldTerm, schemes);
                if (core != null) {
                    lname = core.getIri().split("#")[1];
                    checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
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

    private void checkEntity(String scheme, String lname, String im1Scheme,String term, String code, String oldIri,
                             String description) throws IOException {
        if (entities.containsKey(scheme+lname)) {
            addIM1id(scheme + lname, oldIri);
        }
        else {
					writer.write(oldIri+ "\t" + term + "\t" + im1Scheme + "\t" + code + "\t" + description + "\t" + (used.getOrDefault(oldIri, 0)) + "\n");
				}

    }



    private void addNewEntity(String newIri, String matchedIri, String term, String code,String scheme,String oldIri,
                              String description) {
        TTIriRef graph=TTIriRef.iri(newIri.substring(0,newIri.lastIndexOf("#")+1));
        TTEntity entity= new TTEntity()
          .setGraph(graph)
          .setIri(newIri)
          .setName(term)
          .setScheme(TTIriRef.iri(scheme))
          .setCode(code)
          .setStatus(IM.INACTIVE)
          .setDescription(description)
          .set(IM.IM1ID,TTLiteral.literal(oldIri));
        Set<String> matches= entities.get(matchedIri);
        if (matches!=null) {
            for (String iri : matches)
                entity.addObject(IM.MATCHED_TO, TTIriRef.iri(iri));
        }
        if (used.containsKey(oldIri))
            entity.set(IM.USAGE_TOTAL,TTLiteral.literal(used.get(oldIri)));
        document.addEntity(entity);
    }
    private void addIM1id(String iri,String oldIri){
        TTEntity im1= new TTEntity().setIri(iri);
        TTIriRef graph=TTIriRef.iri(iri.substring(0,iri.lastIndexOf("#")+1));
        im1.setGraph(graph);
        im1.addObject(IM.IM1ID,TTLiteral.literal(oldIri));
        if (used.containsKey(oldIri))
            im1.set(IM.USAGE_TOTAL,TTLiteral.literal(used.get(oldIri)));
        oldIriEntity.put(oldIri,im1);
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
                    String oldIri = fields[0];
                    int count = Integer.parseInt(fields[1]);
                    used.putIfAbsent(oldIri,0);
                    used.put(oldIri,used.get(oldIri)+count);
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
                    String term= fields[2];
                    if (oldIri.startsWith(":CM"))
                        oldIriSnomed.put(oldIri,snomed);
                    oldIriTerm.put(oldIri,term);
                    line = reader.readLine();
                }
            }
        }
    }



    private void calculateWeightings(){
        Map<String,TTEntity> weightedEntities= new HashMap<>();
        for (Map.Entry<String,Integer> usage:used.entrySet()) {
            String oldIri = usage.getKey();
            Integer count = usage.getValue();
            if (count > 0) {
                TTEntity im1 = oldIriEntity.get(oldIri);
                if (im1 != null) {
                    String im1Iri = im1.getIri();
                    if (entities.get(im1Iri) != null) {
                        for (String coreIri : entities.get(im1Iri)) {
                            TTEntity coreEntity = weightedEntities.get(coreIri);
                            if (coreEntity == null) {
                                coreEntity = new TTEntity();
                                coreEntity.setIri(coreIri);
                                statsDocument.addEntity(coreEntity);
                                weightedEntities.put(coreIri,coreEntity);
                            }
                            if (coreEntity.get(IM.WEIGHTING) != null)
                                coreEntity.set(IM.WEIGHTING, TTLiteral.literal(coreEntity.get(IM.WEIGHTING).asLiteral().intValue() + count));
                            else
                                coreEntity.set(IM.WEIGHTING, TTLiteral.literal(count));
                        }
                    }
                }
            }
        }

    }


    @Override
	public TTImport validateFiles(String inFolder)  {

         ImportUtils.validateFiles(inFolder,conceptCounts,im1Codes,oldIris);
         return this;
	}


}
