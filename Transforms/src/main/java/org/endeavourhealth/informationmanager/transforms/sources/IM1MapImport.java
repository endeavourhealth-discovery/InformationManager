package org.endeavourhealth.informationmanager.transforms.sources;

import org.apache.commons.text.CaseUtils;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.logic.exporters.ImportMaps;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.DataFormatException;

public class IM1MapImport implements TTImport {
    private static final Logger LOG = LoggerFactory.getLogger(IM1MapImport.class);
    private static final String[] im1Codes = {".*\\\\IMv1\\\\concepts.txt"};
    private static final String[] oldIris= {  ".*\\\\IMv1\\\\oldiris.txt"};
    private static final String[] context= {  ".*\\\\IMv1\\\\ContextMaps.txt"};
    private static final String[] usageDbid = {
        ".*\\\\DiscoveryLive\\\\stats1.txt",
        ".*\\\\DiscoveryLive\\\\stats2.txt",
        ".*\\\\DiscoveryLive\\\\stats3.txt",
        ".*\\\\DiscoveryLive\\\\stats4.txt"
    };
    private static final Map<String,TTEntity> oldIriEntity = new HashMap<>();
    private static TTDocument document;
    private static TTDocument statsDocument;
    private static Map<String,Set<String>> entities;
    private static final Map<String,TTEntity> iriToCore= new HashMap<>();
    private static final ImportMaps importMaps = new ImportMaps();
    private static Map<String,Integer> used= new HashMap<>();
    private static final Map<Integer,Integer> usedDbid= new HashMap<>();
    private static final Set<Integer> numericConcepts= new HashSet<>();
    private FileWriter writer;
    private final Map<String,String> oldIriTerm= new HashMap<>();
    private final Map<String,String> oldIriSnomed = new HashMap<>();
    private final Map<String,Integer> IdToDbid = new HashMap<>();
    private final Map<String,TTEntity> iriToSet= new HashMap<>();



    @Override
	public void importData(TTImportConfig config) throws Exception {
        importData(config.getFolder(), config.isSecure());
    }

    public void importData(String inFolder, boolean secure) throws Exception {

        importOld(inFolder);
        importUsage(inFolder);
        LOG.info("Retrieving all entities and matches...");
        entities= importMaps.getAllPlusMatches();
        TTManager manager = new TTManager();
        document = manager.createDocument(IM.GRAPH_IM1.getIri());
        newSchemes();
        statsDocument= manager.createDocument(IM.GRAPH_STATS.getIri());
        importv1Codes(inFolder);
        importContext(inFolder);
        calculateWeightings();
        try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
           filer.fileDocument(document);
        }

        try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(statsDocument);
        }
    }

    private void importv1Codes(String inFolder) throws Exception {
        LOG.info("Importing IMv1");
        Path file =  ImportUtils.findFileForId(inFolder, im1Codes[0]);
        int count = 0;
        try (BufferedReader reader= new BufferedReader(new FileReader(file.toFile()))) {
            writer= new FileWriter(file.toFile().getParentFile().toString()+"/UnmappedConcepts.txt");
            writer.write("oldIri"+ "\t" + "term" + "\t" + "im1Scheme" + "\t" + "code" + "\t" + "description" + "\t" + "used"+ "\n");
            reader.readLine();
            String line = reader.readLine();
            while (line != null && !line.isEmpty()) {
                if (line.toLowerCase().contains("accurx")){
                    System.out.println(line);
                }
                String[] fields = line.split("\t");
                while (fields.length<6) {
                    line = line + reader.readLine();
                    fields = line.split("\t");
                }
                Integer dbid= Integer.parseInt(fields[0]);
                String oldIri = fields[1];
                if (oldIri.equals("CM_DidNotAttendEncounter"))
                    LOG.info("");
                if (usedDbid.get(dbid)!=null)
                    used.put(oldIri,usedDbid.get(dbid));
                IdToDbid.put(oldIri,dbid);
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
                            if (code.startsWith("LPV_Imp_Crn"))
                                scheme= IM.GRAPH_IMPERIAL_CERNER.getIri();
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
                            if (code.startsWith("DM_"))
                                LOG.info("data model property");
                            LOG.error("Scheme [{}], Code [{}], Iri [{}]", im1Scheme, code, oldIri);
                            scheme = "X";
                    }
                    if (scheme == null)
                        throw new IOException();
                    if (code.startsWith("_")){
                        if (scheme.equals("X")){
                            scheme=IM.CODE_SCHEME_ENCOUNTERS.getIri();

                        }
                    }
                    if (!scheme.equals("X")) {
                        String lname = code;

                        if (scheme.equals(IM.CODE_SCHEME_TPP.getIri())) {
                            lname = lname.replace(".", "_");
                            if (lname.contains("65MZ1"))
                                System.out.println("65MZ1");
                            if (entities.containsKey(scheme+lname)) {
                                checkEntity(scheme, lname, im1Scheme, term, code, oldIri,description);
                            }
                            else {
                                TTIriRef core = importMaps.getReferenceFromCoreTerm(term);
                                if (core != null) {
                                    addNewEntity(scheme+lname,core.getIri(),term,code,im1Scheme,oldIri,description,IM.CONCEPT);
                                }
                                else {
                                    checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
                                }

                            }
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_EMIS.getIri())) {
                            if (".....".equals(code)) {
                                LOG.warn("Skipping READ [.....]");
                            } else {
                                lname = lname.replaceAll("[&/' |()^]", "_");
                                lname = lname.replace("[", "_").replace("]", "_");
                                if (im1Scheme.equals("READ2"))
                                    lname = lname.replace(".", "");
                                else
                                    lname = lname.replace(".", "_");
                                if (entities.containsKey(scheme + lname)) {
                                    checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
                                } else if (term.startsWith("[")) {
                                    String suffix = term.substring(1, term.indexOf("]"));
                                    String realName = lname.replace("_", "") + "-" + suffix;
                                    if (entities.containsKey(scheme + realName)) {
                                        addIM1id(scheme + realName, oldIri);

                                    } else if (entities.containsKey(scheme + code.replace(".", ""))) {
                                        realName = code.replace(".", "");
                                        addIM1id(scheme + realName, oldIri);
                                    } else
                                        checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
                                }
                            }
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_ENCOUNTERS.getIri())) {
                            lname = "LE_" + lname;
                            if (entities.containsKey(scheme+lname))
                                checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
                            else {
                                TTIriRef core = importMaps.getReferenceFromCoreTerm(term);
                                if (core != null) {
                                    addIM1id(core.getIri(),oldIri);
                                }
                                else {
                                    checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
                                }

                            }
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_ICD10.getIri())) {
                            String icd10 = lname.replace(".", "");
                            if (entities.containsKey(scheme + icd10)){
                                checkEntity(scheme,icd10,im1Scheme,term,code,oldIri,description);
                            }
                            else if (lname.endsWith("X")) {
                                    String realName = lname.split("\\.")[0];
                                    if (entities.containsKey(scheme + realName)) {
                                        addIM1id(scheme+realName,oldIri);
                                    }
                                }
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_OPCS4.getIri())) {
                            lname = lname.replace(".", "");
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
                                    TTIriRef core = importMaps.getReferenceFromCoreTerm(term);
                                    if (core != null) {
                                            addIM1id(core.getIri(), oldIri);
                                    }
                                     else {
                                            checkEntity(scheme, lname, im1Scheme, term, code, oldIri, description);
                                        }
                                }
                            }
                        }
                        else if (scheme.equals(SNOMED.NAMESPACE)){
                            String visionNamespace = "1000027";
                            if (getNameSpace(code).equals(visionNamespace)){
                                scheme= IM.CODE_SCHEME_VISION.getIri();
                                addNewEntity(scheme+code,null,term,code,im1Scheme,oldIri,description,IM.CONCEPT);
                            }
                            checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
                        }

                        else if (scheme.equals(IM.CODE_SCHEME_BARTS_CERNER.getIri())){
                            if (entities.containsKey(scheme+lname)){
                                checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
                            }
                            else {
                                TTIriRef core = importMaps.getReferenceFromCoreTerm(term);
                                if (core != null) {
                                    addNewEntity(scheme+lname,core.getIri(),term,code,scheme,oldIri,description,IM.CONCEPT);
                                }
                                else {
                                    checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
                                }

                            }
                        }
                        else if (scheme.equals(IM.GRAPH_IMPERIAL_CERNER.getIri())){
                            if (entities.containsKey(scheme+lname)){
                                checkEntity(scheme,lname,im1Scheme,term,code,oldIri,description);
                            }
                            else {
                                TTIriRef core = importMaps.getReferenceFromCoreTerm(term);
                                if (core != null) {
                                    addNewEntity(scheme+lname,core.getIri(),term,code,im1Scheme,oldIri,description,IM.CONCEPT);
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
        LOG.info("Process ended with " + count);

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
                String oldTerm = oldIriTerm.get(lname);
                TTIriRef core = importMaps.getReferenceFromCoreTerm(oldTerm);
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
            if (oldIri.startsWith("CM_")) {
                String potential = oldIri.substring(oldIri.lastIndexOf("_") + 1);
                TTIriRef core = importMaps.getReferenceFromCoreTerm(getPhrase(potential));
                if (core != null) {
                    addIM1id(core.getIri(), oldIri);
                }
                else
                    createUnassigned(scheme,lname,im1Scheme,term,code,oldIri,description);
            } else {
                createUnassigned(scheme,lname,im1Scheme,term,code,oldIri,description);

            }
        }

    }
    private void createUnassigned(String scheme, String lname, String im1Scheme,String term, String code, String oldIri,
                             String description) throws IOException {
        TTEntity unassigned = new TTEntity();
        unassigned.setGraph(TTIriRef.iri(scheme));
        unassigned.setIri(scheme + lname);
        unassigned.setStatus(IM.UNASSIGNED);
        unassigned.set(IM.IM1ID, TTLiteral.literal(oldIri));
        unassigned.setName(term);
        if (description != null)
            unassigned.setDescription(description);
        if (code != null) {
            unassigned.set(IM.CODE, TTLiteral.literal(code));
            unassigned.setScheme(TTIriRef.iri(scheme));
        }
        unassigned.set(IM.IM1SCHEME, TTLiteral.literal(im1Scheme));
        if (scheme.equals(IM.CODE_SCHEME_ENCOUNTERS.getIri()))
            unassigned.set(IM.PRIVACY_LEVEL, TTLiteral.literal(1));
        if (used.get(oldIri) != null)
            if (used.get(oldIri) > 0)
                unassigned.set(IM.USAGE_TOTAL, TTLiteral.literal(used.get(oldIri)));
        if (scheme.equals(IM.NAMESPACE)) {
            if (oldIri.startsWith("DM_")) {
                unassigned.set(RDF.TYPE, RDF.PROPERTY);
            } else
                unassigned.set(RDF.TYPE, IM.CONCEPT);
        } else
            unassigned.set(RDF.TYPE, IM.CONCEPT);
        oldIriEntity.put(oldIri,unassigned);
        document.addEntity(unassigned);
        writer.write(oldIri + "\t" + term + "\t" + im1Scheme + "\t" + code + "\t" + description + "\t" + (used.getOrDefault(oldIri, 0)) + "\n");

    }


    private TTEntity addNewCoreEntity(String newIri, String term, String code,
                                  String description,TTIriRef type) {
        TTIriRef graph = TTIriRef.iri(newIri.substring(0, newIri.lastIndexOf("#") + 1));
        TTEntity entity = new TTEntity()
          .addType(type)
          .setGraph(graph)
          .setIri(newIri)
          .setName(term)
          .setScheme(TTIriRef.iri(newIri.substring(0, newIri.lastIndexOf("#") + 1)));
        if (code != null)
            entity.setCode(code);
        if (description != null) {
            entity.setDescription(description);
        }
        document.addEntity(entity);
        return entity;
    }





    private TTEntity addNewEntity(String newIri, String matchedIri, String term, String code,String scheme,String oldIri,
                              String description,TTIriRef type) {
        TTIriRef graph=TTIriRef.iri(newIri.substring(0,newIri.lastIndexOf("#")+1));
        TTEntity entity= new TTEntity()
          .addType(type)
          .setGraph(graph)
          .setIri(newIri)
          .setName(term)
          .setScheme(TTIriRef.iri(newIri.substring(0,newIri.lastIndexOf("#")+1)));
        if (code!=null)
          entity.setCode(code);
        if (description!=null) {
            entity.setDescription(description);
        }
        if (oldIri!=null)
          entity.set(IM.IM1ID,TTLiteral.literal(oldIri));
        if (scheme!=null)
        entity.set(IM.IM1SCHEME,TTLiteral.literal(scheme));
        if (matchedIri!=null) {
            entity.addObject(IM.MATCHED_TO, TTIriRef.iri(matchedIri));
            entity.setStatus(IM.DRAFT);
            Set<String> matches = entities.get(matchedIri);
            if (matches != null) {
                for (String iri : matches)
                    entity.addObject(IM.MATCHED_TO, TTIriRef.iri(iri));
            }
        }
        else {
            if (entity.getScheme().equals(IM.CODE_SCHEME_DISCOVERY))
                entity.setStatus(IM.DRAFT);
            else
                entity.setStatus(IM.UNASSIGNED);
        }
        if (used.containsKey(oldIri))
            entity.set(IM.USAGE_TOTAL,TTLiteral.literal(used.get(oldIri)));
        document.addEntity(entity);
        if (oldIri.equals("CM_CritCareSrcLctn04"))
            LOG.info("CM_CritCareSrcLctn04");
        oldIriEntity.put(oldIri,entity);
        return entity;
    }
    private void addIM1id(String iri,String oldIri){
        TTEntity im1= new TTEntity().setIri(iri);
        TTIriRef graph=TTIriRef.iri(iri.substring(0,iri.lastIndexOf("#")+1));
        im1.setGraph(graph);
        im1.addObject(IM.IM1ID,TTLiteral.literal(oldIri));

        Integer usedCount=0;
        if (used.containsKey(oldIri)){
            if (im1.get(IM.USAGE_TOTAL)!=null)
              usedCount= im1.get(IM.USAGE_TOTAL).asLiteral().intValue();
        }
        if (used.get(oldIri)!=null)
            usedCount= used.get(oldIri);
        if (IdToDbid.get(oldIri)!=null){
            Integer dbid= IdToDbid.get(oldIri);
            if (numericConcepts.contains(dbid))
              im1.set(IM.HAS_NUMERIC,TTLiteral.literal("true"));
            Integer dbused= usedDbid.get(dbid);
            if (dbused!=null)
                usedCount=usedCount+dbused;
        }
        if (usedCount>0)
            im1.set(IM.USAGE_TOTAL,TTLiteral.literal(usedCount));
        if (oldIri.equals("CM_CritCareSrcLctn04"))
          LOG.info("CM_CritCareSrcLctn04");
        oldIriEntity.put(oldIri,im1);
        document.addEntity(im1);
    }

    private void importUsage(String inFolder) throws IOException {

        for (String statsFile : usageDbid) {
            Path file = ImportUtils.findFilesForId(inFolder, statsFile).get(0);
            LOG.info("Retrieving counts from..."+ file.toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                reader.readLine();  // NOSONAR - Skipping header
                String line = reader.readLine();
                while (line != null && !line.isEmpty()) {
                    String[] fields = line.split("\t");
                    Integer dbid = Integer.parseInt(fields[1]);
                    String numeric= fields[3];
                    int count = Integer.parseInt(fields[4]);
                    usedDbid.putIfAbsent(dbid,0);
                    usedDbid.put(dbid,usedDbid.get(dbid)+count);
                    if (numeric.equals("Y"))
                        numericConcepts.add(dbid);
                    line = reader.readLine();
                }
            }
        }

    }


    private void importContext(String inFolder) throws IOException,DataFormatException {
        for (String statsFile : context) {
            Path file = ImportUtils.findFilesForId(inFolder, statsFile).get(0);
            LOG.info("Retrieving counts from..." + file.toString());
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                reader.readLine();  // NOSONAR - Skipping header
                String line = reader.readLine();
                while (line != null && !line.isEmpty()) {
                    String[] fields = line.split("\t");
                    String publisher= fields[1];
                    String system= fields[2];
                    String schema= fields[3];
                    String table= fields[4];
                    String field= fields[5];
                    String targetProperty= fields[6];
                    String sourceValue= fields[7];
                    if (sourceValue.equals("NULL"))
                        sourceValue=null;
                    String oldIri1= fields[8];
                    String headerCode= fields[9];
                    if (headerCode.equals("NULL"))
                        headerCode=null;
                    String regex= fields[10];
                    if (regex.equals("NULL"))
                        regex=null;
                    String oldIri2=fields[11];
                    String oldIri;
                    if (!oldIri2.equals("NULL")){
                        oldIri=oldIri2;
                        targetProperty="DM_concept";
                    }
                    else
                    {
                        oldIri= oldIri1;
                        if (headerCode!=null)
                            throw new IOException("value column should be null");

                    }
                    if (oldIri.equals("CM_CritCareSrcLctn04"))
                        LOG.info("CM_CritCareSrcLctn04");
                    TTIriRef newScheme = getScheme(publisher,system);
                    TTNode context= new TTNode();
                    TTEntity propertyEntity= oldIriEntity.get(targetProperty);
                    TTIriRef propertyIri;
                    if (propertyEntity==null) {
                        String lname= targetProperty.split("_")[1];
                        if (!entities.containsKey(IM.NAMESPACE+lname)) {
                            propertyEntity = addNewEntity(IM.NAMESPACE + lname,
                              null, getTerm(lname), null, "CM_DiscoveryCode", targetProperty, null, RDF.PROPERTY);
                            propertyIri= TTIriRef.iri(propertyEntity.getIri());
                            propertyEntity.addObject(RDFS.SUBCLASSOF, TTIriRef.iri(IM.NAMESPACE+"dataModelObjectProperty"));
                        }
                        else {
                            propertyIri= TTIriRef.iri(IM.NAMESPACE+ lname);
                        }
                    }
                    else
                        propertyIri= TTIriRef.iri(propertyEntity.getIri());

                    setContext(context,publisher,system,schema,table,field,sourceValue,regex,headerCode,propertyIri);



                    TTEntity entity= oldIriEntity.get(oldIri);
                    if (entity==null) {
                        entity= addOldEntity(newScheme,oldIri,publisher,system,sourceValue,headerCode,regex);
                    }
                    if (entity.getIri().equals("http://endhealth.info/bc#PF4"))
                        System.out.println("pf4");
                    TTIriRef scheme= entity.getScheme();
                    if (scheme!=null)
                        if (!scheme.equals(SNOMED.GRAPH_SNOMED)&&(!scheme.equals(IM.CODE_SCHEME_DISCOVERY))) {
                            entity.set(IM.SOURCE_CONTEXT, context);
                    }
               //     bindToSet(propertyIri.getIri(),entity.getIri());

                    line = reader.readLine();
                }
            }
        }
    }



    private void setContext(TTNode context, String publisher, String system, String schema, String table, String field, String sourceValue, String regex, String headerCode,TTIriRef propertyIri) {
        context.set(IM.SOURCE_PUBLISHER,TTLiteral.literal(publisher));
        context.set(IM.SOURCE_SYSTEM,TTLiteral.literal(system));
        context.set(IM.SOURCE_SCHEMA,TTLiteral.literal(schema));
        context.set(IM.SOURCE_TABLE,TTLiteral.literal(table));
        context.set(IM.SOURCE_FIELD,TTLiteral.literal(field));
        context.set(IM.TARGET_PROPERTY,propertyIri);
        if (sourceValue!=null)
            context.set(IM.SOURCE_VALUE,TTLiteral.literal(sourceValue));
        if (regex!=null)
            context.set(IM.SOURCE_REGEX,TTLiteral.literal(regex));
        if (headerCode!=null)
            context.set(IM.SOURCE_HEADING,TTLiteral.literal(headerCode));
    }


    private static String getPhrase(String iri) {
        iri = iri.substring(0, 1).toUpperCase() + iri.substring(1);
        StringBuilder term= new StringBuilder();
        term.append(iri.charAt(0));
        for (int i=1; i<iri.length();i++){
            if (Character.isUpperCase(iri.charAt(i))){
                term.append(" ");
            }
            term.append(iri.charAt(i));
        }
        return term.toString();
    }

    private String getTerm(String lname) {
        StringBuilder term= new StringBuilder(lname.substring(0, 1));
        for (int i=1; i<lname.length(); i++){
            if (Character.isUpperCase(lname.charAt(i)))
                term.append(" ").append(lname.charAt(i));
            else
                term.append(lname.charAt(i));

        }
        return term.toString();
    }

    private TTEntity addOldEntity(TTIriRef newScheme, String oldIri,String publisher,String system,String value,String headerCode,String regex) throws IOException {
        String term=null;
        if (headerCode!=null){
            term= oldIriTerm.get(headerCode);
            if (term!=null){
                if (regex!=null){
                   term= "Orginal term : "+ headerCode+ " parsed "+regex;
                }
            }
        }


        publisher= publisher.split("_")[2];
        system=system.split("_")[2];
        if (term==null){
            term="Original term : "+  value+ "("+ publisher +" "+ system+")";
        }
        if (oldIri.startsWith("CM_"))
            oldIri= oldIri.split("_")[1];
        if (value==null)
            value= oldIri;

        TTEntity entity= new TTEntity()
          .setGraph(newScheme)
          .setIri(newScheme.getIri()+oldIri)
          .setName(term)
          .setScheme(newScheme)
          .setCode(value)
          .set(IM.IM1ID,TTLiteral.literal(oldIri))
            .setStatus(IM.UNASSIGNED);
        document.addEntity(entity);
        oldIriEntity.put(oldIri,entity);
        if (value!=null) {
            String coreTerm = getPhrase(oldIri);
            TTIriRef core = importMaps.getReferenceFromCoreTerm(coreTerm);
            if (core != null)
                entity.addObject(IM.MATCHED_TO, TTIriRef.iri(core.getIri()));
        }

        return entity;
    }

    private TTIriRef getScheme(String publisher, String system) throws DataFormatException {
        if (publisher.equals("CM_Org_Barts"))
            if (system.equals("CM_Sys_Cerner"))
                return IM.CODE_SCHEME_BARTS_CERNER;
        if (publisher.equals("CM_Org_BHRUT"))
            if (system.equals("CM_Sys_Medway"))
                return (TTIriRef.iri(IM.DOMAIN+"bhrutm#"));
        if (publisher.equals("CM_Org_CWH"))
            if (system.equals("CM_Sys_Cerner"))
                return (TTIriRef.iri(IM.DOMAIN+"cwhcc#"));
        if (publisher.equals("CM_Org_Imperial"))
            if (system.equals("CM_Sys_Cerner"))
                return (TTIriRef.iri(IM.DOMAIN+"impc#"));
        if (publisher.equals("CM_Org_Kings"))
            if (system.equals("CM_Sys_PIMS"))
                return (TTIriRef.iri(IM.DOMAIN+"kingsp#"));
        if (publisher.equals("CM_Org_LNWH"))
            if (system.equals("CM_Sys_Silverlink"))
                return (TTIriRef.iri(IM.DOMAIN+"lnwhsl#"));
            else if (system.equals("CM_Sys_Symphony"))
                return TTIriRef.iri(IM.DOMAIN+"lnwhsy#");
        if (publisher.equals("CM_Org_THH"))
            if (system.equals("CM_Sys_Silverlink"))
                return (TTIriRef.iri(IM.DOMAIN+"thhsl#"));
        throw new DataFormatException("Unrecognised publisher and system : "+ publisher+" "+ system);

    }

    private void newSchemes(){
        TTEntity newScheme= addNewCoreEntity(IM.DOMAIN+"bhrutm#","BHRUT Medway code scheme and graph",
          null,null,IM.GRAPH);
        newScheme.addObject(RDFS.SUBCLASSOF,IM.GRAPH);
        newScheme= addNewCoreEntity(IM.DOMAIN+"cwhcc#","CWHC Cerner code scheme and graph",
          null,null,IM.GRAPH);
        newScheme.addObject(RDFS.SUBCLASSOF,IM.GRAPH);
        newScheme= addNewCoreEntity(IM.DOMAIN+"impc#","Imperial Cerner code scheme and graph",
          null,null,IM.GRAPH);
        newScheme.addObject(RDFS.SUBCLASSOF,IM.GRAPH);
        newScheme= addNewCoreEntity(IM.DOMAIN+"kingsp#","KCH PIMS code scheme and graph",
          null,null,IM.GRAPH);
        newScheme.addObject(RDFS.SUBCLASSOF,IM.GRAPH);
        newScheme= addNewCoreEntity(IM.DOMAIN+"lnwhsl#","LNWH Silverlink code scheme and graph",
          null,null,IM.GRAPH);
        newScheme.addObject(RDFS.SUBCLASSOF,IM.GRAPH);
        newScheme= addNewCoreEntity(IM.DOMAIN+"thhsl#","THH Silverlink code scheme and graph",
          null,null,IM.GRAPH);
        newScheme.addObject(RDFS.SUBCLASSOF,IM.GRAPH);

    }


    private void importOld(String inFolder) throws IOException {
        for (String oldFile : oldIris) {
            Path file = ImportUtils.findFilesForId(inFolder, oldFile).get(0);
            LOG.info("Retrieving old iri maps from..."+ file.toString());
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
                    if (oldIri.startsWith("CM"))
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
	public void validateFiles(String inFolder)  {
         ImportUtils.validateFiles(inFolder,im1Codes,oldIris,context,usageDbid);
	}

    @Override
    public void close() throws Exception {
        iriToCore.clear();
        used.clear();
        usedDbid.clear();
        numericConcepts.clear();
        oldIriTerm.clear();
        oldIriSnomed.clear();
        IdToDbid.clear();
        iriToSet.clear();
        importMaps.close();
    }
}
