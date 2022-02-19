package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.dataaccess.EntityRepository2;
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class IM1MapImport implements TTImport {
    private static final Logger LOG = LoggerFactory.getLogger(IM1MapImport.class);
    private static final String[] im1Codes = {".*\\\\IMv1\\\\concepts.txt"};
    private static final String[] conceptCounts = {".*\\\\DiscoveryLive\\\\concept_counts\\.txt"};
    private Map<String,TTEntity> dbidMap= new HashMap<>();
    private TTDocument document;
    private Set<String> entities;
    private final Map<String,TTEntity> iriToCore= new HashMap<>();
    private final EntityRepository2 repo= new EntityRepository2();


	@Override
	public TTImport importData(TTImportConfig config) throws Exception {
        return importData(config.getFolder(), config.isSecure(), null);
    }

    public TTImport importData(String inFolder, boolean secure, Integer lastDbid) throws Exception {
      System.out.println("Retrieving all entities...");
      entities= ImportUtils.importEntities();
        TTManager manager = new TTManager();
        document = manager.createDocument(IM.GRAPH_IM1.getIri());
        TTDocument statsDocument= manager.createDocument(IM.GRAPH_STATS.getIri());
        statsDocument.setCrud(IM.UPDATE);
        importv1Codes(inFolder);
        System.out.println("Importing Concept counts");
        importStats(inFolder,statsDocument);
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
        Path file = ImportUtils.findFileForId(inFolder, im1Codes[0]);
        int count = 0;
        try (BufferedReader reader= new BufferedReader(new FileReader(file.toFile()))) {
            FileWriter writer= new FileWriter(file.toFile().getParentFile().toString()+"\\UnmappedConcepts.txt");
            reader.readLine();
            String line = reader.readLine();
            while (line != null && !line.isEmpty()) {
                String[] fields = line.split("\t");
                while (fields.length<6) {
                    line = line + reader.readLine();
                    fields = line.split("\t");
                }
                String dbid = fields[0];
                String oldConcept= fields[1];
                String term=fields[2];
                String im1Scheme= fields[4];
                String code= fields[5];
                String scheme=null;
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
                            System.err.println(im1Scheme + " :" + code + " : " + dbid);
                            scheme = "X";
                    }
                    if (scheme == null)
                        throw new IOException();
                    if (!scheme.equals("X")) {
                        String lname = code;
                        if (scheme.equals(IM.CODE_SCHEME_TPP.getIri()))
                            lname = lname.replace(".", "_");
                        if (scheme.equals(IM.CODE_SCHEME_EMIS.getIri())) {
                            lname=lname.replaceAll("[&/' |()^]","_");
                            lname=lname.replace("[","_").replace("]","_");
                            if (im1Scheme.equals("READ2"))
                                lname=lname.replace(".","");
                            else
                                lname=lname.replace(".","_");
                        }
                        if (scheme.equals(IM.CODE_SCHEME_ENCOUNTERS.getIri()))
                            lname = "LE_" + lname;
                        if (scheme.equals(IM.CODE_SCHEME_ICD10.getIri()))
                            lname = lname.replace(".", "_");
                        if (scheme.equals(IM.CODE_SCHEME_OPCS4.getIri()))
                            lname = lname.replace(".", "_");

                        if (scheme.equals(IM.CODE_SCHEME_DISCOVERY.getIri())){
                            if (lname.contains("_"))
                                lname=lname.substring(lname.indexOf("_")+1);
                            if (!entities.contains(scheme+lname)) {
                                List<String> schemes = Arrays.asList(IM.NAMESPACE);
                                TTIriRef core = repo.getReferenceFromCoreTerm(term, schemes);
                                if (core != null)
                                    lname = core.getIri().split("#")[1];
                            }
                        }
                        TTEntity im1 = new TTEntity()
                          .setIri(scheme + lname);

                        if (!entities.contains(im1.getIri())) {
                            writer.write(line+"\n");
                        }
                        else {
                            TTNode im1Map = new TTNode();
                            im1.addObject(IM.IM1MAP, im1Map);
                            im1Map.set(IM.IM1SCHEME, TTLiteral.literal(im1Scheme));
                            im1Map.set(IM.IM1CODE, TTLiteral.literal(code));
                            im1Map.set(IM.DBID, TTLiteral.literal(dbid));
                            dbidMap.put(dbid, im1);
                            document.addEntity(im1);
                        }
                    }
                }
                count++;
                if (count %50000== 0){
                    LOG.info("Imported {} im1 concepts",count);
                }
                line= reader.readLine();
            }
        }
        System.out.println("Process ended with " + count);

	}


    private void importStats(String inFolder, TTDocument statsDocument) throws IOException, SQLException {
        Path file = ImportUtils.findFileForId(inFolder, conceptCounts[0]);
        System.out.println("Retrieving legacy to core maps");
        Map<String,Set<String>> legacyMap= repo.getAllMatchedLegacy();
        int i = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            reader.readLine();  // NOSONAR - Skipping header
            String line = reader.readLine();
            while (line != null && !line.isEmpty()) {
                String[] fields = line.split("\t");
                String dbid = fields[0];
                Integer count = Integer.parseInt(fields[fields.length-1]);
                TTEntity im1= dbidMap.get(dbid);
                if (im1!=null) {
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
                            TTEntity coreEntity= iriToCore.get(core);
                            if (coreEntity==null) {
                                coreEntity = new TTEntity().setIri(core);
                                iriToCore.put(core, coreEntity);
                                statsDocument.addEntity(coreEntity);
                            }
                            if (coreEntity.get(IM.WEIGHTING)!=null)
                                 coreEntity.set(IM.WEIGHTING,TTLiteral.literal(coreEntity.get(IM.WEIGHTING).asLiteral().intValue()+count));
                            else
                                coreEntity.set(IM.WEIGHTING, TTLiteral.literal(count));

                        }
                    }
                }

                i++;
                line= reader.readLine();
            }
        }
        System.out.println("Process ended with " + i);


    }



	@Override
	public TTImport validateFiles(String inFolder)  {
        ImportUtils.validateFiles(".*\\\\IMv1\\\\IMv1DbidSchemeCode.txt");
        ImportUtils.validateFiles(inFolder,conceptCounts,im1Codes);
        return this;
	}


}
