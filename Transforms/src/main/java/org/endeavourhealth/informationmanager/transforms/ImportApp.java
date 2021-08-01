package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.BulkFilerJDBC;
import org.endeavourhealth.informationmanager.ClosureGenerator;
import org.endeavourhealth.informationmanager.TTImportByType;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility app for importing one or all of the various source files for the ontology initial population.
 */
public class ImportApp {
    public static void main(String[] args) throws Exception {
        if (args.length <2) {
            System.err.println("You need to provide a root path containing data and an import type");
            System.exit(-1);
        }
        String folder = args[0];
        boolean bulk=false;
        Map<String,Integer> entityMap=null;
        if (args.length==3)
            if (args[2].equalsIgnoreCase("bulk")) {
                bulk = true;
                entityMap = new HashMap<>();
            }
        String importType = args[1].toLowerCase();
        if (bulk)
            BulkFilerJDBC.dropIndexes();

        switch (importType) {
            case "all":
                TTImportByType importer = new Importer().validateByType(IM.GRAPH_DISCOVERY, folder);
                importer.validateByType(IM.GRAPH_SNOMED, folder)
                    .validateByType(IM.GRAPH_EMIS, folder)
                    .validateByType(IM.GRAPH_TPP, folder)
                    .validateByType(IM.GRAPH_OPCS4, folder)
                    .validateByType(IM.GRAPH_ICD10, folder)
                  .validateByType(IM.GRAPH_VISION,folder)
                  .validateByType(IM.GRAPH_KINGS_APEX,folder)
                  .validateByType(IM.GRAPH_KINGS_WINPATH,folder)
                .validateByType(IM.MAP_DISCOVERY,folder)
                .validateByType(IM.GRAPH_REPORTS,folder)
                .validateByType(IM.GRAPH_CEG16,folder);
                importer.importByType(IM.GRAPH_DISCOVERY,folder,bulk,entityMap);
                importer.importByType(IM.GRAPH_SNOMED,folder,bulk,entityMap);
                importer.importByType(IM.GRAPH_EMIS,folder,bulk,entityMap);
                importer.importByType(IM.GRAPH_TPP,folder,bulk,entityMap);
                importer.importByType(IM.GRAPH_OPCS4,folder,bulk,entityMap);
                importer.importByType(IM.GRAPH_ICD10,folder,bulk,entityMap);
                importer.importByType(IM.MAP_DISCOVERY,folder,bulk,entityMap);
                importer.importByType(IM.GRAPH_VISION,folder,bulk,entityMap);
                importer.importByType(IM.GRAPH_KINGS_APEX,folder,bulk,entityMap);
                importer.importByType(IM.GRAPH_KINGS_WINPATH,folder,bulk,entityMap);
                importer.importByType(IM.GRAPH_REPORTS, folder,bulk,entityMap);
                importer.importByType(IM.GRAPH_CEG16,folder,bulk,entityMap);
                break;
            case "prsb":
                importer = new Importer().validateByType(IM.GRAPH_PRSB,folder);
                importer.importByType(IM.GRAPH_PRSB,folder,bulk,entityMap);
                break;

            case "core":
                importer = new Importer().validateByType(IM.GRAPH_DISCOVERY, folder);
                importer.importByType(IM.GRAPH_DISCOVERY, folder,bulk,entityMap);
                break;
            case "snomed":
                importer = new Importer().validateByType(IM.GRAPH_SNOMED, folder);
                importer.importByType(IM.GRAPH_SNOMED, folder,bulk,entityMap);
                break;
            case "emis":
                importer = new Importer().validateByType(IM.GRAPH_EMIS, folder);
                importer.importByType(IM.GRAPH_EMIS, folder,bulk,entityMap);
                break;
            case "tpp":
            case "ctv3":
                importer = new Importer().validateByType(IM.GRAPH_TPP, folder);
                importer.importByType(IM.GRAPH_TPP, folder,bulk,entityMap);
                break;
            case "opcs4":
                importer = new Importer().validateByType(IM.GRAPH_OPCS4, folder);
                importer.importByType(IM.GRAPH_OPCS4, folder,bulk,entityMap);
                break;
            case "icd10":
                importer = new Importer().validateByType(IM.GRAPH_ICD10, folder);
                importer.importByType(IM.GRAPH_ICD10, folder,bulk,entityMap);
                break;
            case "discoverymaps":
                importer = new Importer().validateByType(IM.MAP_DISCOVERY, folder);
                importer.importByType(IM.MAP_DISCOVERY, folder,bulk,entityMap);
                break;
            case "read2" :
            case "vision":
                importer = new Importer().validateByType(IM.GRAPH_VISION, folder);
                importer.importByType(IM.GRAPH_VISION, folder,bulk,entityMap);
                break;
            case "kingsapex":
                importer = new Importer().validateByType(IM.GRAPH_KINGS_APEX,folder);
                importer.importByType(IM.GRAPH_KINGS_APEX,folder,bulk,entityMap);
                break;
            case "kingswinpath":
                importer = new Importer().validateByType(IM.GRAPH_KINGS_WINPATH,folder);
                importer.importByType(IM.GRAPH_KINGS_WINPATH,folder,bulk,entityMap);
                break;
            case "reports":
                importer = new Importer().validateByType(IM.GRAPH_REPORTS, folder);
                importer.importByType(IM.GRAPH_REPORTS, folder,bulk,entityMap);
                break;
            case "cegethnicity":
                importer= new Importer().validateByType(IM.GRAPH_CEG16,folder);
                importer.importByType(IM.GRAPH_CEG16,folder,bulk,entityMap);
                break;
            default:
                throw new Exception("Unknown import type");

        }
        if (bulk)
            BulkFilerJDBC.createIndexes();

        ClosureGenerator.generateClosure(args[0]);
        SearchTermGenerator.generateSearchTerms(args[0]);

        System.out.println("Finished - " + (new Date().toString()));
    }
}

