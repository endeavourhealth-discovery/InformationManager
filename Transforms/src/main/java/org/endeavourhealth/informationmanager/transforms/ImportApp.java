package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.ClosureGenerator;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTImportByType;
import org.endeavourhealth.informationmanager.TTImportConfig;

import java.util.Date;

/**
 * Utility app for importing one or all of the various source files for the ontology initial population.
 */
public class ImportApp {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Insufficient parameters supplied:");
            System.err.println("<folder> <import type> [secure|skiptct|skipsearch]");
            System.exit(-1);
        }

        TTImportConfig cfg = new TTImportConfig();

        // Mandatory/ordered args
        cfg.folder = args[0];
        cfg.importType = args[1].toLowerCase();

        // Optional switch args
        if (args.length >= 3) {
            for (int i = 2; i < args.length; i++) {
                switch (args[i].toLowerCase()) {
                    case "secure":
                        cfg.secure = true;
                        break;
                    case "skiptct":
                        cfg.skiptct = true;
                        break;
                    case "skipsearch":
                        cfg.skipsearch = true;
                        break;
                    default:
                        System.err.println("Unknown parameter " + args[i]);
                }
            }
        }

        LoadDataTester.testLoadData(cfg.folder, cfg.secure);

        importData(cfg);
    }

    private static void importData(TTImportConfig cfg) throws Exception {
        switch (cfg.importType) {
            case "all":
                TTImportByType importer = new Importer()
                        .validateByType(IM.GRAPH_DISCOVERY, cfg.folder)
                        .validateByType(IM.GRAPH_SNOMED, cfg.folder)
                        .validateByType(IM.GRAPH_EMIS, cfg.folder)
                        .validateByType(IM.GRAPH_TPP, cfg.folder)
                        .validateByType(IM.GRAPH_OPCS4, cfg.folder)
                        .validateByType(IM.GRAPH_ICD10, cfg.folder)
                        .validateByType(IM.GRAPH_VISION, cfg.folder)
                        .validateByType(IM.GRAPH_KINGS_APEX, cfg.folder)
                        .validateByType(IM.GRAPH_KINGS_WINPATH, cfg.folder)
                        .validateByType(IM.MAP_DISCOVERY, cfg.folder)
                        .validateByType(IM.GRAPH_REPORTS, cfg.folder)
                        .validateByType(IM.GRAPH_CEG16, cfg.folder)
                        .validateByType(IM.GRAPH_BARTS_CERNER, cfg.folder)
                        .validateByType(IM.GRAPH_IM1, cfg.folder)
                        .validateByType(IM.GRAPH_ODS, cfg.folder);
                importer.importByType(IM.GRAPH_DISCOVERY, cfg);
                importer.importByType(IM.GRAPH_SNOMED, cfg);
                importer.importByType(IM.GRAPH_EMIS, cfg);
                importer.importByType(IM.GRAPH_TPP, cfg);
                importer.importByType(IM.GRAPH_OPCS4, cfg);
                importer.importByType(IM.GRAPH_ICD10, cfg);
                importer.importByType(IM.MAP_DISCOVERY, cfg);
                importer.importByType(IM.GRAPH_VISION, cfg);
                importer.importByType(IM.GRAPH_KINGS_APEX, cfg);
                importer.importByType(IM.GRAPH_KINGS_WINPATH, cfg);
                importer.importByType(IM.GRAPH_REPORTS, cfg);
                importer.importByType(IM.GRAPH_CEG16, cfg);
                importer.importByType(IM.GRAPH_BARTS_CERNER, cfg);
                importer.importByType(IM.GRAPH_IM1, cfg);
                importer.importByType(IM.GRAPH_ODS, cfg);
                break;
            case "imv1":
                importer = new Importer().validateByType(IM.GRAPH_IM1, cfg.folder);
                importer.importByType(IM.GRAPH_IM1, cfg);
                break;
            case "prsb":
                importer = new Importer().validateByType(IM.GRAPH_PRSB, cfg.folder);
                importer.importByType(IM.GRAPH_PRSB, cfg);
                break;

            case "core":
                importer = new Importer().validateByType(IM.GRAPH_DISCOVERY, cfg.folder);
                importer.importByType(IM.GRAPH_DISCOVERY, cfg);
                break;
            case "snomed":
                importer = new Importer().validateByType(IM.GRAPH_SNOMED, cfg.folder);
                importer.importByType(IM.GRAPH_SNOMED, cfg);
                break;
            case "emis":
                importer = new Importer().validateByType(IM.GRAPH_EMIS, cfg.folder);
                importer.importByType(IM.GRAPH_EMIS, cfg);
                break;
            case "tpp":
            case "ctv3":
                importer = new Importer().validateByType(IM.GRAPH_TPP, cfg.folder);
                importer.importByType(IM.GRAPH_TPP, cfg);
                break;
            case "opcs4":
                importer = new Importer().validateByType(IM.GRAPH_OPCS4, cfg.folder);
                importer.importByType(IM.GRAPH_OPCS4, cfg);
                break;
            case "icd10":
                importer = new Importer().validateByType(IM.GRAPH_ICD10, cfg.folder);
                importer.importByType(IM.GRAPH_ICD10, cfg);
                break;
            case "discoverymaps":
                importer = new Importer().validateByType(IM.MAP_DISCOVERY, cfg.folder);
                importer.importByType(IM.MAP_DISCOVERY, cfg);
                break;
            case "read2":
            case "vision":
                importer = new Importer().validateByType(IM.GRAPH_VISION, cfg.folder);
                importer.importByType(IM.GRAPH_VISION, cfg);
                break;
            case "kingsapex":
                importer = new Importer().validateByType(IM.GRAPH_KINGS_APEX, cfg.folder);
                importer.importByType(IM.GRAPH_KINGS_APEX, cfg);
                break;
            case "kingswinpath":
                importer = new Importer().validateByType(IM.GRAPH_KINGS_WINPATH, cfg.folder);
                importer.importByType(IM.GRAPH_KINGS_WINPATH, cfg);
                break;
            case "reports":
                importer = new Importer().validateByType(IM.GRAPH_REPORTS, cfg.folder);
                importer.importByType(IM.GRAPH_REPORTS, cfg);
                break;
            case "cegethnicity":
                importer = new Importer().validateByType(IM.GRAPH_CEG16, cfg.folder);
                importer.importByType(IM.GRAPH_CEG16, cfg);
                break;
            case "barts":
                importer = new Importer().validateByType(IM.GRAPH_BARTS_CERNER, cfg.folder);
                importer.importByType(IM.GRAPH_BARTS_CERNER, cfg);
                break;
            case "ods":
                importer = new Importer().validateByType(IM.GRAPH_ODS, cfg.folder);
                importer.importByType(IM.GRAPH_ODS, cfg);
                break;
            case "tct":
                cfg.skipsearch = true;
                break;
            case "search":
                cfg.skiptct = true;
                break;
            default:
                throw new Exception("Unknown import type");

        }

        if (!cfg.skiptct) ClosureGenerator.generateClosure(cfg.folder, cfg.secure);
        if (!cfg.skipsearch) SearchTermGenerator.generateSearchTerms(cfg.folder, cfg.secure);

        System.out.println("Finished - " + (new Date()));
    }
}

