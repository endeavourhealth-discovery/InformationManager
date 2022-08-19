package org.endeavourhealth.informationmanager.transforms.online;

import org.endeavourhealth.imapi.filer.TCGenerator;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImportByType;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.filer.rdf4j.LuceneIndexer;
import org.endeavourhealth.imapi.logic.reasoner.SetExpander;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.transforms.sources.LoadDataTester;
import org.endeavourhealth.informationmanager.transforms.sources.Importer;
import org.endeavourhealth.informationmanager.transforms.sources.SingleFileImporter;

import java.util.Date;

/**
 * Utility app for importing one or all of the various source files for the ontology initial population.
 */
public class ImportApp {
    public static String testDirectory;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
                System.err.println("Insufficient parameters supplied:");
                System.err.println("<folder> <import type> <privacy={0 public 1 private publication 2 private authoring}> [secure|skiptct|skipsearch]");
                System.exit(-1);
        }


        TTImportConfig cfg = new TTImportConfig();

        // Mandatory/ordered args
        cfg.setFolder(args[0]);
        cfg.setImportType(args[1].toLowerCase());



        TTFilerFactory.setSkipDeletes("all".equals(cfg.getImportType()));

        // Optional switch args
        if (args.length >= 3) {
            for (int i = 2; i < args.length; i++) {
                switch (args[i].toLowerCase().split("=")[0]) {
                    case "secure":
                        cfg.setSecure(true);
                        break;
                    case "skiptct":
                        cfg.setSkiptct(true);
                        break;
                    case "skipsearch":
                        cfg.setSkipsearch(true);
                        break;
                    case "skiplucene":
                        cfg.setSkiplucene(true);
                        break;
                    case "skipdeletes":
                        TTFilerFactory.setSkipDeletes(true);
                        break;
                    case "privacy":
                        TTFilerFactory.setPrivacyLevel(Integer.parseInt(args[i].split("=")[1]));
                        break;
                    case "file" :
                        cfg.setFolder(args[i].split("=")[1]);
                        break;
                    default:
                        if (args[i].contains("test="))
                            testDirectory= args[i].substring(args[i].lastIndexOf("=")+1);
                        else
                         System.err.println("Unknown parameter " + args[i]);
                }
            }
        }


         LoadDataTester.testLoadData(cfg.getFolder(), cfg.isSecure());
        importData(cfg);
    }

    private static void importData(TTImportConfig cfg) throws Exception {

        switch (cfg.getImportType()) {
            case "all":
                TTImportByType importer = new Importer()
                    .validateByType(IM.GRAPH_DISCOVERY, cfg.getFolder())
                    .validateByType(SNOMED.GRAPH_SNOMED, cfg.getFolder())
                    .validateByType(IM.GRAPH_ENCOUNTERS, cfg.getFolder())
                    .validateByType(IM.GRAPH_EMIS, cfg.getFolder())
                    .validateByType(IM.GRAPH_TPP, cfg.getFolder())
                    .validateByType(IM.GRAPH_OPCS4, cfg.getFolder())
                    .validateByType(IM.GRAPH_ICD10, cfg.getFolder())
                    .validateByType(IM.GRAPH_VISION, cfg.getFolder())
                    .validateByType(IM.GRAPH_KINGS_APEX, cfg.getFolder())
                    .validateByType(IM.GRAPH_KINGS_WINPATH, cfg.getFolder())
                    .validateByType(IM.GRAPH_BARTS_CERNER, cfg.getFolder())
                    .validateByType(IM.GRAPH_ODS, cfg.getFolder())
                  .validateByType(IM.GRAPH_NHS_TFC, cfg.getFolder())
                 .validateByType(IM.GRAPH_CEG_QUERY, cfg.getFolder())
                  .validateByType(IM.GRAPH_IM1, cfg.getFolder())
                    .validateByType(IM.GRAPH_CONFIG, cfg.getFolder())
                      .validateByType(IM.GRAPH_DELTAS,cfg.getFolder());
                importer.importByType(IM.GRAPH_DISCOVERY, cfg);
                importer.importByType(SNOMED.GRAPH_SNOMED, cfg);
                importer.importByType(IM.GRAPH_ENCOUNTERS, cfg);
                importer.importByType(IM.GRAPH_EMIS, cfg);
                importer.importByType(IM.GRAPH_TPP, cfg);
                importer.importByType(IM.GRAPH_OPCS4, cfg);
                importer.importByType(IM.GRAPH_ICD10, cfg);
                importer.importByType(IM.GRAPH_VISION, cfg);
                importer.importByType(IM.GRAPH_KINGS_APEX, cfg);
                importer.importByType(IM.GRAPH_KINGS_WINPATH, cfg);
                importer.importByType(IM.GRAPH_BARTS_CERNER, cfg);
                importer.importByType(IM.GRAPH_ODS, cfg);
                importer.importByType(IM.GRAPH_NHS_TFC,cfg);
                importer.importByType(IM.GRAPH_CEG_QUERY,cfg);
                importer.importByType(IM.GRAPH_CONFIG,cfg);
                importer.importByType(IM.GRAPH_IM1, cfg);
                importer.importByType(IM.GRAPH_DELTAS,cfg);
                break;
            case "imv1":
                importer = new Importer().validateByType(IM.GRAPH_IM1, cfg.getFolder());
                importer.importByType(IM.GRAPH_IM1, cfg);
                break;
            case "prsb":
                importer = new Importer().validateByType(IM.GRAPH_PRSB, cfg.getFolder());
                importer.importByType(IM.GRAPH_PRSB, cfg);
                break;

            case "core":
                importer = new Importer().validateByType(IM.GRAPH_DISCOVERY, cfg.getFolder());
                importer.importByType(IM.GRAPH_DISCOVERY, cfg);
                break;
            case "snomed":
                importer = new Importer().validateByType(SNOMED.GRAPH_SNOMED, cfg.getFolder());
                importer.importByType(SNOMED.GRAPH_SNOMED, cfg);
                break;
            case "emis":
                importer = new Importer().validateByType(IM.GRAPH_EMIS, cfg.getFolder());
                importer.importByType(IM.GRAPH_EMIS, cfg);
                break;
            case "tpp":
            case "ctv3":
                importer = new Importer().validateByType(IM.GRAPH_TPP, cfg.getFolder());
                importer.importByType(IM.GRAPH_TPP, cfg);
                break;
            case "opcs4":
                importer = new Importer().validateByType(IM.GRAPH_OPCS4, cfg.getFolder());
                importer.importByType(IM.GRAPH_OPCS4, cfg);
                break;
            case "icd10":
                importer = new Importer().validateByType(IM.GRAPH_ICD10, cfg.getFolder());
                importer.importByType(IM.GRAPH_ICD10, cfg);
                break;
            case "discoverymaps":
            case "encounters":
                importer = new Importer().validateByType(IM.GRAPH_ENCOUNTERS, cfg.getFolder());
                importer.importByType(IM.GRAPH_ENCOUNTERS, cfg);
                break;
            case "read2":
            case "vision":
                importer = new Importer().validateByType(IM.GRAPH_VISION, cfg.getFolder());
                importer.importByType(IM.GRAPH_VISION, cfg);
                break;
            case "kingsapex":
                importer = new Importer().validateByType(IM.GRAPH_KINGS_APEX, cfg.getFolder());
                importer.importByType(IM.GRAPH_KINGS_APEX, cfg);
                break;
            case "kingswinpath":
                importer = new Importer().validateByType(IM.GRAPH_KINGS_WINPATH, cfg.getFolder());
                importer.importByType(IM.GRAPH_KINGS_WINPATH, cfg);
                break;

            case "ceg" :
                importer = new Importer().validateByType(IM.GRAPH_CEG_QUERY, cfg.getFolder());
                importer.importByType(IM.GRAPH_CEG_QUERY, cfg);
                break;
            case "barts":
                importer = new Importer().validateByType(IM.GRAPH_BARTS_CERNER, cfg.getFolder());
                importer.importByType(IM.GRAPH_BARTS_CERNER, cfg);
                break;
            case "ods":
                importer = new Importer().validateByType(IM.GRAPH_ODS, cfg.getFolder());
                importer.importByType(IM.GRAPH_ODS, cfg);
                break;
            case "nhstfc":
                importer = new Importer().validateByType(IM.GRAPH_NHS_TFC, cfg.getFolder());
                importer.importByType(IM.GRAPH_NHS_TFC, cfg);
                break;
            case "tct":
                cfg.setSkipsearch(true);
                break;
            case "search":
                cfg.setSkiptct(true);
                break;
            case "config":
                importer = new Importer().validateByType(IM.GRAPH_CONFIG, cfg.getFolder());
                importer.importByType(IM.GRAPH_CONFIG, cfg);
            case "deltas":
                importer = new Importer().validateByType(IM.GRAPH_DELTAS, cfg.getFolder());
                importer.importByType(IM.GRAPH_DELTAS, cfg);
                break;
            case "singlefile" :
                importer= new Importer().validateByType(TTIriRef.iri(IM.NAMESPACE+"SingleFileImporter"),cfg.getFolder());
                importer.importByType(TTIriRef.iri(IM.NAMESPACE+"SingleFileImporter"), cfg);
                break;
            default:
                throw new Exception("Unknown import type");

        }

        if (!cfg.isSkiptct()) {
            TCGenerator closureGenerator = TTFilerFactory.getClosureGenerator();
            closureGenerator.generateClosure(cfg.getFolder(), cfg.isSecure());
        }

        System.out.println("expanding value sets");
        new SetExpander().expandAllSets();
        if (!cfg.isSkiplucene()){
            System.out.println("building lucene index");
            new LuceneIndexer().buildIndexes();
        }

        System.out.println("Finished - " + (new Date()));
    }
}

