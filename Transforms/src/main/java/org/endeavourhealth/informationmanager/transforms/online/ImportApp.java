package org.endeavourhealth.informationmanager.transforms.online;

import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImportByType;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.filer.rdf4j.LuceneIndexer;
import org.endeavourhealth.imapi.logic.reasoner.SetExpander;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.QR;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.imapi.vocabulary.im.GRAPH;
import org.endeavourhealth.informationmanager.transforms.sources.LoadDataTester;
import org.endeavourhealth.informationmanager.transforms.sources.Importer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Utility app for importing one or all of the various source files for the ontology initial population.
 */
public class ImportApp {
    private static final Logger LOG = LoggerFactory.getLogger(ImportApp.class);

    public static String testDirectory;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Insufficient parameters supplied:");
            System.err.println("<source> <import type> <privacy={0 public 1 private publication 2 private authoring}> [secure|skiptct|skipsearch]");
            System.exit(-1);
        }

        TTImportConfig cfg = new TTImportConfig();

        // Mandatory/ordered args
        cfg.setFolder(args[0]);
        cfg.setImportType(args[1].toLowerCase());

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
                    case "privacy":
                        TTFilerFactory.setPrivacyLevel(Integer.parseInt(args[i].split("=")[1]));
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
        TTImportByType importer = new Importer();
        switch (cfg.getImportType()) {
            case "all":
                    importer.validateByType(GRAPH.DISCOVERY, cfg.getFolder())
                  .validateByType(GRAPH.QUERY,cfg.getFolder())
                    .validateByType(SNOMED.GRAPH_SNOMED, cfg.getFolder())
                    .validateByType(GRAPH.ENCOUNTERS, cfg.getFolder())
                    .validateByType(GRAPH.EMIS, cfg.getFolder())
                    .validateByType(GRAPH.TPP, cfg.getFolder())
                    .validateByType(GRAPH.OPCS4, cfg.getFolder())
                    .validateByType(GRAPH.ICD10, cfg.getFolder())
                    .validateByType(GRAPH.VISION, cfg.getFolder())
                    .validateByType(GRAPH.KINGS_APEX, cfg.getFolder())
                    .validateByType(GRAPH.KINGS_WINPATH, cfg.getFolder())
                    .validateByType(GRAPH.BARTS_CERNER, cfg.getFolder())
                    .validateByType(GRAPH.ODS, cfg.getFolder())
                  .validateByType(GRAPH.NHS_TFC, cfg.getFolder())
                 .validateByType(GRAPH.CEG_QUERY, cfg.getFolder())
                  .validateByType(GRAPH.IM1, cfg.getFolder())
//                    .validateByType(GRAPH.CONFIG, cfg.getFolder())
                      .validateByType(GRAPH.DELTAS,cfg.getFolder());
                importer.importByType(GRAPH.DISCOVERY, cfg);
                importer.importByType(GRAPH.QUERY,cfg);
                importer.importByType(SNOMED.GRAPH_SNOMED, cfg);
                importer.importByType(GRAPH.ENCOUNTERS, cfg);
                importer.importByType(GRAPH.EMIS, cfg);
                importer.importByType(GRAPH.TPP, cfg);
                importer.importByType(GRAPH.OPCS4, cfg);
                importer.importByType(GRAPH.ICD10, cfg);
                importer.importByType(GRAPH.VISION, cfg);
                importer.importByType(GRAPH.KINGS_APEX, cfg);
                importer.importByType(GRAPH.KINGS_WINPATH, cfg);
                importer.importByType(GRAPH.BARTS_CERNER, cfg);
                importer.importByType(GRAPH.ODS, cfg);
                importer.importByType(GRAPH.NHS_TFC,cfg);
                importer.importByType(GRAPH.CEG_QUERY,cfg);
//                importer.importByType(GRAPH.CONFIG,cfg);
                importer.importByType(GRAPH.IM1, cfg);
                importer.importByType(GRAPH.DELTAS,cfg);
                break;
            case "corequery":
                importer = new Importer().validateByType(GRAPH.QUERY, cfg.getFolder());
                importer.importByType(GRAPH.QUERY,cfg);
                break;
            case "imv1":
                importer = new Importer().validateByType(GRAPH.IM1, cfg.getFolder());
                importer.importByType(GRAPH.IM1, cfg);
                break;
            case "prsb":
                importer = new Importer().validateByType(GRAPH.PRSB, cfg.getFolder());
                importer.importByType(GRAPH.PRSB, cfg);
                break;

            case "core":
                importer = new Importer().validateByType(GRAPH.DISCOVERY, cfg.getFolder());
                importer.importByType(GRAPH.DISCOVERY, cfg);
                break;
            case "snomed":
                importer = new Importer().validateByType(SNOMED.GRAPH_SNOMED, cfg.getFolder());
                importer.importByType(SNOMED.GRAPH_SNOMED, cfg);
                break;
            case "emis":
                importer = new Importer().validateByType(GRAPH.EMIS, cfg.getFolder());
                importer.importByType(GRAPH.EMIS, cfg);
                break;
            case "cprd":
                importer = new Importer().validateByType(GRAPH.CPRD_MED, cfg.getFolder());
                importer.importByType(GRAPH.CPRD_MED, cfg);
                break;
            case "tpp":
            case "ctv3":
                importer = new Importer().validateByType(GRAPH.TPP, cfg.getFolder());
                importer.importByType(GRAPH.TPP, cfg);
                break;
            case "opcs4":
                importer = new Importer().validateByType(GRAPH.OPCS4, cfg.getFolder());
                importer.importByType(GRAPH.OPCS4, cfg);
                break;
            case "icd10":
                importer = new Importer().validateByType(GRAPH.ICD10, cfg.getFolder());
                importer.importByType(GRAPH.ICD10, cfg);
                break;
            case "discoverymaps":
            case "encounters":
                importer = new Importer().validateByType(GRAPH.ENCOUNTERS, cfg.getFolder());
                importer.importByType(GRAPH.ENCOUNTERS, cfg);
                break;
            case "read2":
            case "vision":
                importer = new Importer().validateByType(GRAPH.VISION, cfg.getFolder());
                importer.importByType(GRAPH.VISION, cfg);
                break;
            case "kingsapex":
                importer = new Importer().validateByType(GRAPH.KINGS_APEX, cfg.getFolder());
                importer.importByType(GRAPH.KINGS_APEX, cfg);
                break;
            case "kingswinpath":
                importer = new Importer().validateByType(GRAPH.KINGS_WINPATH, cfg.getFolder());
                importer.importByType(GRAPH.KINGS_WINPATH, cfg);
                break;

            case "ceg" :
                importer = new Importer().validateByType(GRAPH.CEG_QUERY, cfg.getFolder());
                importer.importByType(GRAPH.CEG_QUERY, cfg);
                break;
            case "barts":
                importer = new Importer().validateByType(GRAPH.BARTS_CERNER, cfg.getFolder());
                importer.importByType(GRAPH.BARTS_CERNER, cfg);
                break;
            case "ods":
                importer = new Importer().validateByType(GRAPH.ODS, cfg.getFolder());
                importer.importByType(GRAPH.ODS, cfg);
                break;
            case "nhstfc":
                importer = new Importer().validateByType(GRAPH.NHS_TFC, cfg.getFolder());
                importer.importByType(GRAPH.NHS_TFC, cfg);
                break;
            case "tct":
                cfg.setSkipsearch(true);
                break;
            case "search":
                cfg.setSkiptct(true);
                break;
            case "config":
                importer = new Importer().validateByType(GRAPH.CONFIG, cfg.getFolder());
                importer.importByType(GRAPH.CONFIG, cfg);
            case "deltas":
                importer = new Importer().validateByType(GRAPH.DELTAS, cfg.getFolder());
                importer.importByType(GRAPH.DELTAS, cfg);
                break;
            case "singlefile" :
                importer= new Importer().validateByType(TTIriRef.iri(IM.NAMESPACE.iri+"SingleFileImporter"),cfg.getFolder());
                importer.importByType(TTIriRef.iri(IM.NAMESPACE.iri+"SingleFileImporter"), cfg);
                break;
            case "qcodegroups" :
                importer= new Importer().validateByType(QR.NAMESPACE,cfg.getFolder());
                importer.importByType(QR.NAMESPACE, cfg);
                break;
            default:
                throw new Exception("Unknown import type");

        }

        if (cfg.getImportType().equals("all")||cfg.getImportType().equals("core")) {
                LOG.info("expanding value sets");
                new SetExpander().expandAllSets();
        }

        if (!cfg.isSkiplucene())
            new LuceneIndexer().buildIndexes();

        LOG.info("Finished - ", new Date());
    }
}

