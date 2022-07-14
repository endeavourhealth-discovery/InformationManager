package org.endeavourhealth.informationmanager.transforms.preload;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.filer.rdf4j.LuceneIndexer;
import org.endeavourhealth.imapi.filer.rdf4j.TTBulkFiler;
import org.endeavourhealth.imapi.logic.reasoner.SetExpander;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;
import org.endeavourhealth.informationmanager.transforms.sources.CoreImporter;
import org.endeavourhealth.informationmanager.transforms.sources.DeltaImporter;
import org.endeavourhealth.informationmanager.transforms.sources.ImportUtils;
import org.endeavourhealth.informationmanager.transforms.sources.Importer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

public class Preload {


	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
			System.err.println("Insufficient parameters supplied:");
			System.err.println("source={sourcefolder} preload={foldercontaing preload} "+
				"temp= {folder for temporary data} and privacy= {0 public, 1 private publisher 2 private for authoring} cmd={graphdbExecutable}");
			System.exit(-1);
		}
		TTFilerFactory.setBulk(true);
		String graphdbCommand=null;

		TTImportConfig cfg = new TTImportConfig();

			for (int i = 0; i < args.length; i++) {
				String arg=args[i].toLowerCase();
				if (arg.startsWith("source=")) {
					cfg.setFolder(arg.split("=")[1]);
					TTBulkFiler.setConfigTTl(arg.split("=")[1]);
				}
				else if (arg.startsWith("preload"))
					TTBulkFiler.setPreload(arg.split("=")[1]);
				else if (arg.startsWith("temp="))
					TTBulkFiler.setDataPath(arg.split("=")[1]);
				else if (arg.startsWith("privacy"))
					TTBulkFiler.setPrivacyLevel(Integer.parseInt(arg.split("=")[1]));
				else if (arg.startsWith("cmd"))
					graphdbCommand= arg.split("=")[1];
				else 	if (args[i].contains("test="))
					ImportApp.testDirectory= args[i].substring(args[i].lastIndexOf("=")+1);
				else
							System.err.println("Unknown parameter " + args[i]);
			}
			if (graphdbCommand==null){
				System.err.println("Must set cmd={graphexecutable} argument to start graph db at the end of the process before processing deltas");
				System.exit(-1);
			}

		importData(cfg,graphdbCommand);
	}

	private static void importData(TTImportConfig cfg,String graphdb) throws Exception {

		    validateGraphConfig(cfg.getFolder());
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
					.validateByType(IM.GRAPH_CEG_QUERY,cfg.getFolder())
					.validateByType(IM.GRAPH_IM1,cfg.getFolder());

				importer.importByType(IM.GRAPH_DISCOVERY, cfg);
				importer.importByType(SNOMED.GRAPH_SNOMED, cfg);
				importer.importByType(IM.GRAPH_ENCOUNTERS, cfg);
				importer.importByType(IM.GRAPH_EMIS, cfg);
				importer.importByType(IM.GRAPH_TPP, cfg);
				importer.importByType(IM.GRAPH_OPCS4, cfg);
				importer.importByType(IM.GRAPH_ICD10, cfg);
				importer.importByType(IM.GRAPH_VISION, cfg);
				importer.importByType(IM.GRAPH_BARTS_CERNER, cfg);
				importer.importByType(IM.GRAPH_ODS, cfg);
				importer.importByType(IM.GRAPH_NHS_TFC,cfg);
				importer.importByType(IM.GRAPH_IM1,cfg);

		TCGenerator closureGenerator = TTFilerFactory.getClosureGenerator();
		closureGenerator.generateClosure(TTBulkFiler.getDataPath(), cfg.isSecure());

			TTBulkFiler.createRepository();
			startGraph(graphdb);
			System.out.println("Filing into live graph starting with CEG");



		  TTFilerFactory.setBulk(false);
		TTFilerFactory.setTransactional(true);
			importer.importByType(IM.GRAPH_KINGS_APEX, cfg);
		importer.importByType(IM.GRAPH_KINGS_WINPATH, cfg);
	   importer.importByType(IM.GRAPH_CEG_QUERY,cfg);
			TTImport deltaImporter= new DeltaImporter();
			deltaImporter.importData(cfg);

		System.out.println("expanding value sets");
		new SetExpander().expandAllSets();
		System.out.println("Finished - " + (new Date()));


		System.out.println("Building text index");
		new LuceneIndexer().buildIndexes();
		System.exit(0);
	}

	public static void validateGraphConfig(String inFolder){
		ImportUtils.validateFiles(inFolder, new String[] {
			".*config.ttl"});

	}

	private static void startGraph(String graphdb) throws IOException, InterruptedException {

		Scanner scanner = new Scanner(System.in);
		boolean ok = false;
		while (!ok) {
			System.out.println("");
			System.err.println("Please start graph db in the usual manner and enter 'OK' when done : ");
			String line = scanner.nextLine();
			if (line.equalsIgnoreCase("ok"))
				ok = true;
		}

		 /*

		System.out.println("Starting graph db....");
		 new Thread(new GraphDBRunner(graphdb)).start();
		 Thread.sleep(15000);

		  */
	}

}
