package org.endeavourhealth.informationmanager.transforms.preload;

import org.endeavourhealth.imapi.filer.TCGenerator;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImportByType;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.filer.rdf4j.TTBulkFiler;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.transforms.sources.Importer;

import java.util.Date;

public class Preload {
	public static String testDirectory;

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("Insufficient parameters supplied:");
			System.err.println("source={sourcefolder} preload={foldercontaing preload} "+
				"temp= {folder for temporary data} and privacy= {0 public, 1 private publisher 2 private for authoring");
			System.exit(-1);
		}
		TTFilerFactory.setBulk(true);

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
				else
							System.err.println("Unknown parameter " + args[i]);
			}

		importData(cfg);
	}

	private static void importData(TTImportConfig cfg) throws Exception {
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
					.validateByType(IM.GRAPH_IM1,cfg.getFolder());
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
				importer.importByType(IM.GRAPH_IM1,cfg);

			TCGenerator closureGenerator = TTFilerFactory.getClosureGenerator();
			closureGenerator.generateClosure(TTBulkFiler.getDataPath(), cfg.isSecure());
			TTBulkFiler.createRepository();
		System.out.println("Finished - " + (new Date()));
	}
}
