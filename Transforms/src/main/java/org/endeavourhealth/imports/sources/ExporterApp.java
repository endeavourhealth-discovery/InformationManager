package    org.endeavourhealth.imports.sources;

import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.logic.service.SetService;

public class ExporterApp {
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("You need to provide a root path to send the data to, and set type");
			System.exit(-1);
		}
		String exportType=args[1].toLowerCase();
        SetService exporter= new SetService();
		switch (exportType) {
			case "conceptsets":
				exporter.exportAll(args[0], IM.CONCEPT_SET);
				break;
            case "singleset":
                if (args.length != 3){
                    System.err.println("You need to provide a set IRI when exporting a single set");
                    System.exit(-1);
                }

                exporter.exportSingle(args[0], args[2]);
                break;
			default:
				throw new Exception("Unrecognised export type");
		}
	}
}
