package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.vocabulary.IM;

public class ExporterApp {
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("You need to provide a root path  to send the data to, and set type");
			System.exit(-1);
		}
		String exportType=args[1].toLowerCase();
		switch (exportType) {
			case "conceptsets":
				SetExporter exporter= new SetExporter();
				exporter.exportAll(args[0], IM.CONCEPT_SET);
				break;
			default:
				throw new Exception("Unrecognised export type");
		}
	}
}
