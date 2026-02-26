package org.endeavourhealth.informationmanager.transforms.online;

import org.endeavourhealth.imapi.vocabulary.NAMESPACE;
import org.endeavourhealth.informationmanager.transforms.sources.IndicatorImporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Indicators {
	private static final Logger LOG = LoggerFactory.getLogger(ImportApp.class);

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			LOG.error("Insufficient parameters supplied:");
			LOG.error("Supply namespace iri, full path and file name of txt indicator file, indicator folder iri");
			System.exit(-1);
		}
		NAMESPACE namespace = null;
		String indicatorFile = null;
		String indicatorFolder = null;
		for (String arg : args) {
			String field= arg.split("=")[0].trim();
			String value= arg.split("=")[1].trim();
			if (field.equals("namespace")) {
				namespace = NAMESPACE.from(value);
			}
			if (field.equals("indicatorFile")) {
				indicatorFile = value;
			}
			if (field.equals("indicatorFolder")) {
				indicatorFolder = value;
			}

		}
		new IndicatorImporter().generate(indicatorFile, indicatorFolder, namespace);
	}
}
