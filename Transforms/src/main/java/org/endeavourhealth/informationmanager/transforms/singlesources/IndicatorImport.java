package org.endeavourhealth.informationmanager.transforms.singlesources;

import org.endeavourhealth.imapi.vocabulary.NAMESPACE;
import org.endeavourhealth.informationmanager.transforms.sources.IndicatorImporter;
import org.endeavourhealth.informationmanager.transforms.sources.SingleEqdQueryImport;

public class IndicatorImport {
	public static void main(String[] argv) throws Exception {
		String indicatorFile = argv[0];

		String indicatorFolderIri = argv[1];
		String indicatorFolderName = argv[2];
		NAMESPACE namespace= NAMESPACE.from(argv[3]);
		new IndicatorImporter().generate(indicatorFile,indicatorFolderIri,indicatorFolderName,namespace);
	}
}
