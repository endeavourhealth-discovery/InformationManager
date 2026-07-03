package org.endeavourhealth.informationmanager.transforms.singlesources;

import org.endeavourhealth.informationmanager.transforms.sources.SingleEqdQueryImport;
import org.endeavourhealth.imapi.vocabulary.NAMESPACE;

public class SingleQuery {
	public static void main(String[] argv) throws Exception {
		String importFolder = argv[0];
		String reportId = argv[1];
		String subFolder = argv[2];
		NAMESPACE namespace= NAMESPACE.from(argv[3]);
		new SingleEqdQueryImport().importEqd(importFolder,subFolder,reportId,namespace);
		}
}
