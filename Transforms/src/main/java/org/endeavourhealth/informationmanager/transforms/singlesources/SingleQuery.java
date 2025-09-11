package org.endeavourhealth.informationmanager.transforms.singlesources;

import org.endeavourhealth.imapi.vocabulary.Namespace;
import org.endeavourhealth.informationmanager.transforms.sources.SingleEqdQueryImport;

public class SingleQuery {
	public static void main(String[] argv) throws Exception {
		String importFolder = argv[0];
		String reportId = argv[1];
		Namespace namespace= Namespace.from(argv[2]);
		new SingleEqdQueryImport().importEqd(importFolder,reportId,namespace);
		}
}
