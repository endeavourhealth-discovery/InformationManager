package org.endeavourhealth.informationmanager.utils.autogenerators;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.model.customexceptions.OpenSearchException;
import org.endeavourhealth.imapi.model.imq.QueryException;
import org.endeavourhealth.imapi.vocabulary.Namespace;

import java.io.IOException;

public class Main {
		public static void main(String[] argv) throws Exception {
			new IndicatorGenerator().generate(argv[0],argv[1], argv[2]);
		}
}

