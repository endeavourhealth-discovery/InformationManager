package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.TTFilerException;
import org.endeavourhealth.informationmanager.TTFilerFactory;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class ImportUtilsTest {

	@Test
	void importEmisToSnomed() throws TTFilerException, SQLException, ClassNotFoundException {
		TTFilerFactory.getDocumentFiler();
		ImportUtils.importEmisToSnomed(new HashMap<>(),new HashMap<>());

	}

	@Test
	void importSnomedCodes() throws SQLException, TTFilerException, ClassNotFoundException {
		TTFilerFactory.getDocumentFiler();
		ImportUtils.importSnomedCodes();
	}

	@Test
	void importReadToSnomed() throws TTFilerException, SQLException, ClassNotFoundException {
		TTFilerFactory.getDocumentFiler();
		ImportUtils.importReadToSnomed();
	}

	@Test
	void getDescendants() throws TTFilerException {
		TTFilerFactory.getDocumentFiler();
		ImportUtils.getDescendants("http://endhealth.info/im#Concept", IM.CODE_SCHEME_DISCOVERY.getIri());
	}

	@Test
	void importSimpleSet() throws TTFilerException {
		TTFilerFactory.getDocumentFiler();
	}
}