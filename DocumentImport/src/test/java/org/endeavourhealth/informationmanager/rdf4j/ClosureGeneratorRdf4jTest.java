package org.endeavourhealth.informationmanager.rdf4j;

import org.endeavourhealth.imapi.filer.TCGenerator;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.rdf4j.ClosureGeneratorRdf4j;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

class ClosureGeneratorRdf4jTest {

	@Test
	void generateClosure() throws TTFilerException, IOException, SQLException, ClassNotFoundException {
		TCGenerator generator= new ClosureGeneratorRdf4j();
		generator.generateClosure("c:/temp",false);
	}
}