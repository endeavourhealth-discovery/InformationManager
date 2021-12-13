package org.endeavourhealth.informationmanager.rdf4j;

import org.endeavourhealth.informationmanager.TCGenerator;
import org.endeavourhealth.informationmanager.TTFilerException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class ClosureGeneratorRdf4jTest {

	@Test
	void generateClosure() throws TTFilerException, IOException, SQLException, ClassNotFoundException {
		TCGenerator generator= new ClosureGeneratorRdf4j();
		generator.generateClosure("c:/temp",false);
	}
}