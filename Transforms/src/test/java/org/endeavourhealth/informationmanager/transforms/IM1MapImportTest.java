package org.endeavourhealth.informationmanager.transforms;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class IM1MapImportTest {

	@Test
	void importData() throws SQLException, ClassNotFoundException {

		IM1MapImport im1Mapper= new IM1MapImport();
		im1Mapper.importData("",false,null);
	}
}