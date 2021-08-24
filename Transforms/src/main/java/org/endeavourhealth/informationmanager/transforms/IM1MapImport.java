package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.informationmanager.TTImport;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

public class IM1MapImport implements TTImport {
	private Connection conn;


	public IM1MapImport() throws SQLException, ClassNotFoundException {
		conn= ImportUtils.getConnection();

	}
	@Override
	public TTImport importData(String inFolder, boolean bulkImport, Map<String, Integer> entityMap) throws SQLException {
		System.out.println("Importing IM1 maps from IM1 database");
		PreparedStatement dropTemp= conn.prepareStatement("drop table if exists im1codes");
		PreparedStatement createTemp= conn.prepareStatement("create temporary table im1codes(\n" +
			"id INT auto_increment NOT null,\n" +
			"im1 INT,\n" +
			"scheme varchar(150),\n" +
			"code varchar(150),\n" +
			"primary key (id),\n" +
			"index x (scheme,code,im1)\n" +
			")\n" +
			";");
		PreparedStatement populateIm1Codes= conn.prepareStatement("insert into im1codes (im1,scheme,code)\n" +
			"SELECT dbid, CASE\n" +
			"    WHEN id like 'SN_%' THEN 'http://snomed.info/sct#'\n" +
			"    WHEN id like 'R2_%' THEN 'http://endhealth.info/vis#'\n" +
			"\tWHEN id like 'R3_%' THEN 'http://endhealth.info/tpp#'\n" +
			"    WHEN id like 'EMLOC_%' THEN 'http://endhealth.info/emis#'\n" +
			"    WHEN id like 'VISLOC_%' THEN 'http://endhealth.info/vis#'\n" +
			"    WHEN id like 'TPPLOC_%' THEN 'http://endhealth.info/tpp#'\n" +
			"    WHEN id like 'BC_%' THEN 'http://endhealth.info/bc#'\n" +
			"    WHEN id like 'LENC_%' THEN 'http://endhealth.info/im#'\n" +
			"    WHEN id like 'O4_%' THEN 'http://endhealth.info/opcs4#'\n" +
			"      WHEN id like 'I10_%' THEN 'http://endhealth.info/icd10#'\n" +
			"\tELSE\n" +
			"    'http://endhealth.info/im#'\n" +
			"\tEND as scheme,\n" +
			"    code\n" +
			"    from im1.concept\n" +
			"union all\n" +
			"SELECT dbid, 'http://endhealth.info/emis#',replace(code,'.','')\n" +
			"from im1.concept\n" +
			"where im1.concept.id like 'R2_%'");

		PreparedStatement generateMap= conn.prepareStatement("insert ignore into im1map (im2,im1)\n" +
			"select entity.dbid as im2, im1codes.im1 as im1\n" +
			" from entity\n" +
			"join im1codes on entity.scheme= im1codes.scheme and entity.code= im1codes.code\n" +
			"\n" +
			";");

		dropTemp.executeUpdate();
		createTemp.executeUpdate();
		System.out.println("Matching code schemes between IM1 and IM2...");
		populateIm1Codes.executeUpdate();
		System.out.println("Importing matched codes and schemes...");
		generateMap.executeUpdate();

		return null;
	}

	@Override
	public TTImport validateFiles(String inFolder)  {

		return this;
	}

	@Override
	public TTImport validateLookUps(Connection conn) throws SQLException, ClassNotFoundException {
		return this;
	}

	@Override
	public void close() throws Exception {

	}
}
