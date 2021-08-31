package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.common.dal.DALHelper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IM1MapImport implements TTImport {
	public static final String[] im1 = {
		".*\\\\IMv1DbidSchemeCode.txt"};

	private Connection conn;

	private Map<String,Map<String,Set<Integer>>> im1SchemeCodes;
	private Map<Integer, Set<Integer>>im1Map;


	public IM1MapImport() throws SQLException, ClassNotFoundException {
		conn= ImportUtils.getConnection();

	}
	@Override
	public TTImport importData(String inFolder, boolean bulkImport, Map<String, Integer> entityMap) throws SQLException, IOException {
		im1SchemeCodes = new HashMap<>();
		im1Map = new HashMap<>();
		System.out.println("Importing im1 dbid,scheme,codes  .....");
		importIm1Codes(inFolder);
		matchIm2Codes();
		exportMap(inFolder);
		importMapFile(inFolder);
		return this;
	}

	private void matchIm2Codes() throws SQLException {

		//Matches im2 codes and schemes to im1 codes
		System.out.println("Importing IM2 maps from IM1 database");
		PreparedStatement getIm2 = conn.prepareStatement("select dbid,code,scheme from entity where code is not null");
		ResultSet rs = getIm2.executeQuery();
		while (rs.next()) {
			String scheme = rs.getString("scheme");
			String code = rs.getString("code");
			Integer im2 = rs.getInt("dbid");
			Set<Integer> im2im1 = im1Map.get(im2);
			if (im2im1 == null) {
				im2im1 = new HashSet<>();
				im1Map.put(im2, im2im1);
			}

			Map<String, Set<Integer>> codeIM1Db = im1SchemeCodes.get(scheme);
			if (codeIM1Db != null) {
				if (codeIM1Db.get(code) != null) {
					Set<Integer> im1ids = codeIM1Db.get(code);
					for (Integer im1 : im1ids) {
						im2im1.add(im1);
					}
				}
			}
		}
	}

	private void exportMap(String outFolder) throws IOException {
		//Outputs to file
		String outFile= outFolder+"\\im2im1map.txt";
		FileWriter wr= new FileWriter(outFile);
		for (Map.Entry<Integer,Set<Integer>> entry:im1Map.entrySet()) {
			Integer im2= entry.getKey();
			for (Integer im1:entry.getValue()) {
				wr.write(im2 + "\t" + im1 + "\n");
			}
		}
		wr.flush();
		wr.close();
	}

	private void importMapFile(String outpath) throws SQLException {
		System.out.println("Importing im1 map");
		PreparedStatement dropMap = conn.prepareStatement("TRUNCATE TABLE im1Map");
		dropMap.executeUpdate();
		conn.setAutoCommit(false);
		PreparedStatement buildMap = conn.prepareStatement("LOAD DATA INFILE ?"
			+ " INTO TABLE im1map"
			+ " FIELDS TERMINATED BY '\t'"
			+ " LINES TERMINATED BY '\n'"
			+ " (im2, im1)");
		buildMap.setString(1, outpath + "\\im2im1map.txt");
		buildMap.executeUpdate();
		conn.commit();
	}

	private void importIm1Codes(String inFolder) throws IOException {
		Path file = ImportUtils.findFileForId(inFolder, im1[0]);
		try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
			reader.readLine();
			String line = reader.readLine();
			while (line != null && !line.isEmpty()) {
				String[] fields = line.split("\t");
				Integer dbid = Integer.parseInt(fields[0]);
				String scheme = fields[1];
				String code = fields[2];
				String schemeIri = null;
				//Note DM+D not imported
				switch (scheme) {
					case "SNOMED":
						schemeIri = "http://snomed.info/sct#";
						break;
					case "READ2":
						schemeIri = "http://endhealth.info/vis#";
						break;
					case "EMIS_LOCAL":
						schemeIri = "http://endhealth.info/emis#";
						break;
					case "TPP_LOCAL":
						schemeIri = "http://endhealth.info/tpp#";
						break;
					case "CTV3":
						schemeIri = "http://endhealth.info/tpp#";
						break;
					case "OPCS4":
						schemeIri = "http://endhealth.info/opcs4#";
						break;
					case "VISION_LOCAL":
						schemeIri = "http://endhealth.info/opcs4#";
						break;
					case "ICD10":
						schemeIri = "http://endhealth.info/icd10#";
						break;
					case "BartsCerner":
						schemeIri = "http://endhealth.info/bc#";
						break;
					case "ImperialCerner":
						schemeIri = "http://endhealth.info/impc#";
						break;
					case "LE_TYPE":
						schemeIri = "http://endhealth.info/im#";
						break;
					case "CM_DiscoveryCode":
						schemeIri = "http://endhealth.info/im#";
						break;
					default:
						if (scheme.equals("DM+D"))
							schemeIri = "http://endhealth.info/" + scheme.toLowerCase() + "#";
						break;
				}

				if (schemeIri != null) {
					addMap(schemeIri, code, dbid);
				}
				//read 2 doubles up as emis
				if (scheme.equals("READ2")) {
					addMap("http://endhealth.info/emis#", code.replace(".", ""), dbid);
				}
				line = reader.readLine();
			}
		}

	}

	private void addMap(String schemeIri, String code, Integer dbid) {
		Map<String, Set<Integer>> codeDb = im1SchemeCodes.get(schemeIri);
		if (codeDb == null) {
			codeDb = new HashMap<>();
			im1SchemeCodes.put(schemeIri, codeDb);
		}
		Set<Integer> im1ids= codeDb.get(code);
		if (im1ids==null){
			im1ids= new HashSet<>();
			codeDb.put(code,im1ids);
		}
		im1ids.add(dbid);
	}


	@Override
	public TTImport validateFiles(String inFolder)  {
		ImportUtils.validateFiles(inFolder,im1);
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
