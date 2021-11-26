package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.TTImportConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.StringJoiner;

public class IM1MapImport implements TTImport {
	private Connection conn;


	public IM1MapImport() throws SQLException, ClassNotFoundException {
		conn= ImportUtils.getConnection();
	}

	@Override
	public TTImport importData(TTImportConfig config) throws SQLException {
        return importData(config.folder, config.secure, null);
    }

    public TTImport importData(String inFolder, boolean secure, Integer lastDbid) throws SQLException {
        importv1SchemeCodeTable(inFolder, secure);
        populateMapTable(lastDbid);
        return this;
    }

    private void importv1SchemeCodeTable(String inFolder, boolean secure) throws SQLException {
        System.out.println("Importing IMv1");

        try (PreparedStatement stmt = conn.prepareStatement("TRUNCATE TABLE im1_dbid_scheme_code")) {
            stmt.executeUpdate();
        }

        if (secure) {
            try (PreparedStatement stmt = conn.prepareStatement("SET GLOBAL local_infile=1")) {
                stmt.executeUpdate();
            }
        }

        StringJoiner sql = new StringJoiner("\n")
            .add("LOAD DATA");
        if (secure)
            sql.add("LOCAL");
        sql.add("INFILE ?")
            .add("INTO TABLE im1_dbid_scheme_code")
            .add("FIELDS TERMINATED BY '\t'")
            .add("LINES TERMINATED BY '\n'")
            .add("IGNORE 1 LINES");

        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            stmt.setString(1, inFolder + "/IMv1/IMv1DbidSchemeCode.txt");
            stmt.executeUpdate();
        }
	}

    private void populateMapTable(Integer lastId) throws SQLException {
        System.out.println("Generating map");

        if (lastId == null) {
            try (PreparedStatement stmt = conn.prepareStatement("TRUNCATE TABLE im1Map")) {
                stmt.executeUpdate();
            }
        }

        // Direct scheme based maps
        StringJoiner sql = new StringJoiner("\n")
            .add("INSERT INTO im1Map")
            .add("(im2, im1)")
            .add("SELECT e.dbid, c.dbid")
            .add("FROM entity e")
            .add("JOIN im1_scheme_map m ON m.namespace = e.scheme")
            .add("JOIN im1_dbid_scheme_code c ON c.scheme = m.scheme AND c.code = e.code")
            .add("WHERE e.code IS NOT NULL");

        if (lastId != null)
            sql.add("AND c.dbid > " + lastId);

        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            stmt.executeUpdate();
        }

        // Additional READ 2/EMIS map
        sql = new StringJoiner("\n")
            .add("INSERT INTO im1Map")
            .add("(im2, im1)")
            .add("SELECT e.dbid, c.dbid")
            .add("FROM im1_dbid_scheme_code c")
            .add("JOIN entity e ON e.scheme = 'http://endhealth.info/emis#' AND e.code = REPLACE(c.code, '.', '')")
            .add("WHERE c.scheme = 'READ2'");

        if (lastId != null)
            sql.add("AND c.dbid > " + lastId);

        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            stmt.executeUpdate();
        }
    }

	@Override
	public TTImport validateFiles(String inFolder)  {
        ImportUtils.validateFiles(".*\\\\IMv1\\\\IMv1DbidSchemeCode.txt");
        return this;
	}


}
