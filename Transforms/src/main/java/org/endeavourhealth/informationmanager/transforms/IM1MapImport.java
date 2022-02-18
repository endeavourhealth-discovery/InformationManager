package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringJoiner;

public class IM1MapImport implements TTImport {
    private static final String[] conceptCounts = {".*\\\\DiscoveryLive\\\\concept_counts\\.txt"};

    private TTDocument document;

	// private Connection conn;


	public IM1MapImport() throws SQLException, ClassNotFoundException {
		// conn= ImportUtils.getConnection();
	}

	@Override
	public TTImport importData(TTImportConfig config) throws Exception {
        return importData(config.getFolder(), config.isSecure(), null);
    }

    public TTImport importData(String inFolder, boolean secure, Integer lastDbid) throws Exception {
//        importv1SchemeCodeTable(inFolder, secure);
//        populateMapTable(lastDbid);
//        TTManager manager = new TTManager();
//        document = manager.createDocument(IM.GRAPH_IM1.getIri());
//        System.out.println("Importing Concept counts");
//        importStats(inFolder, document);
//        try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
//            filer.fileDocument(document);
//        }
        return this;
    }
/*

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

    private void importStats(String inFolder, TTDocument document) throws IOException, SQLException {
        Path file = ImportUtils.findFileForId(inFolder, conceptCounts[0]);
        int i = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            reader.readLine();  // NOSONAR - Skipping header
            String line = reader.readLine();
            while (line != null && !line.isEmpty()) {
                String[] fields = line.split("\t");
                String dbid = fields[0];
                String count = fields[fields.length-1];

                addEntity(document, dbid, count);
                i++;
                line= reader.readLine();
            }
        }
        System.out.println("Process ended with " + i);
    }

    private void addEntity(TTDocument document, String dbid, String count) throws SQLException {
        StringJoiner sql = new StringJoiner("\n")
                .add("SELECT e.iri ")
                .add("FROM im1_dbid_scheme_code sc")
                .add("JOIN im1_scheme_map s ON s.scheme = sc.scheme")
                .add("JOIN entity e ON s.namespace = e.scheme AND sc.code = e.code")
                .add("where sc.dbid = ?");
        try (PreparedStatement statement = conn.prepareStatement(sql.toString())) {
            statement.setString(1, dbid);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                TTEntity entity = new TTEntity()
                    .setIri(rs.getString("iri"))
                    .set(IM.USAGE_STATS, new TTArray()
                        .add(new TTNode()
                            .set(IM.USAGE_TOTAL, new TTLiteral().setValue(count))));
                document.addEntity(entity);
            }
        }
    }
*/

	@Override
	public TTImport validateFiles(String inFolder)  {
        ImportUtils.validateFiles(".*\\\\IMv1\\\\IMv1DbidSchemeCode.txt");
        ImportUtils.validateFiles(inFolder,conceptCounts);
        return this;
	}


}
