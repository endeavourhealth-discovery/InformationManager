package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringJoiner;


public class ConceptCountsImport implements TTImport {
    private static final String[] conceptCounts = {".*\\\\DiscoveryLive\\\\concept_counts\\.txt"};

    private TTDocument document;

    private Connection conn;


    @Override
    public TTImport importData(TTImportConfig config) throws Exception {
        System.out.println("Importing Concept counts");
        importStats(config.folder, document);
        try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }
        return this;
    }

    private void importStats(String inFolder, TTDocument document) throws IOException, SQLException {
        Path file = ImportUtils.findFileForId(inFolder, conceptCounts[0]);
        int i = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
            reader.readLine();
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
                .add("where dbid = ?");
        PreparedStatement statement = conn.prepareStatement(sql.toString());
        statement.setString(1, dbid);
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            TTEntity entity = new TTEntity()
                    .setIri(rs.getString("iri"))
                    .set(IM.USAGE_STATS, new TTArray()
                            .add(new TTNode()
                                    .set(IM.USAGE_TOTAL , new TTLiteral().setValue(count))));
            document.addEntity(entity);
        }
    }

    @Override
    public TTImport validateFiles(String inFolder) {
        ImportUtils.validateFiles(inFolder,conceptCounts);
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
