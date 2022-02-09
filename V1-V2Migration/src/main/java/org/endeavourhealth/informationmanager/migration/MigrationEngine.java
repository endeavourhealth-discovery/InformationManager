package org.endeavourhealth.informationmanager.migration;

import org.endeavourhealth.informationmanager.migration.models.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MigrationEngine {

    private PreparedStatement v1Entities;
    private PreparedStatement imv1CPO;

    private PreparedStatement v2Populated;
    private PreparedStatement v2InsNamespace;
    private PreparedStatement v2InsModule;
    private PreparedStatement v2InsEntity;

    public MigrationEngine(Connection imv1, Connection imv2) throws SQLException {
        try {
            // Compile SQL
            v1Entities = imv1.prepareStatement("SELECT c.*, s.id AS schemeId, d.path AS docPath FROM entity c LEFT JOIN entity s ON s.dbid = c.scheme LEFT JOIN document d ON d.dbid = c.document WHERE d.path <> 'InformationModel/dm/DMD' AND (s.id IS NULL OR s.id <> 'DM+D') AND c.use_count > 0 ORDER BY c.dbid");
            imv1CPO = imv1.prepareStatement("SELECT tpl_group.*, p.id AS propertyId FROM entity_property_object tpl_group JOIN entity p ON p.dbid = tpl_group.property WHERE tpl_group.dbid = ? ORDER BY tpl_group.group, tpl_group.property");

            v2Populated = imv2.prepareStatement("SELECT 1 FROM entity LIMIT 1");
            v2InsNamespace = imv2.prepareStatement("INSERT INTO namespace (dbid, prefix, iri) VALUES (?, ?, ?)");
            v2InsModule = imv2.prepareStatement("INSERT INTO module (dbid, iri) VALUES (?, ?)");
            v2InsEntity = imv2.prepareStatement("INSERT INTO entity (dbid, namespace, module, iri, name, description, type, code, scheme, status, weighting, updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        } catch (SQLException e) {
            System.err.println("Failed to prepare SQL");
            e.printStackTrace();
            throw e;
        }
    }

    public void execute() throws SQLException {
        CheckSource();
        CheckDest();

        // preSeed();

        List<DBEntity> entities = importv1Entities();

        // storeV2Entities(entities);
    }


    private void CheckSource() {
        System.out.print("Checking IM v1...");
        System.out.println("Done");
    }
    private void CheckDest() throws SQLException {
        System.out.print("Checking IM v2...");
        try (ResultSet rs = v2Populated.executeQuery()) {
            if (rs.next()) {
                System.err.println("Destination db not empty - it should be to maintain DBID's");
                // throw new IllegalStateException("Destination db not empty - it should be to maintain DBID's");
            }
        }

        System.out.println("Done");
    }

    private void preSeed() throws SQLException {
        // Namespaces
        for (DBNamespace ns : DBNamespace.namespaces)
            insertNamespace(ns.dbid, ns.prefix, ns.iri);

        // Modules
        for (DBModule m : DBModule.modules)
            insertModule(m.dbid, m.iri);
    }

    private void insertNamespace(int dbid, String prefix, String iri) throws SQLException {
        v2InsNamespace.setInt(1, dbid);
        v2InsNamespace.setString(2, prefix);
        v2InsNamespace.setString(3, iri);
        v2InsNamespace.executeUpdate();
    }
    private void insertModule(int dbid, String iri) throws SQLException {
        v2InsModule.setInt(1, dbid);
        v2InsModule.setString(2, iri);
        v2InsModule.executeUpdate();
    }

    private List<DBEntity> importv1Entities() throws SQLException {
        List<String> ignoreSchemes = Arrays.asList("SNOMED", "READ2", "CTV3", "ICD10", "OPCS4");
        List<DBEntity> entities = new ArrayList<>();
        int c = 0;
        System.out.println("Fetching entities...");
        try (ResultSet rs = v1Entities.executeQuery()) {
            System.out.println("Importing...");
            while (rs.next()) {
                c++;

                String scheme = rs.getString("schemeId");
                String docPath = rs.getString("docPath");

                MapData mapData = MapData.getMap(docPath, scheme);

                String id = rs.getString("id");
                String code = rs.getString("code");

                String iri = String.format(mapData.namespace.prefix+mapData.format, code == null ? id : code);

                DBEntity entity = new DBEntity()
                    .setDbid(rs.getInt("dbid"))
                    .setNamespace(mapData.namespace.dbid)
                    .setModule(mapData.module.dbid)
                    .setIri(iri)
                    .setName(rs.getString("name"))
                    .setDescription(rs.getString("Description"))
                    .setCode(rs.getString("code"))
                    .setScheme(rs.getInt("scheme"))
                    .setStatus(rs.getByte("draft"))
                    .setWeighting(rs.getInt("use_count"))
                    .setUpdated(rs.getTimestamp("updated"));

                entities.add(entity);

                if (!ignoreSchemes.contains(scheme))
                    importEntityDefinition(entity);

                if (c % 1000 == 0) {
                    System.out.println("\rImported entity " + c + " (" + id + " -> " + iri + ")...");
                }
            }
        }
        System.out.println("\nImported " + c + " entities");

        return entities;
    }

    private void importEntityDefinition(DBEntity entity) throws SQLException {
        int group = -1;
        String propertyId = "";
        DBAxiom ax = null;
        imv1CPO.setInt(1, entity.getDbid());
        try (ResultSet rs = imv1CPO.executeQuery()) {
            while (rs.next()) {
                if (!"has_parent".equals(rs.getString("propertyId"))) {
                    if (group != rs.getInt("group") || !propertyId.equals(rs.getString("propertyId"))) {
                        group = rs.getInt("group");
                        propertyId = rs.getString("propertyId");
                        ax = new DBAxiom();
                        entity.getAxioms().add(ax);

                        switch (propertyId) {
                            case "is_subtype_of":
                            case "SN_116680003":
                                ax.setType(DBAxiom.SUBCLASS);
                                ax.getExpressions().add(
                                    new DBExpression(DBExpression.CLASS, rs.getInt("value"))
                                );
                                break;
                            case "is_equivalent_to":
                                ax.setType(DBAxiom.EQUIVALENT);
                                ax.getExpressions().add(
                                    new DBExpression(DBExpression.CLASS, rs.getInt("value"))
                                );
                                break;
                            default:
                                throw new IllegalStateException("Unhandled property type [" + propertyId + "] on [" + entity.getIri() + "]");
                        }
                    } else {
                        throw new IllegalStateException("Grouping issue!");
                    }
                }
            }
        }
    }

    private void storeV2Entities(List<DBEntity> entities) throws SQLException {
        int c = 0;
        for (DBEntity entity: entities) {
            System.out.print("\rInserting entity " + c++ + "...");
            v2InsEntity.setInt(1, entity.getDbid());
            v2InsEntity.setInt(2, entity.getNamespace());
            v2InsEntity.setInt(3, entity.getModule());
            v2InsEntity.setString(4, entity.getIri());
            v2InsEntity.setString(5, entity.getName());
            v2InsEntity.setString(6, entity.getDescription());
            v2InsEntity.setByte(7, entity.getType());
            v2InsEntity.setString(8, entity.getCode());
            v2InsEntity.setInt(9, entity.getScheme());
            v2InsEntity.setInt(10, entity.getStatus());
            v2InsEntity.setInt(11, entity.getWeighting());
            v2InsEntity.setTimestamp(12, entity.getUpdated());
            v2InsEntity.executeUpdate();

            if (c % 1000 == 0) {
                System.out.println("\rInserting entity " + c + "...");
            }

        }
        System.out.println("\nInserted " + c + " entities");
    }
}
