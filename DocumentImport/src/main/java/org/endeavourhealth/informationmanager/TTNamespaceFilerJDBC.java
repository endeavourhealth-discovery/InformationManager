package org.endeavourhealth.informationmanager;

import org.endeavourhealth.imapi.model.tripletree.TTPrefix;
import org.endeavourhealth.informationmanager.common.dal.DALHelper;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TTNamespaceFilerJDBC implements TTNamespaceFiler {
    private Connection conn;

    private PreparedStatement getNamespace;
    private PreparedStatement getNsFromPrefix;
    private PreparedStatement insertNamespace;
    private PreparedStatement updateNamespace;

    public TTNamespaceFilerJDBC() throws TTFilerException {
        conn = ConnectionManagerJDBC.get();
        prepareStatements();
    }

    private void prepareStatements() throws TTFilerException {
        try {
            getNamespace = conn.prepareStatement("SELECT * FROM namespace WHERE iri = ?");
            getNsFromPrefix = conn.prepareStatement("SELECT * FROM namespace WHERE prefix = ?");
            insertNamespace = conn.prepareStatement("INSERT INTO namespace (iri, prefix,name) VALUES (?, ?,?)", Statement.RETURN_GENERATED_KEYS);
            updateNamespace = conn.prepareStatement("UPDATE namespace SET name=? WHERE dbid=?");
        } catch (SQLException e) {
            throw new TTFilerException("Failed to prepare SQL statements", e);
        }
    }

    @Override
    public void fileNamespaces(List<TTPrefix> prefixes, Map<String, String> prefixMap) throws TTFilerException {
        if (prefixes == null || prefixes.isEmpty())
            return;

        try {
            conn.setAutoCommit(false);
            //Populates the namespace map with both namespace iri and prefix as keys
            for (TTPrefix prefix : prefixes) {
                upsertNamespace(prefixMap, prefix);
            }
            conn.commit();
        } catch (SQLException e) {
            throw new TTFilerException("Failed to file namespaces", e);
        }
    }

    private void upsertNamespace(Map<String, String> prefixMap, TTPrefix ns) throws SQLException {
        DALHelper.setString(getNamespace, 1, ns.getIri());
        try (ResultSet rs = getNamespace.executeQuery()) {
            if (rs.next()) {
                Integer dbid = rs.getInt("dbid");
                if (!ns.getPrefix().equals(rs.getString("prefix"))) {
                    throw new SQLException("prefix in database -> " + ns.getPrefix() + " does not match the iri " + ns.getIri());
                } else {
                    if (ns.getName() != null) {
                        DALHelper.setString(updateNamespace, 1, ns.getName());
                        DALHelper.setInt(updateNamespace, 2, dbid);
                        updateNamespace.executeUpdate();
                    }
                    prefixMap.put(ns.getPrefix(), ns.getIri());
                }
            } else {
                DALHelper.setString(getNsFromPrefix, 1, ns.getPrefix());
                ResultSet ps = getNsFromPrefix.executeQuery();
                if (ps.next()) {
                    if (!ns.getIri().equals(ps.getString("iri"))) {
                        throw new SQLException(ps.getString("prefix") + "->" + ps.getString("iri) "
                            + " does not match " + ns.getIri() + ns.getIri()));
                    }
                } else {
                    DALHelper.setString(insertNamespace, 1, ns.getIri());
                    DALHelper.setString(insertNamespace, 2, ns.getPrefix());
                    DALHelper.setString(insertNamespace, 3, ns.getName());
                    insertNamespace.executeUpdate();
                    prefixMap.put(ns.getPrefix(), ns.getIri());
                }
            }
        }
    }
}
