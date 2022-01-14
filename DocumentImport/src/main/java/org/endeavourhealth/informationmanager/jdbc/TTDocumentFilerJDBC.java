package org.endeavourhealth.informationmanager.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

public class TTDocumentFilerJDBC extends TTDocumentFiler {
    private Connection conn;

    public TTDocumentFilerJDBC() throws TTFilerException {
        conn = ConnectionManagerJDBC.get();

        namespaceFiler = new TTNamespaceFilerJDBC(conn);
        TTConceptFilerJDBC conceptFilerJDBC = new TTConceptFilerJDBC(conn, prefixMap);
        conceptFiler = conceptFilerJDBC;
        instanceFiler = new TTInstanceFilerJDBC(conn, conceptFilerJDBC);
    }

    @Override
    protected void startTransaction() throws TTFilerException {
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new TTFilerException("Failed to start transaction", e);
        }
    }

    @Override
    protected void commit() throws TTFilerException {
        try {
            conn.commit();
        } catch (SQLException e) {
            throw new TTFilerException("Failed to commit transaction", e);
        }
    }

    @Override
    protected void rollback() throws TTFilerException {
        try {
            conn.rollback();
        } catch (SQLException e) {
            throw new TTFilerException("Failed to rollback transaction", e);
        }
    }

    @Override
    public void close() throws Exception {
        conn.close();
    }
}
