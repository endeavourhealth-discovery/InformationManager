package org.endeavourhealth.informationmanager.rdf4j;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTFilerException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TTDocumentFilerRdf4j extends TTDocumentFiler {
    private static final Logger LOG = LoggerFactory.getLogger(TTDocumentFilerRdf4j.class);

    private Repository repo;
    private RepositoryConnection conn;

    public TTDocumentFilerRdf4j() throws TTFilerException {
        String server = System.getenv("GRAPH_SERVER");
        String repoid = System.getenv("GRAPH_REPO");

        if (server == null || server.isEmpty() || repoid == null || repoid.isEmpty()) {
            LOG.error("For graph db connections, GRAPH_SERVER and GRAPH_REPO environment variables must be set");
            System.exit(-1);
        }

        LOG.info("Connecting");

        try {
            repo = new HTTPRepository(server, repoid);
            repo.initialize();
            conn = repo.getConnection();
            LOG.info("Connected");
        } catch (RepositoryException e) {
            LOG.info("Failed");
            throw new TTFilerException("Failed to open repository connection", e);
        }

        LOG.info("Initializing");
        namespaceFiler = new TTNamespaceFilerRdf4j(conn);
        conceptFiler = new TTEntityFilerRdf4j(conn, prefixMap);
        instanceFiler = conceptFiler;   // Concepts & Instances filed in the same way
        LOG.info("Done");
    }

    @Override
    protected void startTransaction() throws TTFilerException {
        try {
            conn.begin();
        } catch (RepositoryException e) {
            throw new TTFilerException("Failed to start transaction", e);
        }
    }

    @Override
    protected void commit() throws TTFilerException {
        try {
            conn.commit();
        } catch (RepositoryException e) {
            throw new TTFilerException("Failed to commit transaction", e);
        }
    }

    @Override
    protected void rollback() throws TTFilerException {
        try {
            conn.rollback();
        } catch (RepositoryException e) {
            throw new TTFilerException("Failed to rollback transaction", e);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Disconnecting");
        conn.close();
        repo.shutDown();
        LOG.info("Disconnected");
    }
}
