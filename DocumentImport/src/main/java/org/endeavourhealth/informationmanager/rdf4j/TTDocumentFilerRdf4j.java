package org.endeavourhealth.informationmanager.rdf4j;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTFilerException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class TTDocumentFilerRdf4j extends TTDocumentFiler {
    private static final Logger LOG = LoggerFactory.getLogger(TTDocumentFilerRdf4j.class);

    private Repository repo;
    private RepositoryConnection conn;

    public TTDocumentFilerRdf4j() throws TTFilerException {
        LOG.info("Connecting");
        //repo = new SailRepository(new NativeStore(new File("Z:\\rdf4j")));
        repo = new HTTPRepository("http://localhost:7200/", "im");

        try {
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
