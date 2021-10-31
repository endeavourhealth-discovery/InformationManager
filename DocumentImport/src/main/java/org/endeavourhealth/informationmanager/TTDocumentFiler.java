package org.endeavourhealth.informationmanager;

import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.Date;

public abstract class TTDocumentFiler implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TTDocumentFiler.class);

    private TTIriRef graph;
    Map<String, String> prefixMap = new HashMap<>();
    protected TTNamespaceFiler namespaceFiler;
    protected TTConceptFiler conceptFiler;
    protected TTInstanceFiler instanceFiler;

    protected abstract void startTransaction() throws TTFilerException;
    protected abstract void commit() throws TTFilerException;
    protected abstract void rollback() throws TTFilerException;

    public void fileDocument(TTDocument document) throws TTFilerException {
        try {
            //Sets the graph namesepace id for use in statements so they are owned by the namespace graph
            this.graph = document.getGraph();

            LOG.info("Saving ontology - {}", new Date());
            LOG.info("Processing namespaces");

            // Ensure all namespaces exist (auto-create)
            // Different prefixes for filing are not allowed in this version
            namespaceFiler.fileNamespaces(document.getContext().getNameSpaces(), prefixMap);

            // Record document details, updating ontology and module
            LOG.info("Processing document-ontology-module");

            fileEntities(prefixMap, document);

            LOG.info("Ontology filed");
        } catch (SQLException e) {
            LOG.info("Error - {}", new Date());
            throw new TTFilerException("Failed to file document", e);
        }
        LOG.info("Finished - {}", new Date());
    }

    private void fileEntities(Map<String, String> prefixMap, TTDocument document) throws TTFilerException, SQLException {
        LOG.info("Filing entities.... ");

        startTransaction();
        if (document.getEntities() != null) {
            int i = 0;
            for (TTEntity entity : document.getEntities()) {
                //inherit crud
                if (entity.getCrud() == null)
                    if (document.getCrud() == null) {
                        entity.setCrud(IM.REPLACE);
                    } else {
                        entity.setCrud(document.getCrud());
                    }
                fileEntity(entity, graph);
                i++;
                if (i % 1000 == 0) {
                    LOG.info("Filed {} entities from {} - example {}", i, document.getEntities().size(), entity.getIri());
                    commit();
                    startTransaction();
                }
            }
        }
        commit();
    }

    private void fileEntity(TTEntity entity, TTIriRef graph) throws TTFilerException {
        if (IM.GRAPH_ODS.equals(graph))
            instanceFiler.fileEntity(entity, graph);
        else
            conceptFiler.fileEntity(entity, graph);
    }
}
