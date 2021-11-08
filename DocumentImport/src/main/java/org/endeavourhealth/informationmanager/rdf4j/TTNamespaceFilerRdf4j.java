package org.endeavourhealth.informationmanager.rdf4j;

import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.endeavourhealth.imapi.model.tripletree.TTPrefix;
import org.endeavourhealth.informationmanager.TTFilerException;
import org.endeavourhealth.informationmanager.TTNamespaceFiler;

import java.util.List;
import java.util.Map;

public class TTNamespaceFilerRdf4j implements TTNamespaceFiler {
    private RepositoryConnection conn;

    public TTNamespaceFilerRdf4j(RepositoryConnection conn) throws TTFilerException {
        this.conn = conn;
    }

    @Override
    public void fileNamespaces(List<TTPrefix> prefixes, Map<String, String> prefixMap) throws TTFilerException {
        if (prefixes == null || prefixes.isEmpty())
            return;

        try {
            for (TTPrefix prefix : prefixes) {
                conn.setNamespace(prefix.getPrefix(), prefix.getIri());
                prefixMap.put(prefix.getPrefix(), prefix.getIri());
            }
        } catch (RepositoryException e) {
            throw new TTFilerException("Failed to file namespaces", e);
        }
    }
}
