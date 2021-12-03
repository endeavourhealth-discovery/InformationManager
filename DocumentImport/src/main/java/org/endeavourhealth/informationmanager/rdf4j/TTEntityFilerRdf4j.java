package org.endeavourhealth.informationmanager.rdf4j;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.RDF;
import org.endeavourhealth.informationmanager.TTFilerException;
import org.endeavourhealth.informationmanager.TTEntityFiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.eclipse.rdf4j.model.util.Values.*;

public class TTEntityFilerRdf4j implements TTEntityFiler {
    private static final Logger LOG = LoggerFactory.getLogger(TTEntityFilerRdf4j.class);

    private final RepositoryConnection conn;
    private final Map<String, String> prefixMap;

    public TTEntityFilerRdf4j(RepositoryConnection conn, Map<String, String> prefixMap) {
        this.conn = conn;
        this.prefixMap = prefixMap;
    }

    @Override
    public void fileEntity(TTEntity entity, TTIriRef graph) throws TTFilerException {
        try {
            ModelBuilder builder = new ModelBuilder();
            builder = builder.namedGraph(graph.getIri());
            for (Map.Entry<TTIriRef, TTArray> entry : entity.getPredicateMap().entrySet()) {
                addTriple(builder, toIri(entity.getIri()), toIri(entry.getKey().getIri()), entry.getValue());
            }
            conn.add(builder.build());
        } catch (RepositoryException e) {
            throw new TTFilerException("Failed to file entities", e);
        }
    }

    private void addTriple(ModelBuilder builder, Resource subject, IRI predicate, TTArray array) throws TTFilerException {
        for (TTValue value : array.iterator()) {
            addTriple(builder, subject, predicate, value);
        }
    }

    private void addTriple(ModelBuilder builder, Resource subject, IRI predicate, TTValue value) throws TTFilerException {
        if (value.isLiteral()) {
            builder.add(subject, predicate, value.asLiteral().getType() == null
                ? literal(value.asLiteral().getValue())
                : literal(value.asLiteral().getValue(), toIri(value.asLiteral().getType().getIri())));
        }
        else if (value.isIriRef())
            builder.add(subject, predicate, toIri(value.asIriRef().getIri()));
        else if (value.isNode()) {
            TTNode node = value.asNode();
            BNode bNode = bnode();
            builder.add(subject, predicate, bNode);
            for (Map.Entry<TTIriRef, TTArray> entry : node.getPredicateMap().entrySet()) {
                addTriple(builder, bNode, toIri(entry.getKey().getIri()), entry.getValue());
            }
        } else {
            throw new TTFilerException("Unknown value type");
        }
    }

    private IRI toIri(TTIriRef iriRef) throws TTFilerException {
        return toIri(iriRef.getIri());
    }

    private IRI toIri(String iri) throws TTFilerException {
        iri = expand(iri);
        int h = iri.indexOf("#");
        if (h == -1)
            return iri(iri);

        String prefix = iri.substring(0, h +1);
        String suffix = iri.substring(h + 1);

        try {
            String result = prefix + URLEncoder.encode(suffix, StandardCharsets.UTF_8.toString());


            if (!iri.equals(result))
                LOG.trace("Encoded iri [{}] => [{}]", iri, result);

            return iri(result);
        } catch (UnsupportedEncodingException e) {
            throw new TTFilerException("Unable to encode iri", e);
        }
    }

    public String expand(String iri) {
        if (prefixMap == null)
            return iri;
        try {
            int colonPos = iri.indexOf(":");
            String prefix = iri.substring(0, colonPos);
            String path = prefixMap.get(prefix);
            if (path == null)
                return iri;
            else
                return path + iri.substring(colonPos + 1);
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println("invalid iri " + iri);
            return null;
        }
    }
}
