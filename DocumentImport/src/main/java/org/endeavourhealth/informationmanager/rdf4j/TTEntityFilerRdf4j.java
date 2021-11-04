package org.endeavourhealth.informationmanager.rdf4j;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.informationmanager.TTEntityFiler;
import org.endeavourhealth.informationmanager.TTFilerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.eclipse.rdf4j.model.util.Values.*;

public class TTEntityFilerRdf4j implements TTEntityFiler {
    private static final Logger LOG = LoggerFactory.getLogger(TTEntityFilerRdf4j.class);

    private RepositoryConnection conn;

    public TTEntityFilerRdf4j(RepositoryConnection conn, Map<String, String> prefixMap) {
        this.conn = conn;
    }

    @Override
    public void fileEntity(TTEntity entity, TTIriRef graph) throws TTFilerException {
        try {
            ModelBuilder builder = new ModelBuilder();
            builder = builder.namedGraph(graph.getIri());
            for (Map.Entry<TTIriRef, TTValue> entry : entity.getPredicateMap().entrySet()) {
                addTriple(builder, iri(entity.getIri()), iri(entry.getKey().getIri()), entry.getValue());
            }
            conn.add(builder.build());
        } catch (RepositoryException e) {
            throw new TTFilerException("Failed to file entities", e);
        }
    }

    private void addTriple(ModelBuilder builder, Resource subject, IRI predicate, TTValue value) throws TTFilerException {
        if (value.isLiteral()) {
            builder.add(subject, predicate, value.asLiteral().getType() == null
                ? literal(value.asLiteral().getValue())
                : literal(value.asLiteral().getValue(), iri(value.asLiteral().getType().getIri())));
        }
        else if (value.isIriRef())
            builder.add(subject, predicate, iri(value.asIriRef().getIri()));
        else if (value.isNode()) {
            TTNode node = value.asNode();
            BNode bNode = bnode();
            builder.add(subject, predicate, bNode);
            for (Map.Entry<TTIriRef, TTValue> entry : node.getPredicateMap().entrySet()) {
                addTriple(builder, bNode, iri(entry.getKey().getIri()), entry.getValue());
            }
        } else if (value.isList()) {
            TTArray list = value.asArray();
            for (TTValue element: list.getElements()) {
                addTriple(builder, subject, predicate, element);
            }
        } else {
            throw new TTFilerException("Unknown value type");
        }
    }
}
