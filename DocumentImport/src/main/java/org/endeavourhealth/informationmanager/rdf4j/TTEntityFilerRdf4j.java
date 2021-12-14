package org.endeavourhealth.informationmanager.rdf4j;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDF;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.informationmanager.TTEntityFiler;
import org.endeavourhealth.informationmanager.TTFilerException;
import org.endeavourhealth.informationmanager.TTFilerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.Objects;

import static org.eclipse.rdf4j.model.util.Values.*;

public class TTEntityFilerRdf4j implements TTEntityFiler {
    private static final Logger LOG = LoggerFactory.getLogger(TTEntityFilerRdf4j.class);

    private final RepositoryConnection conn;
    private final Map<String, String> prefixMap;
    private final Update deleteTriples;

    private static final ValueFactory valueFactory= new ValidatingValueFactory(SimpleValueFactory.getInstance());

    public TTEntityFilerRdf4j(RepositoryConnection conn, Map<String, String> prefixMap) {
        this.conn = conn;
        this.prefixMap = prefixMap;
        deleteTriples= conn.prepareUpdate("DELETE {?concept ?p1 ?o1.\n" +
          "        ?o1 ?p2 ?o2.\n" +
          "        ?o2 ?p3 ?o3.\n" +
          "        ?o3 ?p4 ?o4." +
          "        ?o4 ?p5 ?o5.}\n" +
          "where \n" +
          "    { GRAPH ?graph {?concept ?p1 ?o1.\n" +
          "    OPTIONAL {?o1 ?p2 ?o2.\n" +
          "        filter (isBlank(?o1))\n" +
          "        OPTIONAL { \n" +
          "            ?o2 ?p3 ?o3\n" +
          "            filter (isBlank(?o2))\n" +
          "            OPTIONAL {?o3 ?p4 ?o4.\n" +
          "                filter(isBlank(?o3))" +
          "                OPTIONAL {?o4 ?p5 ?o5" +
          "                    filter(isBlank(?o4))}}}\n" +
          "        }}}");
    }

    @Override
    public void fileEntity(TTEntity entity, TTIriRef graph) throws TTFilerException {

        if (entity.get(RDFS.LABEL) != null && entity.get(IM.HAS_STATUS) == null)
            entity.set(IM.HAS_STATUS, IM.ACTIVE);
        if (entity.getCrud() != null) {
            if (entity.getCrud().equals(IM.UPDATE))
                updatePredicates(entity, graph);
            else if (entity.getCrud().equals(IM.ADD))
               fileEntityPredicates(entity, graph);
            else
                replacePredicates(entity, graph);
        } else
            replacePredicates(entity, graph);
    }


    private void replacePredicates(TTEntity entity,TTIriRef graph) throws TTFilerException {
        if (!TTFilerFactory.skipDeletes)
            deleteTriples(entity, graph);
        fileEntityPredicates(entity, graph);
    }

    private void fileEntityPredicates(TTEntity entity,TTIriRef graph) throws TTFilerException {
        try {
            ModelBuilder builder = new ModelBuilder();
            builder = builder.namedGraph(graph.getIri());
            for (Map.Entry<TTIriRef, TTArray> entry : entity.getPredicateMap().entrySet()) {
                addTriple(builder, toIri(entity.getIri()), toIri(entry.getKey().getIri()), entry.getValue());
            }
            conn.add(builder.build());
        } catch (RepositoryException | TTFilerException e) {
            throw new TTFilerException("Failed to file entities", e);
        }

    }

    private void deleteTriples(TTEntity entity, TTIriRef graph) throws TTFilerException {
        try {
            deleteTriples.setBinding("concept", valueFactory.createIRI(entity.getIri()));
            deleteTriples.setBinding("graph", valueFactory.createIRI(graph.getIri()));
            deleteTriples.execute();
        } catch (RepositoryException e){
            throw new TTFilerException("Failed to delete triples");
        }

    }

    private void deletePredicates(TTEntity entity, TTIriRef graph) throws TTFilerException {
        StringBuilder predList= new StringBuilder();
        int i=0;
        Map<TTIriRef,TTArray> predicates= entity.getPredicateMap();
        for (Map.Entry<TTIriRef, TTArray> po : predicates.entrySet()) {
            String predicateIri = po.getKey().getIri();
            i++;
            if (i > 1)
                predList.append(", ");
            predList.append("<" + predicateIri + ">");
        }
        String spq="DELETE {?concept ?p1 ?o1.\n" +
          "        ?o1 ?p2 ?o2.\n" +
          "        ?o2 ?p3 ?o3.\n" +
          "        ?o3 ?p4 ?o4.}\n" +
          "where \n" +
          "    {?concept ?p1 ?o1.\n" +
          "    filter(?p1 in("+predList+"))\n" +
          "    OPTIONAL {?o1 ?p2 ?o2.\n" +
          "        filter (isBlank(?o1))\n" +
          "        OPTIONAL { \n" +
          "            ?o2 ?p3 ?o3\n" +
          "            filter (isBlank(?o2))\n" +
          "            OPTIONAL {?o3 ?p4 ?o4.\n" +
          "                filter(!isBlank(?o3))}}\n" +
          "        }}\n";
        Update deletePredicates= conn.prepareUpdate(spq);
        deletePredicates.setBinding("concept",valueFactory.createIRI(entity.getIri()));
        try {
            deletePredicates.execute();
        }catch (RepositoryException e){
            throw new TTFilerException("Failed to delete triples");
        }

    }
    private void updatePredicates(TTEntity entity, TTIriRef graph) throws TTFilerException {

        //Deletes the previous predicate objects ie. clears out all previous objects
        if (!TTFilerFactory.skipDeletes)
            deletePredicates(entity,graph);
        fileEntityPredicates(entity,graph);
    }

    private void addTriple(ModelBuilder builder, Resource subject, IRI predicate, TTArray values) throws TTFilerException {
        for (TTValue value:values.getElements()){
            if (value.isLiteral()) {
                builder.add(subject, predicate, value.asLiteral().getType() == null
                ? literal(value.asLiteral().getValue())
                : literal(value.asLiteral().getValue(), toIri(value.asLiteral().getType().getIri())));
            }
            else if (value.isIriRef()) {
                builder.add(subject, predicate, toIri(value.asIriRef().getIri()));
            }
            else if (value.isNode()) {
                TTNode node = value.asNode();
                BNode bNode = bnode();
                builder.add(subject, predicate, bNode);
                for (Map.Entry<TTIriRef, TTArray> entry : node.getPredicateMap().entrySet()) {
                    addTriple(builder, bNode, toIri(entry.getKey().getIri()), entry.getValue());
                }
            }
            else {
                throw new TTFilerException("Arrays of arrays not allowed ");
            }
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
