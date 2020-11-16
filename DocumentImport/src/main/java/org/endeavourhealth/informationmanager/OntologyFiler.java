package org.endeavourhealth.informationmanager;

import com.sun.istack.internal.NotNull;
import org.endeavourhealth.informationmanager.common.transform.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

public class OntologyFiler {
    private static final Logger LOG = LoggerFactory.getLogger(OntologyFiler.class);
    private static final String SUBTYPE = "sn:116680003";
    private final OntologyFilerJDBCDAL dal;
    private final Set<String> undefinedConcepts = new HashSet<>();
    /**
     * Files a Discovery syntax ontology module and/or classification module into a relational database OWL ontoology store.
     * <p>This assumes that all axioms for a concept for this module are included i.e. is a replace all axioms for concept for module</p>
     * <p></p>Concepts from other modules can be referenced in the axiom but if the entity is declared in this ontology, then the associated axioms for this module will be filed</p>
     *<p>Note that only those declared concepts whose IRIS have module/ namespace authoring permission will be updated </p>
     * Thus if the ontology module contains annotation properties for concepts with IRIs from external namespaces that do not have autthoring permission
     * they will be ignored.
     * @throws Exception
     */
    public OntologyFiler() throws Exception {
        dal = new OntologyFilerJDBCDAL();

    }

    // ============================== PUBLIC METHODS ============================

    /**
     * Files a classification module consisting of child/parent nodes of concepts.
     * <p> Assumes that all parents are present for a child concept for this module i.e. is an add+ replacement process.
     * If other concept parents exist for this module which are not included in the set they will not be removed.</p>
     *
     *  @param nodeSet A set of concept references assuming concepts already exist in the data store
     * @throws Exception
     */
    public void fileClassification(@NotNull Set<ConceptReferenceNode> nodeSet, String moduleIri) throws Exception {
        try {
            if (!nodeSet.isEmpty()) {
                int i=0;
                startTransaction();
                for (ConceptReferenceNode node : nodeSet)
                    dal.FileIsa(node,moduleIri);
                i++;
                if (i % 1000 == 0) {
                    LOG.info("Processed " + i + " of " + nodeSet.size());
                    dal.commit();
                }
            }
             commit();
             close();

        } catch (Exception e) {
            rollback();
            close();
            throw e;

        } finally {
            close();
        }
    }

    /**
     * Files a Discovery syntax ontology module into a relational database OWL ontoology store.
     * <p>This assumes that all axioms for a concept for this module are included i.e. is a replace all axioms for concept for module</p>
     * <p></p>Concepts from other modules can be referenced in the axiom but if the entity is declared in this ontology, then the associated axioms for this module will be filed</p>
     *<p>Note that only those declared concepts whose IRIS have module/ namespace authoring permission will be updated </p>
     * Thus if the ontology module contains annotation properties for concepts with IRIs from external namespaces that do not have autthoring permission
     * they will be ignored.
     * @param ontology  An ontology module document in Discovery syntax
        * @throws Exception
     */
    public boolean fileOntology(Ontology ontology) throws Exception {
        try {

            LOG.info("Saving ontology");
            startTransaction();
            LOG.info("Processing namespaces");
            // Ensure all namespaces exist (auto-create)
            //The document prefixes (ns) may not be the same as the IM DB prefixes
            fileNamespaces(ontology.getNamespace());
            commit();
            ;
            // Record document details, updating ontology and module
            LOG.info("Processing document-ontology-module");
            fileDocument(ontology);
            commit();

            LOG.info("Processing Classes");
            fileConcepts(ontology.getConcept());
            commit();

            fileIndividuals(ontology.getIndividual());
            commit();

            LOG.info("Ontology filed");
        } catch (Exception e) {
            rollback();
            close();
            throw e;
        } finally {
            close();
            return true;
        }
    }


    //==================PRIVATE METHODS=================

    private void startTransaction() throws SQLException {
        dal.startTransaction();
    }

    private void close() throws SQLException {
        dal.close();
    }

    private void rollback() throws SQLException {
        dal.rollBack();
    }

    private void commit() throws SQLException {
        dal.commit();
    }


    private void fileNamespaces(Set<Namespace> namespaces) throws SQLException {
        Namespace nullNamespace = new Namespace();
        nullNamespace.setIri("");
        nullNamespace.setPrefix("");
        dal.upsertNamespace(nullNamespace);
        if (namespaces == null || namespaces.size() == 0)
            return;
        //Populates the namespace map with both namespace iri and prefix as keys
        for (Namespace ns : namespaces) {
            dal.upsertNamespace(ns);

        }

    }


    // ------------------------------ DOCUMENT MODULE ONTOLOGY ------------------------------
    private void fileDocument(Ontology ontology) throws SQLException {
        dal.upsertModule(ontology.getModule());
        dal.upsertOntology(ontology.getIri());
        dal.addDocument(ontology);
    }


    private void fileIndividuals(Set<Individual> indis) throws Exception {
        if (indis == null || indis.size() == 0)
            return;

        int i = 0;
        for (Individual ind : indis) {

        }

    }

    private void fileConcepts(Set<? extends Concept> concepts) throws Exception {
        if (concepts == null || concepts.size() == 0)
            return;

        int i = 0;
        for (Concept concept : concepts) {
            dal.upsertConcept(concept);
           dal.fileAxioms(concept);

            i++;
            if (i % 1000 == 0) {
                LOG.info("Processed " + i + " of " + concepts.size());
                dal.commit();
            }
        }
        dal.commit();
    }




}

