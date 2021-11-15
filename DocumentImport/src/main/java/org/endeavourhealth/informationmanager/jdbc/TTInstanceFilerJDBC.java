package org.endeavourhealth.informationmanager.jdbc;

import com.google.common.base.Strings;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDF;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.XSD;
import org.endeavourhealth.informationmanager.TTFilerException;
import org.endeavourhealth.informationmanager.TTEntityFiler;
import org.endeavourhealth.informationmanager.common.dal.DALHelper;

import java.sql.*;
import java.util.*;
import java.util.zip.DataFormatException;

public class TTInstanceFilerJDBC implements TTEntityFiler {
    private TTConceptFilerJDBC conceptFiler;
    private Map<String, Integer> instanceMap = new HashMap<>();

    private final Connection conn;

    private final PreparedStatement getInstanceDbid;

    private final PreparedStatement insertInstance;
    private final PreparedStatement updateInstance;

    private final PreparedStatement deleteInstanceTriples;
    private final PreparedStatement insertInstanceTriple;

    /**
     * Constructor for use as part of a TTDocument
     *
     * @param conn      the JDBC connection
     * @param conceptFiler  an instance of a concept filer
     * @throws SQLException in the event of a connection exception
     */
    public TTInstanceFilerJDBC(Connection conn, TTConceptFilerJDBC conceptFiler) throws TTFilerException {
        this(conn);
        this.conceptFiler = conceptFiler;
    }

    /**
     * Constructor to file a entity, requires fully specified IRIs.
     * If used as part of a document use the constructor with entity map to improve performance
     * If the IRIs are prefixed use a constructor with a prefix map parameter
     *
     * @param conn JDBC connection
     * @throws SQLException SQL exception
     */
    private TTInstanceFilerJDBC(Connection conn) throws TTFilerException {
        this.conn = conn;
        try {
            getInstanceDbid = conn.prepareStatement("SELECT dbid FROM inst_entity WHERE iri = ?");
            insertInstance = conn.prepareStatement("INSERT INTO inst_entity (iri,name, description, code, scheme, status) VALUES (?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
            updateInstance = conn.prepareStatement("UPDATE inst_entity SET iri= ?, name = ?, description = ?, code = ?, scheme = ?, status = ? WHERE dbid = ?");

            deleteInstanceTriples = conn.prepareStatement("DELETE FROM inst_tpl WHERE subject=? and graph= ?");
            insertInstanceTriple = conn.prepareStatement("INSERT INTO inst_tpl (subject,blank_node,graph,predicate,instance,object,literal,functional) VALUES(?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
        } catch (SQLException e) {
            throw new TTFilerException("Failed to prepare statements", e);
        }
    }

    public void fileEntity(TTEntity instance, TTIriRef graph) throws TTFilerException {
        int graphId = conceptFiler.getOrSetEntityId(graph);
        Integer instanceDbid = fileInstanceTable(instance);

        if (instance.get(RDFS.LABEL) != null && instance.get(IM.HAS_STATUS) == null)
                instance.set(IM.HAS_STATUS, IM.ACTIVE);

        if (instance.getCrud() != null) {
            if (instance.getCrud().equals(IM.UPDATE))
                updatePredicates(instance, instanceDbid, graphId);
            else if (instance.getCrud().equals(IM.ADD))
                addPredicateObjects(instance, instanceDbid, graphId);
            else
                replacePredicates(instance, instanceDbid, graphId);
        } else
            replacePredicates(instance, instanceDbid, graphId);

    }

    private Integer fileInstanceTable(TTEntity instance) throws TTFilerException {
        String iri = conceptFiler.expand(instance.getIri());
        Integer instanceDbId = getInstanceDbidByIri(iri);
        String label = instance.getName();
        String scheme;
        if (instance.getScheme() != null)
            scheme = instance.getScheme().getIri();
        else
            scheme = iri.substring(0, iri.indexOf("#") + 1);

        // If unnamed and not exists, create draft
        if (label == null && instanceDbId == null) {
            instanceDbId = upsertInstance(null, conceptFiler.expand(iri), null, null, null, scheme, IM.DRAFT.getIri());
        } else {
            String comment = instance.getDescription();
            String code = instance.getCode();

            String status = IM.ACTIVE.getIri();
            if (instance.getStatus() != null)
                status = instance.getStatus().getIri();

            instanceDbId = upsertInstance(instanceDbId, conceptFiler.expand(iri), label, comment, code, scheme, status);
        }

        return instanceDbId;
    }

    private Integer upsertInstance(Integer id, String iri, String name,
                                   String description, String code, String scheme,
                                   String status) throws TTFilerException {

        try {
            if (id == null) {
                // Insert
                int i = 0;
                if (name != null && name.length() > 200)
                        name = name.substring(0, 199);
                DALHelper.setString(insertInstance, ++i, iri);
                DALHelper.setString(insertInstance, ++i, name);
                DALHelper.setString(insertInstance, ++i, description);
                DALHelper.setString(insertInstance, ++i, code);
                DALHelper.setString(insertInstance, ++i, scheme);
                DALHelper.setString(insertInstance, ++i, status);

                if (insertInstance.executeUpdate() == 0)
                    throw new TTFilerException("Failed to insert instance [" + iri + "]");
                else {
                    id = DALHelper.getGeneratedKey(insertInstance);
                    return id;
                }
            } else {
                //update
                int i = 0;
                if (name != null && name.length() > 200)
                        name = name.substring(0, 199);
                DALHelper.setString(updateInstance, ++i, iri);
                DALHelper.setString(updateInstance, ++i, name);
                DALHelper.setString(updateInstance, ++i, description);
                DALHelper.setString(updateInstance, ++i, code);
                DALHelper.setString(updateInstance, ++i, scheme);
                DALHelper.setString(updateInstance, ++i, status);
                DALHelper.setInt(updateInstance, ++i, id);

                if (updateInstance.executeUpdate() == 0) {
                    throw new TTFilerException("Failed to update instance [" + iri + "]");
                } else
                    return id;
            }
        } catch (SQLException e) {
            throw new TTFilerException("Failed to file instance", e);

        }
    }

    /**
     * Adds or replaces a set of predicate objects to a entity, updating the triple tables
     * Note that predicates that need to be removed must use the remove predicate method
     *
     * @param instance the the entity with the predicates to replace
     * @throws SQLException          in the event of a jdbc sql issue
     * @throws DataFormatException   in the node format is incorrect
     * @throws IllegalStateException if the entity is not in the datbase
     */
    private void updatePredicates(TTEntity instance, Integer entityId, int graphId) throws TTFilerException {
        Map<TTIriRef, TTValue> predicates = instance.getPredicateMap();

        //Deletes the previous predicate objects ie. clears out all previous objects
        deletePredicates(entityId, predicates, graphId);
        if (instance.get(RDF.TYPE) != null)
            conceptFiler.deleteEntityTypes(entityId, graphId);
        //Creates transactional adds
        TTNode subject = new TTNode();
        subject.setPredicateMap(predicates);
        fileNode(entityId, null, subject, graphId);

        if (instance.get(RDF.TYPE) != null)
            conceptFiler.fileEntityTypes(instance, entityId, graphId);
    }

    private void deletePredicates(Integer entityId, Map<TTIriRef, TTValue> predicates, int graphId) throws TTFilerException {
        List<Integer> predList = new ArrayList<>();
        int i = 0;
        for (Map.Entry<TTIriRef, TTValue> po : predicates.entrySet()) {
            String predicateIri = po.getKey().getIri();
            Integer predicateId = conceptFiler.getEntityId(predicateIri);
            predList.add(predicateId);
            i++;
        }

        String placeHolders = DALHelper.inListParams(predList.size());

        String stmt = "DELETE from inst_tpl where subject=? and graph=? and predicate in (" + placeHolders + ")";
        try (PreparedStatement deleteObjectPredicates = conn.prepareStatement(stmt)) {
            i = 0;
            DALHelper.setInt(deleteObjectPredicates, ++i, entityId);
            DALHelper.setInt(deleteObjectPredicates, ++i, graphId);
            for (Integer predDbId : predList) {
                DALHelper.setInt(deleteObjectPredicates, ++i, predDbId);
            }
            deleteObjectPredicates.executeUpdate();
        } catch (SQLException e) {
            throw new TTFilerException("Failed to delete predicates", e);
        }
    }

    private void fileNode(Integer entityId, Long parent, TTNode node, int graphId) throws TTFilerException {
        if (node.getPredicateMap() != null && !node.getPredicateMap().isEmpty()) {
                Set<Map.Entry<TTIriRef, TTValue>> entries = node.getPredicateMap().entrySet();
                for (Map.Entry<TTIriRef, TTValue> entry : entries) {
                    //Term codes are denormalised into term code table
                    if (!entry.getKey().equals(IM.HAS_TERM_CODE) && (!entry.getKey().equals(IM.HAS_SCHEME)) && (!entry.getKey().equals(IM.GROUP_NUMBER))) {
                        TTValue object = entry.getValue();
                        if (object.isIriRef()) {
                            fileTriple(entityId, parent, entry.getKey(), object.asIriRef(), null, 1, graphId);
                        } else if (object.isLiteral()) {
                            TTIriRef dataType = XSD.STRING;
                            if (object.asLiteral().getType() != null) {
                                dataType = object.asLiteral().getType();
                            }
                            String data = object.asLiteral().getValue();
                            if (data.length() > 1000)
                                data = data.substring(0, 1000) + "...";
                            fileTriple(entityId, parent, entry.getKey(), dataType, data, 1, graphId);
                        } else if (object.isList()) {
                            fileArray(entityId, parent, entry.getKey(), entry.getValue().asArray(), graphId);
                        } else if (object.isNode()) {
                            Long blankNode = fileTriple(entityId, parent, entry.getKey(), null, null, 1, graphId);
                            fileNode(entityId, blankNode, entry.getValue().asNode(), graphId);
                        }
                    }
                }
            }
    }

    /**
     * Adds or replaces a set of predicate objects to a entity, updating the triple tables
     * Note that predicates that need to be removed must use the remove predicate method
     *
     * @param entity the the entity with the predicates to replace
     * @throws SQLException          in the event of a jdbc sql issue
     * @throws DataFormatException   in the node format is incorrect
     * @throws IllegalStateException if the entity is not in the datbase
     */
    public void addPredicateObjects(TTEntity entity, Integer entityId, int graphId) throws TTFilerException {
        if (entityId == null)
            throw new IllegalStateException("No entity for this iri - " + entity.getIri());

        if (entity.get(RDF.TYPE) != null)
            conceptFiler.fileEntityTypes(entity, entityId, graphId);

        Map<TTIriRef, TTValue> predicates = entity.getPredicateMap();
        //Creates transactional adds
        TTNode subject = new TTNode();
        subject.setPredicateMap(predicates);
        fileNode(entityId, null, subject, graphId);
    }


    private void replacePredicates(TTEntity entity, Integer entityId, int graphId) throws TTFilerException {
        conceptFiler.deleteEntityTypes(entityId, graphId);
        deleteTriples(entityId, graphId);
        fileNode(entityId, null, entity, graphId);
        conceptFiler.fileEntityTypes(entity, entityId, graphId);
    }

    private void deleteTriples(Integer entityId, int graphId) throws TTFilerException {
        DALHelper.setInt(deleteInstanceTriples, 1, entityId);
        DALHelper.setInt(deleteInstanceTriples, 2, graphId);
        try {
            deleteInstanceTriples.executeUpdate();
        } catch (SQLException e) {
            throw new TTFilerException("Failed to delete instance triples", e);
        }
    }

    private void fileArray(Integer entityId, Long parent, TTIriRef predicate, TTArray array, int graphId) throws TTFilerException {
        for (TTValue element : array.getElements()) {
            if (element.isIriRef()) {
                fileTriple(entityId, parent, predicate, element.asIriRef(), null, 0, graphId);
            } else if (element.isNode()) {
                Long blankNode = fileTriple(entityId, parent, predicate, null, null, 0, graphId);
                fileNode(entityId, blankNode, element.asNode(), graphId);
            } else if (element.isLiteral()) {
                TTIriRef dataType = XSD.STRING;
                if (element.asLiteral().getType() != null)
                    dataType = element.asLiteral().getType();
                fileTriple(entityId, parent, predicate, dataType, element.asLiteral().getValue(), 0, graphId);
            } else
                throw new TTFilerException("Cannot have an array of an array in RDF");
        }
    }


    private Long fileTriple(Integer entityId, Long parent, TTIriRef predicate, TTIriRef object, String data, Integer functional, int graphId) throws TTFilerException {
        int i = 0;

        DALHelper.setInt(insertInstanceTriple, ++i, entityId);
        DALHelper.setLong(insertInstanceTriple, ++i, parent);
        DALHelper.setInt(insertInstanceTriple, ++i, graphId);
        DALHelper.setInt(insertInstanceTriple, ++i, conceptFiler.getOrSetEntityId(predicate));

        Integer conceptId = conceptFiler.getEntityId(object.getIri());
        if (conceptId == null) {
            DALHelper.setInt(insertInstanceTriple, ++i, getOrSetInstanceDbid(object));
            DALHelper.setInt(insertInstanceTriple, ++i, null);
        } else {
            DALHelper.setInt(insertInstanceTriple, ++i, null);
            DALHelper.setInt(insertInstanceTriple, ++i, conceptId);
        }

        DALHelper.setString(insertInstanceTriple, ++i, data);
        DALHelper.setInt(insertInstanceTriple, ++i, functional);

        try {
            insertInstanceTriple.executeUpdate();
            return DALHelper.getGeneratedLongKey(insertInstanceTriple);
        } catch (SQLException e) {
            throw new TTFilerException("Failed to file instance triple", e);
        }
    }


    private Integer getInstanceDbidByIri(String iri) throws TTFilerException {
        if (Strings.isNullOrEmpty(iri))
            return null;
        iri = conceptFiler.expand(iri);
        Integer id = instanceMap.get(iri);

        try {
            if (id == null) {
                DALHelper.setString(getInstanceDbid, 1, iri);
                try (ResultSet rs = getInstanceDbid.executeQuery()) {
                    if (rs.next()) {
                        instanceMap.put(iri, rs.getInt("dbid"));
                        return rs.getInt("dbid");
                    } else {
                        return null;
                    }
                }
            }
            return id;
        } catch (SQLException e) {
            throw new TTFilerException("Failed to get instance dbid", e);
        }
    }

    private Integer getOrSetInstanceDbid(TTIriRef iri) throws TTFilerException {
        if (iri == null)
            return null;
        String stringIri = conceptFiler.expand(iri.getIri());
        String scheme = null;
        int lnpos = stringIri.indexOf("#");
        if (lnpos > 0)
            scheme = stringIri.substring(0, stringIri.indexOf("#"));
        Integer id = instanceMap.get(stringIri);
        if (id == null) {
            DALHelper.setString(getInstanceDbid, 1, stringIri);
            try (ResultSet rs = getInstanceDbid.executeQuery()) {
                if (rs.next()) {
                    instanceMap.put(stringIri, rs.getInt("dbid"));
                    return rs.getInt("dbid");
                } else {
                    id = upsertInstance(null, stringIri,
                        null, null, null, scheme, IM.DRAFT.getIri());
                    instanceMap.put(stringIri, id);
                    return id;
                }
            } catch (SQLException e) {
                throw new TTFilerException("Failed to get/create instance dbid");
            }
        }
        return id;
    }
}
