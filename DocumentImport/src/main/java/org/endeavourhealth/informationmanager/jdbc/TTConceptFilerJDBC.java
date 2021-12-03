package org.endeavourhealth.informationmanager.jdbc;

import com.google.common.base.Strings;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDF;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.XSD;
import org.endeavourhealth.informationmanager.TTEntityFiler;
import org.endeavourhealth.informationmanager.TTFilerException;
import org.endeavourhealth.informationmanager.common.dal.DALHelper;

import java.sql.*;
import java.util.*;
import java.util.zip.DataFormatException;

public class TTConceptFilerJDBC implements TTEntityFiler {

    Map<String, String> prefixMap = new HashMap<>();
    private Map<String, Integer> entityMap = new HashMap<>();

    private final Connection conn;

    private final PreparedStatement getEntityDbId;
    private final PreparedStatement deleteEntityTypes;
    private final PreparedStatement insertEntityType;
    private final PreparedStatement deleteTriples;
    private final PreparedStatement insertEntity;
    private final PreparedStatement updateEntity;
    private final PreparedStatement insertTriple;
    private final PreparedStatement insertTermEntity;
    private final PreparedStatement getTermDbIdFromTerm;
    private final PreparedStatement updateTermEntity;
    private final PreparedStatement deleteTermEntity;

    public TTConceptFilerJDBC(Connection conn, Map<String, String> prefixMap) throws TTFilerException {
        this(conn);
        this.prefixMap = prefixMap;
    }

    /**
     * Constructor to file a entity, requires fully specified IRIs.
     * If used as part of a document use the constructor with entity map to improve performance
     * If the IRIs are prefixed use a constructor with a prefix map parameter
     *
     * @param conn JDBC connection
     * @throws SQLException SQL exception
     */
    private TTConceptFilerJDBC(Connection conn) throws TTFilerException {
        this.conn = conn;
        try {
            getEntityDbId = conn.prepareStatement("SELECT dbid FROM entity WHERE iri = ?");
            insertEntity = conn.prepareStatement("INSERT INTO entity (iri,name, description, code, scheme, status) VALUES (?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
            updateEntity = conn.prepareStatement("UPDATE entity SET iri= ?, name = ?, description = ?, code = ?, scheme = ?, status = ? WHERE dbid = ?");

            deleteTriples = conn.prepareStatement("DELETE FROM tpl WHERE subject=? and graph= ?");
            insertTriple = conn.prepareStatement("INSERT INTO tpl (subject,blank_node,graph,predicate,object,literal,functional) VALUES(?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);

            deleteEntityTypes = conn.prepareStatement("DELETE FROM entity_type where entity=? and graph=?");
            insertEntityType = conn.prepareStatement("INSERT INTO entity_type (entity,type,graph) VALUES(?,?,?)");

            insertTermEntity = conn.prepareStatement("INSERT INTO term_code SET entity=?, term=?, code=?,graph=?");
            getTermDbIdFromTerm = conn.prepareStatement("SELECT dbid from term_code WHERE term =? and entity=?");
            updateTermEntity = conn.prepareStatement("UPDATE term_code SET entity=?, term=?,code=?, graph=? where dbid=?");
            deleteTermEntity = conn.prepareStatement("DELETE from term_code where entity=? and graph=?");
        } catch (SQLException e) {
            throw new TTFilerException("Failed to prepare statements", e);
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
    private void updatePredicates(TTEntity entity, Integer entityId, int graphId) throws TTFilerException {
        if (entity.get(IM.HAS_TERM_CODE) != null) {
            deleteTermEntities(entityId, graphId);
        }

        //Deletes the previous predicate objects ie. clears out all previous objects
        deletePredicates(entityId,  entity.getPredicateMap(), graphId);
        if (entity.get(RDF.TYPE) != null)
            deleteEntityTypes(entityId, graphId);
        //Creates transactional adds
        fileNode(entityId, null, entity, graphId);
        if (entity.get(IM.HAS_TERM_CODE) != null)
            fileTermCodes(entity, entityId, graphId);
        if (entity.get(RDF.TYPE) != null)
            fileEntityTypes(entity, entityId, graphId);
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
        fileTermCodes(entity, entityId, graphId);
        if (entity.get(RDF.TYPE) != null)
            fileEntityTypes(entity, entityId, graphId);
        Map<TTIriRef, TTArray> predicates = entity.getPredicateMap();
        //Creates transactional adds
        TTNode subject = new TTNode();
        subject.setPredicateMap(predicates);
        fileNode(entityId, null, subject, graphId);

    }

    private void deletePredicates(Integer entityId, Map<TTIriRef, TTArray> predicates, int graphId) throws TTFilerException {
        List<Integer> predList = new ArrayList<>();
        int i = 0;
        for (TTIriRef predicateIri : predicates.keySet()) {
            Integer predicateId = getEntityId(predicateIri.getIri());
            predList.add(predicateId);
            i++;
        }
        StringBuilder builder = new StringBuilder();
        for (Integer ignored : predList) {
            builder.append("?,");
        }
        String placeHolders = builder.deleteCharAt(builder.length() - 1).toString();
        String stmt;
        stmt = "DELETE from tpl where subject=? and graph=? and predicate in (" + placeHolders + ")";

        try (PreparedStatement deleteObjectPredicates = conn.prepareStatement(stmt)) {
            DALHelper.setInt(deleteObjectPredicates, 1, entityId);
            DALHelper.setInt(deleteObjectPredicates, 2, graphId);
            i = 2;
            for (Integer predDbId : predList) {
                DALHelper.setInt(deleteObjectPredicates, ++i, predDbId);
            }
            deleteObjectPredicates.executeUpdate();
        } catch (SQLException e) {
            throw new TTFilerException("Failed to delete predicates", e);
        }
    }


    public void fileEntity(TTEntity entity, TTIriRef graph) throws TTFilerException {
        int graphId = getOrSetEntityId(graph);

        Integer entityId = fileEntityTable(entity);
        if (entity.getIri().contains("VSET_Oral_NSAIDs"))
            System.out.println("test entity");
        if (entity.get(RDFS.LABEL) != null && entity.get(IM.HAS_STATUS) == null)
                entity.set(IM.HAS_STATUS, IM.ACTIVE);
        if (entity.getCrud() != null) {
            if (entity.getCrud().equals(IM.UPDATE))
                updatePredicates(entity, entityId, graphId);
            else if (entity.getCrud().equals(IM.ADD))
                addPredicateObjects(entity, entityId, graphId);
            else
                replacePredicates(entity, entityId, graphId);
        } else
            replacePredicates(entity, entityId, graphId);
    }

    private Integer fileEntityTable(TTEntity entity) throws TTFilerException {
        String iri = expand(entity.getIri());
        Integer entityId = getEntityId(iri);
        String label = entity.getName();
        String comment = entity.getDescription();
        String code = entity.getCode();
        String scheme;
        if (entity.getScheme() != null)
            scheme = entity.getScheme().getIri();
        else
            scheme = iri.substring(0, iri.indexOf("#") + 1);
        String status = IM.ACTIVE.getIri();
        if (entity.getStatus() != null)
            status = entity.getStatus().getIri();
        //uses name for now as the proxy for owning the entity annotations
        if (label == null)
            entityId = getOrSetEntityId(TTIriRef.iri(entity.getIri()));
        else {
            entityId = upsertEntity(entityId,
                expand(iri),
                label, comment, code, scheme, status);
        }
        return entityId;
    }

    private void replacePredicates(TTEntity entity, Integer entityId, int graphId) throws TTFilerException {

        deleteEntityTypes(entityId, graphId);
        deleteTriples(entityId, graphId);
        fileNode(entityId, null, entity, graphId);
        deleteTermEntities(entityId, graphId);
        fileTermCodes(entity, entityId, graphId);
        fileEntityTypes(entity, entityId, graphId);
    }

    private void deleteTermEntities(Integer entityId, int graphId) throws TTFilerException {
        try {
            DALHelper.setInt(deleteTermEntity, 1, entityId);
            DALHelper.setInt(deleteTermEntity, 2, graphId);
            deleteTermEntity.executeUpdate();
        } catch (SQLException e) {
            throw new TTFilerException("Failed to delete term entities", e);
        }
    }

    private void fileTermCodes(TTEntity entity, Integer entityId, int graphId) throws TTFilerException {
        boolean nameFiled = false;
        if (entity.get(IM.HAS_TERM_CODE) != null)
            for (TTValue termCode : entity.get(IM.HAS_TERM_CODE).iterator()) {
                fileTermCode(termCode.asNode(), entityId, graphId);
                if (entity.get(RDFS.LABEL) != null && termCode.asNode().get(RDFS.LABEL).asLiteral().getValue().equals(entity.getName()))
                    nameFiled = true;
            }
        if (!nameFiled && entity.get(RDFS.LABEL) != null) {
            String term = entity.get(RDFS.LABEL).asLiteral().getValue();
            TTNode termCode = new TTNode();
            termCode.set(RDFS.LABEL, TTLiteral.literal(term));
            if (entity.get(IM.CODE) != null)
                termCode.set(IM.CODE, entity.get(IM.CODE));
            fileTermCode(termCode, entityId, graphId);
        }

    }

    public void deleteEntityTypes(Integer entityId, int graphId) throws TTFilerException {
        try {
            DALHelper.setInt(deleteEntityTypes, 1, entityId);
            DALHelper.setInt(deleteEntityTypes, 2, graphId);
            deleteEntityTypes.executeUpdate();
        } catch (SQLException e) {
            throw new TTFilerException("Failed to delete entity type", e);
        }
    }

    private void deleteTriples(Integer entityId, int graphId) throws TTFilerException {
        try {
            DALHelper.setInt(deleteTriples, 1, entityId);
            DALHelper.setInt(deleteTriples, 2, graphId);
            deleteTriples.executeUpdate();
        } catch (SQLException e) {
            throw new TTFilerException("Failed to delete triples", e);
        }
    }

    public void fileEntityTypes(TTEntity entity, Integer entityId, int graphId) throws TTFilerException {
        try {
            TTArray typeValue = entity.get(RDF.TYPE);
            if (typeValue == null)
                return;

            for (TTValue type : typeValue.iterator()) {
                if (!type.isIriRef())
                    throw new TTFilerException("Entity types must be array of IriRef ");
                fileEntityType(entityId, type, graphId);
            }

        } catch (SQLException e) {
            throw new TTFilerException("Failed to file entity types", e);
        }
    }

    private void fileEntityType(Integer entityId, TTValue type, int graphId) throws SQLException {
        DALHelper.setInt(insertEntityType, 1, entityId);
        DALHelper.setString(insertEntityType, 2, type.asIriRef().getIri());
        DALHelper.setInt(insertEntityType, 3, graphId);
        insertEntityType.executeUpdate();

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

    private void fileNode(Integer entityId, Long parent, TTNode node, int graphId) throws TTFilerException {
        if (node.getPredicateMap() != null && !node.getPredicateMap().isEmpty()) {
            Set<Map.Entry<TTIriRef, TTArray>> entries = node.getPredicateMap().entrySet();
            for (Map.Entry<TTIriRef, TTArray> entry : entries) {
                //Term codes are denormalised into term code table
                if (!entry.getKey().equals(IM.HAS_TERM_CODE) && (!entry.getKey().equals(IM.HAS_SCHEME)) && (!entry.getKey().equals(IM.GROUP_NUMBER))) {
                    TTArray object = entry.getValue();
                    fileArray(entityId, parent, entry.getKey(), entry.getValue(), graphId);
                }
            }
        }
    }

    private Long fileTriple(Integer entityId, Long parent,
                            TTIriRef predicate, TTIriRef targetType, String data,
                            Integer functional, int graphId) throws TTFilerException {
        int i = 0;
        try {
            DALHelper.setInt(insertTriple, ++i, entityId);
            DALHelper.setLong(insertTriple, ++i, parent);
            DALHelper.setInt(insertTriple, ++i, graphId);
            DALHelper.setInt(insertTriple, ++i, getOrSetEntityId(predicate));
            DALHelper.setInt(insertTriple, ++i, getOrSetEntityId(targetType));
            DALHelper.setString(insertTriple, ++i, data);
            DALHelper.setInt(insertTriple, ++i, functional);
            insertTriple.executeUpdate();
            return DALHelper.getGeneratedLongKey(insertTriple);
        } catch (SQLException e) {
            throw new TTFilerException("Failed to file triple", e);
        }
    }


    public Integer getEntityId(String iri) throws TTFilerException {
        try {
            if (Strings.isNullOrEmpty(iri))
                return null;
            iri = expand(iri);
            Integer id = entityMap.get(iri);
            if (id == null) {
                DALHelper.setString(getEntityDbId, 1, iri);
                try (ResultSet rs = getEntityDbId.executeQuery()) {
                    if (rs.next()) {
                        entityMap.put(iri, rs.getInt("dbid"));
                        return rs.getInt("dbid");
                    } else {
                        return null;
                    }
                }
            }
            return id;
        } catch (SQLException e) {
            throw new TTFilerException("Failed to get entity id by IRI", e);
        }
    }

    // ------------------------------ Entity ------------------------------
    public Integer getOrSetEntityId(TTIriRef iri) throws TTFilerException {
        if (iri == null)
            return null;

        try {
            String stringIri = expand(iri.getIri());
            String scheme = null;
            int lnpos = stringIri.indexOf("#");
            if (lnpos > 0)
                scheme = stringIri.substring(0, stringIri.indexOf("#"));
            Integer id = entityMap.get(stringIri);
            if (id == null) {
                DALHelper.setString(getEntityDbId, 1, stringIri);
                try (ResultSet rs = getEntityDbId.executeQuery()) {
                    if (rs.next()) {
                        entityMap.put(stringIri, rs.getInt("dbid"));
                        return rs.getInt("dbid");
                    } else {
                        id = upsertEntity(null, stringIri,
                            null, null, null, scheme, IM.DRAFT.getIri());
                        entityMap.put(stringIri, id);
                        return id;
                    }
                }
            }
            return id;
        } catch (SQLException e) {
            throw new TTFilerException("Failed to get/create entity id", e);
        }
    }

    private Integer upsertEntity(Integer id, String iri, String name,
                                 String description, String code, String scheme,
                                 String status) throws TTFilerException {

        try {
            if (id == null) {
                // Insert
                int i = 0;
                if (name != null && name.length() > 200)
                        name = name.substring(0, 199);

                DALHelper.setString(insertEntity, ++i, iri);
                DALHelper.setString(insertEntity, ++i, name);
                DALHelper.setString(insertEntity, ++i, description);
                DALHelper.setString(insertEntity, ++i, code);
                DALHelper.setString(insertEntity, ++i, scheme);
                DALHelper.setString(insertEntity, ++i, status);

                if (insertEntity.executeUpdate() == 0)
                    throw new TTFilerException("Failed to insert entity [" + iri + "]");
                else {
                    id = DALHelper.getGeneratedKey(insertEntity);
                    return id;
                }
            } else {
                //update
                int i = 0;
                if (name != null && name.length() > 200)
                        name = name.substring(0, 199);
                DALHelper.setString(updateEntity, ++i, iri);
                DALHelper.setString(updateEntity, ++i, name);
                DALHelper.setString(updateEntity, ++i, description);
                DALHelper.setString(updateEntity, ++i, code);
                DALHelper.setString(updateEntity, ++i, scheme);
                DALHelper.setString(updateEntity, ++i, status);
                DALHelper.setInt(updateEntity, ++i, id);

                if (updateEntity.executeUpdate() == 0) {
                    throw new TTFilerException("Failed to update entity [" + iri + "]");
                } else
                    return id;
            }
        } catch (SQLException e) {
            throw new TTFilerException("Failed to file " + iri, e);
        }
    }

    private void fileTermCode(TTNode termCode, Integer entityId, int graphId) throws TTFilerException {
        int i = 0;
        Integer dbid = null;
        String term = termCode.get(RDFS.LABEL).asLiteral().getValue();
        String code = null;
        if (termCode.get(IM.CODE) != null)

            code = termCode.get(IM.CODE).asLiteral().getValue();
        DALHelper.setString(getTermDbIdFromTerm, ++i, term);
        DALHelper.setInt(getTermDbIdFromTerm, ++i, entityId);

        try (ResultSet rs = getTermDbIdFromTerm.executeQuery()) {
            if (rs.next())
                dbid = rs.getInt("dbid");

            if (dbid != null) {
                updateTermEntity(entityId, term, code, dbid, graphId);
            } else {
                insertTermEntity(entityId, term, code, graphId);
            }
        } catch (SQLException e) {
            throw new TTFilerException("Failed to file term code", e);
        }

    }

    private void insertTermEntity(Integer entityId, String term, String code, int graphId) throws SQLException {
        int i = 0;
        if (term.length() > 250)
            term = term.substring(0, 250);
        DALHelper.setInt(insertTermEntity, ++i, entityId);
        DALHelper.setString(insertTermEntity, ++i, term);
        DALHelper.setString(insertTermEntity, ++i, code);
        DALHelper.setInt(insertTermEntity, ++i, graphId);
        if (insertTermEntity.executeUpdate() == 0)
            throw new SQLException("Failed to save term entity for  ["
                + term + " ]");
    }

    private void updateTermEntity(Integer entityId, String term, String code, Integer dbid, int graphId) throws SQLException {
        int i = 0;
        if (term.length() > 250)
            term = term.substring(0, 250);
        DALHelper.setInt(updateTermEntity, ++i, entityId);
        DALHelper.setString(updateTermEntity, ++i, term);
        DALHelper.setString(updateTermEntity, ++i, code);
        DALHelper.setInt(updateTermEntity, ++i, graphId);
        DALHelper.setInt(updateTermEntity, ++i, dbid);
        updateTermEntity.executeUpdate();

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
