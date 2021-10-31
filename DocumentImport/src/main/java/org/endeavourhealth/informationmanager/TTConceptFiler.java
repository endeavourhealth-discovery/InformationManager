package org.endeavourhealth.informationmanager;

import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;

public interface TTConceptFiler {
    void fileEntity(TTEntity entity, TTIriRef graph) throws TTFilerException;

    Integer getOrSetEntityId(TTIriRef graph) throws TTFilerException;

    String expand(String iri);

    void deleteEntityTypes(Integer entityId, int graphId) throws TTFilerException;

    void fileEntityTypes(TTEntity instance, Integer entityId, int graphId) throws TTFilerException;

    Integer getEntityId(String predicateIri) throws TTFilerException;
}
