package org.endeavourhealth.informationmanager;

import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;

public interface TTInstanceFiler {
    void fileEntity(TTEntity entity, TTIriRef graph) throws TTFilerException;
}
