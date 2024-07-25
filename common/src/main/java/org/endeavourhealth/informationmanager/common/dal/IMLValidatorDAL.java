package org.endeavourhealth.informationmanager.common.dal;

import org.endeavourhealth.imapi.model.tripletree.TTIriRef;

import java.sql.SQLException;

public interface IMLValidatorDAL {
  boolean isA(TTIriRef descendant, TTIriRef ancestor) throws SQLException;
}
