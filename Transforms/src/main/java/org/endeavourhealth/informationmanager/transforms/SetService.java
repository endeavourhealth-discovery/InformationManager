package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;

import java.sql.SQLException;

import java.util.Set;

public class SetService {
	private SetRepository setRepository= new SetRepository();

	public SetService() throws SQLException, ClassNotFoundException {
	}

	public Set<TTEntity> getAllSets(TTIriRef type) throws SQLException, ClassNotFoundException {
		return setRepository.getAllConceptSets(type);

	}

	public TTEntity getExpansion(String iri) throws SQLException, ClassNotFoundException {
		return setRepository.getExpansion(iri);
	}

	public TTEntity getExpansion(TTEntity conceptSet) throws SQLException {
		return setRepository.getExpansion(conceptSet);

	}

	public TTEntity getIM1Expansion(String iri) throws SQLException{
		return setRepository.getIM1Expansion(iri);
	}

	public TTEntity getIM1Expansion(TTEntity conceptSet) throws SQLException{
		return setRepository.getIM1Expansion(conceptSet);
	}

	public TTEntity getLegacyExpansion(String iri) throws SQLException{
		return setRepository.getLegacyExpansion(iri);
	}

	public TTEntity getLegacyExpansion(TTEntity conceptSet) throws SQLException {
		return setRepository.getLegacyExpansion(conceptSet);

	}
	public TTEntity getSetDefinition(String iri) throws SQLException{
		return setRepository.getSetDefinition(iri);
	}
}