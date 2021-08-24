package org.endeavourhealth.informationmanager.transforms;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTValue;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.common.transform.TTToECL;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SetService {
	private SetRepository setRepository= new SetRepository();

	public SetService() throws SQLException, ClassNotFoundException {
	}

	public Set<TTEntity> getAllSets(TTIriRef type) throws SQLException, ClassNotFoundException {
		return setRepository.getAllConceptSets(type);

	}

	public TTEntity getExpandedSet(String iri,boolean includeLegacy) throws SQLException, ClassNotFoundException {
		return setRepository.getExpandedSet(iri,includeLegacy);
	}

	public TTEntity getExpansion(TTEntity conceptSet,boolean includeLegacy) throws SQLException {
		return setRepository.getExpansion(conceptSet,includeLegacy);

	}
	public TTEntity getSet(String iri) throws SQLException, ClassNotFoundException {
		Set<TTIriRef> predicates= new HashSet<>();
		predicates.add(IM.HAS_MEMBER);
		predicates.add(IM.HAS_SUBSET);

		TTEntity conceptSet= setRepository
			.getSet(IM.NAMESPACE+"VSET_Oral_NSAIDs");
		return conceptSet;
	}
}