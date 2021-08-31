package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

class SetServiceTest {


	@Test
	void getExpandedSet() throws SQLException, ClassNotFoundException {
		SetService exporter= new SetService();
		TTEntity ethnics= exporter.getSetDefinition("http://endhealth.info/im#VSET_EthnicCategory2001");

		//TTEntity nsaids= exporter.getIM1Expansion(IM.NAMESPACE+"VSET_OralNSAIDs");
		//TTEntity nsaids= exporter.getExpansion(IM.NAMESPACE+"VSET_OralNSAIDs");
		//TTEntity nsaids= exporter.getLegacyExpansion(IM.NAMESPACE+"VSET_OralNSAIDs");
		//TTEntity nsaids= exporter.getExpansion(IM.NAMESPACE+"VSET_BartsCVSSMeds");
		//for (TTValue memberNode:nsaids.get(IM.HAS_MEMBER).asArray().getElements()){
			//TTEntity member = (TTEntity) memberNode.asNode();
			//System.out.println((member.getName()));
		//}
		System.out.println("Set :"+ ethnics.getIri());
		//System.out.println("Expansion has "+ nsaids.get(IM.HAS_MEMBER).asArray().size()+" members");

	}


}