package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

class SetServiceTest {

	@Test
	void getSet() throws SQLException, ClassNotFoundException {
		SetService exporter= new SetService();
		TTEntity nsaids= exporter.getSet(IM.NAMESPACE+"VSET_OralNSAIDs");
		System.out.println(nsaids.getIri());
	}
	@Test
	void getExpandedSet() throws SQLException, ClassNotFoundException {
		SetService exporter= new SetService();
		TTEntity nsaids= exporter.getExpandedSet(IM.NAMESPACE+"VSET_OralNSAIDs",true);
		//TTEntity nsaids= exporter.getExpandedSet(IM.NAMESPACE+"VSET_BartsCVSSMeds",true);
		//for (TTValue memberNode:nsaids.get(IM.HAS_MEMBER).asArray().getElements()){
			//TTEntity member = (TTEntity) memberNode.asNode();
			//System.out.println((member.getName()));
		//}
		System.out.println("Expansion has "+ nsaids.get(IM.HAS_MEMBER).asArray().size()+" members");

	}


}