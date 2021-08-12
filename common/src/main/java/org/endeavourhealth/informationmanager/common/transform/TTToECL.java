package org.endeavourhealth.informationmanager.common.transform;

import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTNode;
import org.endeavourhealth.imapi.model.tripletree.TTValue;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.OWL;
import org.endeavourhealth.imapi.vocabulary.RDF;

public class TTToECL {
	public String getConceptSetECL(TTEntity entity, TTDocument document){
		StringBuilder ecl= new StringBuilder();
		if (entity.get(IM.HAS_MEMBER)!=null){
			boolean first=true;
			for (TTValue member:entity.get(IM.HAS_MEMBER).asArray().getElements()){
				if (!first)
					ecl.append(" OR ");
				first=false;
				if (member.isIriRef()){
						ecl.append("<<"+ member.asIriRef().getIri().split("#")[1]);
				} else {
					ecl.append("(");
					TTNode expression=member.asNode();
					if (expression.get(OWL.INTERSECTIONOF)!=null) {
						for (TTValue inter : expression.get(OWL.INTERSECTIONOF).asArray().getElements()) {
							if (inter.isIriRef())
								ecl.append("<<" + inter.asIriRef().getIri().split("#")[1]);
							else if (inter.asNode().get(RDF.TYPE).equals(OWL.RESTRICTION)) {
								ecl.append(" : <<"+inter.asNode()
									.get(OWL.ONPROPERTY)
									.asIriRef()
									.getIri().split("#")[1]);
								ecl.append(" = <<"+ inter.asNode()
									.get(OWL.SOMEVALUESFROM)
									.asIriRef()
									.getIri().split("#")[1]);
							}
						}
					}
					ecl.append(")");
				}

			}
		}
		return ecl.toString();
	}
}
