package org.endeavourhealth.informationmanager.common.transform;

import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.OWL;
import org.endeavourhealth.imapi.vocabulary.RDF;

import java.util.Map;
import java.util.zip.DataFormatException;

public class TTToECL {
	public String getConceptSetECL(TTEntity entity, TTDocument document) throws DataFormatException {
			StringBuilder ecl = new StringBuilder();
			if (entity.get(IM.HAS_MEMBER) != null) {
				boolean first = true;
				for (TTValue member : entity.get(IM.HAS_MEMBER).asArray().getElements()) {
					if (!first)
						ecl.append(" OR ");
					first = false;
					if (member.isIriRef()) {
						checkMember(member.asIriRef().getIri());
						ecl.append("<<" + member.asIriRef().getIri().split("#")[1]);
					} else {
						ecl.append("(");
						TTNode expression = member.asNode();
						if (expression.get(OWL.INTERSECTIONOF) != null) {
							for (TTValue inter : expression.get(OWL.INTERSECTIONOF).asArray().getElements()) {
								if (inter.isIriRef()) {
									checkMember(inter.asIriRef().getIri());
									ecl.append("<<" + inter.asIriRef().getIri().split("#")[1]);
								} else {
									for (Map.Entry<TTIriRef, TTValue> entry : inter.asNode().getPredicateMap().entrySet()) {
										checkMember(entry.getKey().getIri());
										checkMember(entry.getValue().asIriRef().getIri());
										ecl.append(" : <<" + entry.getKey().getIri().split("#")[1]);
										ecl.append(" = <<" + entry.getValue().asIriRef().getIri().split("#")[1]);
									}
								}
							}
							ecl.append(")");
						}

					}

				}
				return ecl.toString();
			}
		return null;
	}

	private void checkMember(String iri) throws DataFormatException {
		if (!iri.contains("/sct#")&(!iri.contains("/im#")))
			throw new DataFormatException("ECL converter supports snomed and discovery codes only");
	}
}

