package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.model.iml.Query;
import org.endeavourhealth.imapi.model.iml.Select;
import org.endeavourhealth.imapi.model.tripletree.TTAlias;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.transforms.sources.eqd.*;

import java.util.zip.DataFormatException;

public class EqdAuditToIMQ {
	private EqdResources resources;

	public void convertReport(EQDOCReport eqReport, Query query,EqdResources resources) throws DataFormatException {
		this.resources= resources;

		if (eqReport.getParent().getSearchIdentifier()!=null) {
			String id = eqReport.getParent().getSearchIdentifier().getReportGuid();
			resources.setWith(query, TTIriRef.iri("urn:uuid:" + id).setName(resources.reportNames.get(id)));
		}
		else
			resources.setWith(query,TTIriRef.iri(IM.NAMESPACE+"Patient","Patient"));
		for (String popId : eqReport.getAuditReport().getPopulation()) {
			Query subQuery = new Query();
			query.addSubQuery(subQuery);
			resources.setWith(subQuery, TTIriRef.iri("urn:uuid:" + popId).setName(resources.reportNames.get(popId)));
		}
		EQDOCAggregateReport agg = eqReport.getAuditReport().getCustomAggregate();
		Select select = new Select();
		query.addSelect(select);

		String eqTable = agg.getLogicalTable();
		for (
			EQDOCAggregateGroup group : agg.getGroup()) {
			for (String eqColum : group.getGroupingColumn()) {
				String predicate = resources.getPath(eqTable + "/" + eqColum);
				select.addGroupBy(new TTAlias().setIri(predicate));
			}
		}
	}

}
