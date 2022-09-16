package org.endeavourhealth.informationmanager.transforms.sources;


import org.endeavourhealth.imapi.model.iml.Query;
import org.endeavourhealth.imapi.model.iml.Select;
import org.endeavourhealth.imapi.model.iml.Where;
import org.endeavourhealth.imapi.model.tripletree.TTAlias;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.informationmanager.transforms.sources.eqd.*;

import java.io.IOException;
import java.util.zip.DataFormatException;

public class EqdListToIMQ {
	private EqdResources resources;

	public void convertReport(EQDOCReport eqReport,Query query,EqdResources resources) throws DataFormatException, IOException {
		this.resources= resources;
		this.resources.setQuery(query);
		String id = eqReport.getParent().getSearchIdentifier().getReportGuid();
		resources.setWith(query, TTIriRef.iri("urn:uuid:" + id).setName(resources.reportNames.get(id)));
		for (EQDOCListReport.ColumnGroups eqColGroups : eqReport.getListReport().getColumnGroups()) {
			EQDOCListColumnGroup eqColGroup = eqColGroups.getColumnGroup();
			Query subQuery = new Query();
			query.addSubQuery(subQuery);
			convertListGroup(eqColGroup, subQuery);
		}
	}


	private void convertListGroup(EQDOCListColumnGroup eqColGroup, Query subQuery) throws DataFormatException, IOException {
		String eqTable = eqColGroup.getLogicalTableName();

		if (eqColGroup.getCriteria() == null) {
			convertPatientColumns(eqColGroup, eqTable, subQuery);
		} else {
			convertEventColumns(eqColGroup, eqTable, subQuery);
		}
	}

	private void convertPatientColumns(EQDOCListColumnGroup eqColGroup, String eqTable, Query subQuery) throws DataFormatException {
		EQDOCListColumns eqCols = eqColGroup.getColumnar();

		for (EQDOCListColumn eqCol : eqCols.getListColumn()) {
			Select select= new Select();
			subQuery.addSelect(select);
			String eqColumn= String.join("/",eqCol.getColumn());
			TTAlias property = resources.getPath(eqTable + "/" + eqColumn);
			select.setProperty(property);
		}

	}

	private void convertEventColumns(EQDOCListColumnGroup eqColGroup, String eqTable, Query subQuery) throws DataFormatException, IOException {
		Where match= new Where();
		subQuery.setWhere(match);
		resources.convertCriteria(eqColGroup.getCriteria(), match);
		Select select = new Select();
		subQuery.addSelect(select);
		select.setProperty(resources.getPath(eqTable));
		EQDOCListColumns eqCols = eqColGroup.getColumnar();
		for (EQDOCListColumn eqCol : eqCols.getListColumn()) {
			String eqColumn = String.join("/", eqCol.getColumn());
			TTAlias predicatePath = resources.getPath(eqTable + "/" + eqColumn);
			select.select(s->s
				.setProperty(predicatePath));
		}
	}

}
