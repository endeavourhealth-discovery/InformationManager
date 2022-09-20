package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.model.iml.Where;
import org.endeavourhealth.imapi.model.iml.Query;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.transforms.sources.eqd.*;

import java.io.IOException;
import java.util.List;
import java.util.zip.DataFormatException;

public class EqdPopToIMQ {

	private EqdResources resources;
	private String activeReport;



	public void convertPopulation(EQDOCReport eqReport, Query query,EqdResources resources) throws DataFormatException, IOException {
		this.activeReport= eqReport.getId();
		this.resources= resources;
		if (eqReport.getParent().getParentType() == VocPopulationParentType.ACTIVE) {
			resources.setWith(query, TTIriRef.iri(IM.NAMESPACE + "Q_RegisteredGMS").setName("Registered with GP for GMS services on the reference date"));
		}
		else {
			if (eqReport.getParent().getParentType() == VocPopulationParentType.POP) {
				String id = eqReport.getParent().getSearchIdentifier().getReportGuid();
				resources.setWith(query, TTIriRef.iri("urn:uuid:" + id).setName(resources.reportNames.get(id)));
			}
		}
		Where mainWhere= new Where();
		query.setWhere(mainWhere);
		boolean lastOr= false;

		for (EQDOCCriteriaGroup eqGroup : eqReport.getPopulation().getCriteriaGroup()) {
			VocRuleAction ifTrue = eqGroup.getActionIfTrue();
			VocRuleAction ifFalse = eqGroup.getActionIfFalse();

			if (ifTrue == VocRuleAction.SELECT && ifFalse == VocRuleAction.NEXT) {
					Where or= new Where();
					mainWhere.addOr(or);
					convertGroup(eqGroup,or);
					lastOr= true;
			}
			else if (ifTrue == VocRuleAction.SELECT && ifFalse == VocRuleAction.REJECT) {
				if (lastOr) {
					Where or = new Where();
					mainWhere.addOr(or);
					convertGroup(eqGroup, or);
					lastOr = false;
				} else {
					Where and = new Where();
					mainWhere.addAnd(and);
					convertGroup(eqGroup, and);
				}
			}
			else if (ifTrue == VocRuleAction.NEXT && ifFalse == VocRuleAction.REJECT) {
				if (lastOr) {
					Where or= new Where();
					mainWhere.addOr(or);
					convertGroup(eqGroup, or);
					lastOr = false;
				}
				else {
					Where and = new Where();
					mainWhere.addAnd(and);
					convertGroup(eqGroup, and);
				}
			}
			else if (ifTrue == VocRuleAction.REJECT && ifFalse == VocRuleAction.SELECT||
				(ifTrue == VocRuleAction.REJECT && ifFalse == VocRuleAction.NEXT) ) {
				if (mainWhere.getNotExist()==null){
					Where not= new Where();
					mainWhere.setNotExist(not);
				}
				Where not= mainWhere.getNotExist();
					Where notOr= new Where();
					not.addOr(notOr);
					convertGroup(eqGroup,notOr);
				}
			else
				throw new DataFormatException("unrecognised action rule combination : " + activeReport);
		}
		flatten(query);
	}

	private void convertGroup(EQDOCCriteriaGroup eqGroup, Where topWhere) throws DataFormatException, IOException {
		if (eqGroup.getId().contains("990f1c0b-1676-45f0-b321-d2e710dd7336"))
			System.out.println("");
		VocMemberOperator memberOp = eqGroup.getDefinition().getMemberOperator();
		if (eqGroup.getDefinition().getCriteria().size()==1){
			resources.convertCriteria(eqGroup.getDefinition().getCriteria().get(0),topWhere);
		}
		else {
			for (EQDOCCriteria eqCriteria : eqGroup.getDefinition().getCriteria()) {
				Where where = new Where();
				if (memberOp == VocMemberOperator.OR) {
					topWhere.addOr(where);
				} else
					topWhere.addAnd(where);
				resources.convertCriteria(eqCriteria, where);
			}
		}
	}

	private void flatten(Query query) {
		Where flatWhere = new Where();
		Where oldWhere = query.getWhere();
		if (oldWhere.getProperty()!=null){
			return;
		}
		if (oldWhere.getAnd()!=null){
			for (Where oldAnd:oldWhere.getAnd()){
				flattenAnds(flatWhere,oldAnd);
			}
		}
		if (oldWhere.getOr()!=null){
			for (Where oldOr:oldWhere.getOr()) {
				Where flatOr= new Where();
				flattenOrs(flatOr,oldOr);
				flatWhere.addOr(flatOr);
			}
		}
		query.setWhere(flatWhere);
	}

	private void flattenOrs(Where flatWhere, Where oldWhere) {
		if (oldWhere.getProperty()!=null||oldWhere.getFrom()!=null||oldWhere.getPath()!=null){
			flatWhere.addOr(oldWhere);
		}
		else if (oldWhere.getNotExist() != null) {
			flatWhere.addOr(oldWhere);
		}
		else if (oldWhere.getAnd() != null) {
			for (Where oldAnd : oldWhere.getAnd()) {
				flattenAnds(flatWhere, oldAnd);
			}
		}
		else {
			for (Where oldOr:oldWhere.getOr())
				flattenOrs(oldWhere,oldOr);
		}
	}

	private void flattenAnds(Where flatWhere,Where oldWhere){
		if (oldWhere.getProperty()!=null||oldWhere.getFrom()!=null||oldWhere.getPath()!=null){
			flatWhere.addAnd(oldWhere);
		}
		else if (oldWhere.getNotExist() != null) {
			flatWhere.addAnd(oldWhere);
		}
		else if (oldWhere.getAnd() != null) {
				for (Where oldAnd : oldWhere.getAnd()) {
					flattenAnds(flatWhere, oldAnd);
				}
		} else {
			for (Where oldOr:oldWhere.getOr())
				flatWhere.addOr(oldOr);
			}


		}

}
