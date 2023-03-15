package org.endeavourhealth.informationmanager.transforms.sources;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.endeavourhealth.imapi.model.imq.Bool;
import org.endeavourhealth.imapi.model.imq.From;
import org.endeavourhealth.imapi.model.imq.Query;
import org.endeavourhealth.imapi.model.imq.Where;
import org.endeavourhealth.imapi.model.tripletree.TTAlias;
import org.endeavourhealth.imapi.model.tripletree.TTValue;
import org.endeavourhealth.imapi.transforms.ParserErrorListener;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.parser.ecl.ECLBaseVisitor;
import org.endeavourhealth.informationmanager.parser.ecl.ECLLexer;
import org.endeavourhealth.informationmanager.parser.ecl.ECLParser;


import java.util.UnknownFormatConversionException;
import java.util.zip.DataFormatException;

/**
 * Converts ECL to Discovery syntax, supporting commonly used constructs
 */
public class ECLToIML extends ECLBaseVisitor<TTValue> {
	private final ECLLexer lexer;
	private final ECLParser parser;
	private String ecl;
	private Query query;
	public static final String ROLE_GROUP = IM.ROLE_GROUP.getIri();



	public ECLToIML() {
		this.lexer = new ECLLexer(null);
		this.parser = new ECLParser(null);
		this.parser.removeErrorListeners();
		this.parser.addErrorListener(new ParserErrorListener());
		this.query= new Query();
	}

	/**
	 * Converts an ECL string into IM Query definition class. Assumes active and inactive concepts are requested.
	 * <p>To include only active concepts use method with boolean activeOnly= true</p>
	 * @param ecl String compliant with ECL
	 * @return Class conforming to IM Query model JSON-LD when serialized.
	 * @throws DataFormatException for invalid ECL.
	 */
	public Query getQueryFromECL(String ecl) throws DataFormatException {
		return getClassExpression(ecl,false);
	}

	/**
	 * Converts an ECL string into IM Query definition class.
	 * @param ecl String compliant with ECL
	 * @param activeOnly  boolean true if limited to active concepts
	 * @return Class conforming to IM Query model JSON-LD when serialized.
	 * @throws DataFormatException for invalid ECL.
	 */
	public Query getClassExpression(String ecl,boolean activeOnly) throws DataFormatException {
		this.ecl = ecl;
		lexer.setInputStream(CharStreams.fromString(ecl));
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		parser.setTokenStream(tokens);
		try {
			ECLParser.EclContext eclCtx = parser.ecl();

		query= new Query();
		From from= new From();
		query.setFrom(from);
		convertECContext(eclCtx,from);
		if (activeOnly)
			query.setActiveOnly(true);

		return query;
		}
		catch (UnknownFormatConversionException e) {
			throw new DataFormatException(e.getMessage());
		}

	}
	private void convertECContext(ECLParser.EclContext ctx,From from) throws DataFormatException {
		convertECContext(ctx.expressionconstraint(),from);
	}

	private void convertECContext(ECLParser.ExpressionconstraintContext ctx,From from) throws DataFormatException {
		if (ctx.subexpressionconstraint() != null) {
			convertSubECContext(ctx.subexpressionconstraint(),from);
		} else if (ctx.compoundexpressionconstraint() != null) {
			if (ctx.compoundexpressionconstraint().disjunctionexpressionconstraint() != null) {

				convertDisjunction(ctx.compoundexpressionconstraint().disjunctionexpressionconstraint(),from);

			} else if (ctx.compoundexpressionconstraint().exclusionexpressionconstraint() != null) {
				convertExclusion(ctx.compoundexpressionconstraint().exclusionexpressionconstraint(),from);

			} else if (ctx.compoundexpressionconstraint().conjunctionexpressionconstraint() != null) {
				convertConjunction(ctx.compoundexpressionconstraint().conjunctionexpressionconstraint(),from);

			} else {
				throw new UnknownFormatConversionException("Unknown ECL format " + ecl);
			}

		} else if (ctx.refinedexpressionconstraint() != null) {
			 convertRefined(ctx.refinedexpressionconstraint(),from);
		} else {
			throw new UnknownFormatConversionException(("unknown ECL layout " + ecl));
		}
	}

	private void convertSubECContext(ECLParser.SubexpressionconstraintContext eclSub,From from) throws DataFormatException {
		if (eclSub.expressionconstraint() != null) {
			convertECContext(eclSub.expressionconstraint(),from);
		} else {
			if (eclSub.eclfocusconcept() != null) {
				setFrom(from,eclSub);
			} else {
				throw new UnknownFormatConversionException("Unrecognised ECL subexpression constraint " + ecl);

			}
		}
	}


	private void setFrom(TTAlias from, ECLParser.SubexpressionconstraintContext eclSub) {
		boolean includeSubs=false;
		boolean excludeSelf= false;
		if (eclSub.constraintoperator()!=null) {
			if (eclSub.constraintoperator().descendantorselfof() != null)
				includeSubs = true;
			else if (eclSub.constraintoperator().descendantof() != null) {
				includeSubs = true;
				excludeSelf = true;
			}
		}
		setFrom(from,eclSub,includeSubs,excludeSelf);

	}

	private void setFrom(TTAlias from, ECLParser.SubexpressionconstraintContext eclSub,boolean includeSubs,boolean excludeSelf) {
		String concept= eclSub.eclfocusconcept().eclconceptreference().conceptid().getText();
		String name=null;
		if (eclSub.eclfocusconcept().eclconceptreference().term()!=null){
			name= eclSub.eclfocusconcept().eclconceptreference().term().getText();
		}
		String conceptIri;
		if (concept.matches("[0-9]+")) {
			conceptIri = concept.contains("1000252") ? IM.NAMESPACE + concept : SNOMED.NAMESPACE + concept;
		}
		else
			conceptIri= concept;
		if (excludeSelf) {
			from
				.setIri(conceptIri).setDescendantsOf(true);
			if (name!=null)
				from.setName(name);
		}
		else if (includeSubs){
			from
				.setIri(conceptIri).setDescendantsOrSelfOf(true);
			if (name!=null)
				from.setName(name);
		}
		else {
			from
				.setIri(conceptIri);
			if (name!=null)
				from.setName(name);
		}

	}


	private boolean isWildCard(ECLParser.RefinedexpressionconstraintContext refined){
		return refined.subexpressionconstraint() != null &&
			refined.subexpressionconstraint().eclfocusconcept() != null &&
			refined.subexpressionconstraint().eclfocusconcept().wildcard() != null;
	}

	private void convertRefined(ECLParser.RefinedexpressionconstraintContext refined,From from) throws DataFormatException {
		if (!isWildCard(refined)) {
			if (refined.subexpressionconstraint().expressionconstraint() != null) {
				convertECContext(refined.subexpressionconstraint().expressionconstraint(), from);
			}
			else {
				convertSubECContext(refined.subexpressionconstraint(), from);
			}
		}


		ECLParser.EclrefinementContext refinement = refined.eclrefinement();
		if (refinement.disjunctionrefinementset() != null) {
			convertOrRefinement(from, refinement);
		}
		else if (refinement.conjunctionrefinementset() != null) {
			convertAndRefinement(from, refinement);
		}
		else {
			convertSingleRefinement(from, refinement);
		}
	}

	private void convertAndRefinement(From from, ECLParser.EclrefinementContext refinement) throws DataFormatException {
			Where and= new Where();
			from.addWhere(and);
			ECLParser.SubrefinementContext subref = refinement.subrefinement();
			if (subref.eclattributeset() != null) {
				convertAttributeSet(and,subref.eclattributeset());
			}
		else if (subref.eclattributegroup()!=null){
			convertAttributeGroup(and,subref.eclattributegroup());
		}
		for (ECLParser.SubrefinementContext subOrRef : refinement.conjunctionrefinementset().subrefinement()) {
			Where pv = new Where();
			from.addWhere(pv);
			if (subOrRef.eclattributeset() != null) {
				convertAttributeSet(pv, subOrRef.eclattributeset());
			}
			else if (subOrRef.eclattributegroup() != null) {
				convertAttributeGroup(pv, subOrRef.eclattributegroup());
			}
		}
	}

	private void convertOrRefinement(From from, ECLParser.EclrefinementContext refinement) throws DataFormatException {
		Where or= new Where();
		from.addWhere(or);
		or.setBool(Bool.or);
		Where firstOr= new Where();
		or.addWhere(firstOr);
		ECLParser.SubrefinementContext subref = refinement.subrefinement();
		if (subref.eclattributeset() != null) {
			convertAttributeSet(or,subref.eclattributeset());
		}
		else if (subref.eclattributegroup()!=null){
			convertAttributeGroup(or,subref.eclattributegroup());
		}
		for (ECLParser.SubrefinementContext subOrRef : refinement.disjunctionrefinementset().subrefinement()) {
			Where pv = new Where();
			or.addWhere(pv);
			if (subOrRef.eclattributeset() != null) {
				convertAttributeSet(pv, subOrRef.eclattributeset());
			}
			else if (subOrRef.eclattributegroup() != null) {
				convertAttributeGroup(pv, subOrRef.eclattributegroup());
			}
		}
	}


	private void convertSingleRefinement(From from, ECLParser.EclrefinementContext refinement) throws DataFormatException {
		Where where= new Where();
		from.addWhere(where);
		ECLParser.SubrefinementContext subref = refinement.subrefinement();
		if (subref.eclattributeset() != null) {
			convertAttributeSet(where,subref.eclattributeset());
		}
		else if (subref.eclattributegroup()!=null){
			convertAttributeGroup(where,subref.eclattributegroup());
		}
	}



	private void convertAttributeSet(Where path, ECLParser.EclattributesetContext eclAtSet) throws DataFormatException {
		if (eclAtSet.conjunctionattributeset() == null && eclAtSet.disjunctionattributeset() == null) {
				path.setAnyRoleGroup(true);
				convertAttribute(path, eclAtSet.subattributeset().eclattribute());
			}
		else if (eclAtSet.conjunctionattributeset() != null) {
			convertAndSet(path, eclAtSet);
		}
		else {
			convertOrSet(path, eclAtSet);
		}
	}

	private void convertAndSet(Where where, ECLParser.EclattributesetContext eclAtSet) throws DataFormatException {
			  where.setBool(Bool.and);
				Where and = new Where();
				and.setAnyRoleGroup(true);
				where.addWhere(and);
				convertAttribute(and, eclAtSet.subattributeset().eclattribute());

		for (ECLParser.SubattributesetContext subAt : eclAtSet.conjunctionattributeset().subattributeset()) {
				and= new Where();
				and.setAnyRoleGroup(true);
				where.addWhere(and);
				convertAttribute(and, subAt.eclattribute());
			}
	}

	private void convertOrSet(Where where, ECLParser.EclattributesetContext eclAtSet) throws DataFormatException {
		where.setBool(Bool.or);
		Where or = new Where();
		or.setAnyRoleGroup(true);
		where.addWhere(or);
		convertAttribute(or, eclAtSet.subattributeset().eclattribute());

		for (ECLParser.SubattributesetContext subAt : eclAtSet.disjunctionattributeset().subattributeset()) {
			or= new Where();
			or.setAnyRoleGroup(true);
			where.addWhere(or);
			convertAttribute(or, subAt.eclattribute());
		}
	}

	private void convertConjunction(ECLParser.ConjunctionexpressionconstraintContext eclAnd,From from) throws DataFormatException {
		from.setBoolFrom(Bool.and);
		for (ECLParser.SubexpressionconstraintContext eclInter : eclAnd.subexpressionconstraint()) {
			From and= new From();
			from.addFrom(and);
			convertSubECContext(eclInter,and);
		}
	}

	private void convertExclusion(ECLParser.ExclusionexpressionconstraintContext eclExc,From from) throws DataFormatException {
		from.setBoolFrom(Bool.and);
		From first= new From();
		from.addFrom(first);
		convertSubECContext(eclExc.subexpressionconstraint().get(0),first);
		From notFrom= new From();
		from.addFrom(notFrom);
		notFrom.setExclude(true);
		From notExist= new From();
		notFrom.addFrom(notExist);
		convertSubECContext(eclExc.subexpressionconstraint().get(1),notExist);
	}

	private void convertDisjunction(ECLParser.DisjunctionexpressionconstraintContext eclOr,From from) throws DataFormatException {
		from.setBoolFrom(Bool.or);
		for (ECLParser.SubexpressionconstraintContext eclUnion : eclOr.subexpressionconstraint()) {
			From or= new From();
			from.addFrom(or);
			convertSubECContext(eclUnion,or);
		}
	}

	private void convertAttribute(Where where, ECLParser.EclattributeContext attecl) throws DataFormatException {

		ECLParser.EclconceptreferenceContext eclRef= attecl.eclattributename().subexpressionconstraint().eclfocusconcept().eclconceptreference();
		ECLParser.ConstraintoperatorContext entail= attecl.eclattributename().subexpressionconstraint().constraintoperator();
		conRef(eclRef,entail, where);
		if (attecl.expressioncomparisonoperator() != null) {
			if (attecl.expressioncomparisonoperator().EQUALS() != null) {
				if (attecl.subexpressionconstraint().eclfocusconcept() != null) {
					where.addIn(getValue(attecl
						.subexpressionconstraint().eclfocusconcept().eclconceptreference(),
						attecl.subexpressionconstraint().constraintoperator()));
				} else {
					throw new UnknownFormatConversionException("multi nested ECL not yest supported " + ecl);
				}
			} else {
				throw new UnknownFormatConversionException("unknown comparison type operator " + ecl);
			}
		} else {
			throw new UnknownFormatConversionException("unrecognised comparison operator " + ecl);
		}
	}




	private From getValue(ECLParser.EclconceptreferenceContext eclRef,ECLParser.ConstraintoperatorContext entail ) throws DataFormatException {
		From conRef = new From();
		conRef(eclRef, entail, conRef);
		return conRef;
	}

	private void conRef(ECLParser.EclconceptreferenceContext eclRef,
											ECLParser.ConstraintoperatorContext entail, TTAlias conRef) throws DataFormatException {

	String name= null;
		if (eclRef.term()!=null)
			name= eclRef.term().getText();
		ECLParser.ConceptidContext conceptId= eclRef.conceptid();
		String code=conceptId.getText();
		if (code.matches("[0-9]+")) {
			if (code.contains("1000252"))
				conRef.setIri(IM.NAMESPACE + code);
			else
				conRef.setIri(SNOMED.NAMESPACE + code);
		} else
			throw new DataFormatException("ECL converter can only be used for snomed codes at this stage");
		if (entail!=null) {
			if (entail.descendantorselfof() != null)
				conRef.setDescendantsOrSelfOf(true);
			else if (entail.descendantof() != null) {
				conRef.setDescendantsOf(true);
			}
		}
		if (name!=null)
			conRef.setName(name);
	}


	private void convertAttributeGroup(Where group,
																		 ECLParser.EclattributegroupContext eclGroup) throws DataFormatException {
		group.setIri(IM.ROLE_GROUP.getIri());
		if (eclGroup.eclattributeset()!=null) {
			convertGroupedAttributeSet(group, eclGroup.eclattributeset());
		}
		else
			throw new DataFormatException("Unable to cope with this type of attribute group : "+ ecl);
	}



	private void convertGroupedAttributeSet(Where path, ECLParser.EclattributesetContext eclAtSet) throws DataFormatException {
		if (eclAtSet.conjunctionattributeset() == null && eclAtSet.disjunctionattributeset() == null) {
			Where where = new Where();
			path.addWhere(where);
			convertAttribute(where, eclAtSet.subattributeset().eclattribute());
		}
		else if (eclAtSet.conjunctionattributeset() != null) {
			convertGroupedAndSet(path, eclAtSet);
		}
		else {
			convertGroupedOrSet(path, eclAtSet);
		}
	}

	private void convertGroupedAndSet(Where path, ECLParser.EclattributesetContext eclAtSet) throws DataFormatException {
		path.setBool(Bool.and);
		Where where = new Where();
		path.addWhere(where);
		convertAttribute(where, eclAtSet.subattributeset().eclattribute());

		for (ECLParser.SubattributesetContext subAt : eclAtSet.conjunctionattributeset().subattributeset()) {
			where = new Where();
			path.addWhere(where);
			convertAttribute(where, subAt.eclattribute());
		}
	}

	private void convertGroupedOrSet(Where path, ECLParser.EclattributesetContext eclAtSet) throws DataFormatException {
		path.setBool(Bool.or);
		Where where = new Where();
		path.addWhere(where);
		convertAttribute(where, eclAtSet.subattributeset().eclattribute());

		for (ECLParser.SubattributesetContext subAt : eclAtSet.disjunctionattributeset().subattributeset()) {
			where = new Where();
			path.addWhere(where);
			convertAttribute(where, subAt.eclattribute());
		}
	}

}
