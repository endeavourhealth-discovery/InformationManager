package org.endeavourhealth.informationmanager.common;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.endeavourhealth.imapi.model.imq.*;

import org.endeavourhealth.imapi.model.tripletree.TTValue;
import org.endeavourhealth.imapi.transforms.ParserErrorListener;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.common.parser.ecl.ECLBaseVisitor;
import org.endeavourhealth.informationmanager.common.parser.ecl.ECLLexer;
import org.endeavourhealth.informationmanager.common.parser.ecl.ECLParser;


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
	public static final String ROLE_GROUP = IM.ROLE_GROUP;



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
		Match from= new Match();
		query.addMatch(from);
		convertECContext(eclCtx,from);
		if (activeOnly)
			query.setActiveOnly(true);

		return query;
		}
		catch (UnknownFormatConversionException e) {
			throw new DataFormatException(e.getMessage());
		}

	}
	private void convertECContext(ECLParser.EclContext ctx,Match from) throws DataFormatException {
		convertECContext(ctx.expressionconstraint(),from);
	}

	private void convertECContext(ECLParser.ExpressionconstraintContext ctx,Match from) throws DataFormatException {
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

	private void convertSubECContext(ECLParser.SubexpressionconstraintContext eclSub,Match from) throws DataFormatException {
		if (eclSub.expressionconstraint() != null) {
			convertECContext(eclSub.expressionconstraint(),from);
		} else {
			if (eclSub.eclfocusconcept() != null) {
				setMatch(from,eclSub);
			} else {
				throw new UnknownFormatConversionException("Unrecognised ECL subexpression constraint " + ecl);

			}
		}
	}


	private void setMatch(Match from, ECLParser.SubexpressionconstraintContext eclSub) {
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
		setMatch(from,eclSub,includeSubs,excludeSelf);

	}

	private void setMatch(Match from, ECLParser.SubexpressionconstraintContext eclSub,boolean includeSubs,boolean excludeSelf) {
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
				.setInstanceOf(new Node().setIri(conceptIri).setDescendantsOf(true)
				.setName(name));
		}
		else if (includeSubs){
			from
				.setInstanceOf( new Node().setIri(conceptIri).setDescendantsOrSelfOf(true)
				.setName(name));
		}
		else {
			from
				.setInstanceOf(new Node().setIri(conceptIri).setName(name));
		}

	}


	private boolean isWildCard(ECLParser.RefinedexpressionconstraintContext refined){
		return refined.subexpressionconstraint() != null &&
			refined.subexpressionconstraint().eclfocusconcept() != null &&
			refined.subexpressionconstraint().eclfocusconcept().wildcard() != null;
	}

	private void convertRefined(ECLParser.RefinedexpressionconstraintContext refined,Match from) throws DataFormatException {
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

	private void convertAndRefinement(Match from, ECLParser.EclrefinementContext refinement) throws DataFormatException {
		from.setBool(Bool.and);
		ECLParser.SubrefinementContext subref = refinement.subrefinement();
		if (subref.eclattributeset() != null) {
				convertAttributeSet(from,subref.eclattributeset(),true);
			}
		else if (subref.eclattributegroup()!=null){
			Property where= new Property();
			from.addProperty(where);
			where.setIri(IM.NAMESPACE+"roleGroup");
			Match group= new Match();
			where.setMatch(group);
			convertAttributeGroup(group,subref.eclattributegroup());
		}
		for (ECLParser.SubrefinementContext subAndRef : refinement.conjunctionrefinementset().subrefinement()) {
			if (subAndRef.eclattributeset() != null) {
				convertAttributeSet(from, subAndRef.eclattributeset(),true);
			}
			else if (subAndRef.eclattributegroup() != null) {
				Property where= new Property();
				from.addProperty(where);
				where.setIri(IM.NAMESPACE+"roleGroup");
				Match group= new Match();
				where.setMatch(group);
				convertAttributeGroup(group,subAndRef.eclattributegroup());
			}
		}
	}

	private void convertOrRefinement(Match from, ECLParser.EclrefinementContext refinement) throws DataFormatException {
		from.setBool(Bool.or);
		ECLParser.SubrefinementContext subref = refinement.subrefinement();
		if (subref.eclattributeset() != null) {
			convertAttributeSet(from,subref.eclattributeset(),true);
		}
		else if (subref.eclattributegroup()!=null){
			Property where= new Property();
			from.addProperty(where);
			where.setIri(IM.NAMESPACE+"roleGroup");
			Match group= new Match();
			where.setMatch(group);
			convertAttributeGroup(group,subref.eclattributegroup());
		}
		for (ECLParser.SubrefinementContext subOrRef : refinement.disjunctionrefinementset().subrefinement()) {
			if (subOrRef.eclattributeset() != null) {
				convertAttributeSet(from, subOrRef.eclattributeset(),true);
			}
			else if (subOrRef.eclattributegroup() != null) {
				Property where= new Property();
				from.addProperty(where);
				where.setIri(IM.NAMESPACE+"roleGroup");
				Match group= new Match();
				where.setMatch(group);
				convertAttributeGroup(group,subOrRef.eclattributegroup());
			}
		}
	}


	private void convertSingleRefinement(Match from, ECLParser.EclrefinementContext refinement) throws DataFormatException {
		ECLParser.SubrefinementContext subref = refinement.subrefinement();
		if (subref.eclattributeset() != null) {
			convertAttributeSet(from,subref.eclattributeset(),true);
		}
		else if (subref.eclattributegroup()!=null){
			Property where= new Property();
			from.addProperty(where);
			where.setIri(IM.NAMESPACE+"roleGroup");
			Match group= new Match();
			where.setMatch(group);
			convertAttributeGroup(group,subref.eclattributegroup());
		}
	}



	private void convertAttributeSet(Match match, ECLParser.EclattributesetContext eclAtSet,boolean anyGroup) throws DataFormatException {
		if (eclAtSet.conjunctionattributeset() == null && eclAtSet.disjunctionattributeset() == null) {
			Property where= new Property();
			match.addProperty(where);
			if (anyGroup)
				where.setAnyRoleGroup(true);
			convertAttribute(where, eclAtSet.subattributeset().eclattribute());
			}
		else if (eclAtSet.conjunctionattributeset() != null) {
			convertAndSet(match, eclAtSet,anyGroup);
		}
		else {
			convertOrSet(match, eclAtSet,anyGroup);
		}
	}

	private void convertAndSet(Match match, ECLParser.EclattributesetContext eclAtSet,boolean anyGroup) throws DataFormatException {
			  match.setBool(Bool.and);
				Property and = new Property();
				if (anyGroup)
					and.setAnyRoleGroup(true);
				match.addProperty(and);
				convertAttribute(and, eclAtSet.subattributeset().eclattribute());

		for (ECLParser.SubattributesetContext subAt : eclAtSet.conjunctionattributeset().subattributeset()) {
				and= new Property();
				if (anyGroup)
					and.setAnyRoleGroup(true);
				match.addProperty(and);
				convertAttribute(and, subAt.eclattribute());
			}
	}

	private void convertOrSet(Match match, ECLParser.EclattributesetContext eclAtSet,boolean anyGroup) throws DataFormatException {
		match.setBool(Bool.or);
		Property or = new Property();
		if (anyGroup)
			or.setAnyRoleGroup(true);
		match.addProperty(or);
		convertAttribute(or, eclAtSet.subattributeset().eclattribute());

		for (ECLParser.SubattributesetContext subAt : eclAtSet.disjunctionattributeset().subattributeset()) {
			or= new Property();
			if (anyGroup)
				or.setAnyRoleGroup(true);
			match.addProperty(or);
			convertAttribute(or, subAt.eclattribute());
		}
	}

	private void convertConjunction(ECLParser.ConjunctionexpressionconstraintContext eclAnd,Match from) throws DataFormatException {
		from.setBool(Bool.and);
		for (ECLParser.SubexpressionconstraintContext eclInter : eclAnd.subexpressionconstraint()) {
			Match and= new Match();
			from.addMatch(and);
			convertSubECContext(eclInter,and);
		}
	}

	private void convertExclusion(ECLParser.ExclusionexpressionconstraintContext eclExc,Match from) throws DataFormatException {
		from.setBool(Bool.and);
		Match first= new Match();
		from.addMatch(first);
		convertSubECContext(eclExc.subexpressionconstraint().get(0),first);
		Match notMatch= new Match();
		from.addMatch(notMatch);
		notMatch.setExclude(true);
		Match notExist= new Match();
		notMatch.addMatch(notExist);
		convertSubECContext(eclExc.subexpressionconstraint().get(1),notExist);
	}

	private void convertDisjunction(ECLParser.DisjunctionexpressionconstraintContext eclOr,Match from) throws DataFormatException {
		from.setBool(Bool.or);
		for (ECLParser.SubexpressionconstraintContext eclUnion : eclOr.subexpressionconstraint()) {
			Match or= new Match();
			from.addMatch(or);
			convertSubECContext(eclUnion,or);
		}
	}

	private void convertAttribute(Property where, ECLParser.EclattributeContext attecl) throws DataFormatException {

		ECLParser.EclconceptreferenceContext eclRef= attecl.eclattributename().subexpressionconstraint().eclfocusconcept().eclconceptreference();
		ECLParser.ConstraintoperatorContext entail= attecl.eclattributename().subexpressionconstraint().constraintoperator();
		conRef(eclRef,entail, where);
		if (attecl.expressioncomparisonoperator() != null) {
			if (attecl.expressioncomparisonoperator().EQUALS() != null) {
				if (attecl.subexpressionconstraint().eclfocusconcept() != null) {
					where.addIs(getValue(attecl
						.subexpressionconstraint().eclfocusconcept().eclconceptreference(),
						attecl.subexpressionconstraint().constraintoperator()));
				} else {
					throw new UnknownFormatConversionException("multi nested ECL not yet supported " + ecl);
				}
			} else {
				throw new UnknownFormatConversionException("unknown comparison type operator " + ecl);
			}
		} else {
			throw new UnknownFormatConversionException("unrecognised comparison operator " + ecl);
		}
	}




	private Node getValue(ECLParser.EclconceptreferenceContext eclRef,ECLParser.ConstraintoperatorContext entail ) throws DataFormatException {
		Node conRef = new Node();
		conRef(eclRef, entail, conRef);
		return conRef;
	}

	private void conRef(ECLParser.EclconceptreferenceContext eclRef,
											ECLParser.ConstraintoperatorContext entail, Element conRef) throws DataFormatException {

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


	private void convertAttributeGroup(Match match,
																		 ECLParser.EclattributegroupContext eclGroup) throws DataFormatException {
		if (eclGroup.eclattributeset()!=null) {
			convertAttributeSet(match, eclGroup.eclattributeset(),false);
		}
		else
			throw new DataFormatException("Unable to cope with this type of attribute group : "+ ecl);
	}




}
