// Generated from C:/Users/david/CloudStation/EhealthTrust/DiscoveryDataService/InformationManager/common/src/main/grammars\ECL.g4 by ANTLR 4.12.0
package org.endeavourhealth.informationmanager.common.parser.ecl;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link ECLParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface ECLVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link ECLParser#ecl}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEcl(ECLParser.EclContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#expressionconstraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpressionconstraint(ECLParser.ExpressionconstraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#refinedexpressionconstraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitRefinedexpressionconstraint(ECLParser.RefinedexpressionconstraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#compoundexpressionconstraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCompoundexpressionconstraint(ECLParser.CompoundexpressionconstraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#conjunctionexpressionconstraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConjunctionexpressionconstraint(ECLParser.ConjunctionexpressionconstraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#disjunctionexpressionconstraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDisjunctionexpressionconstraint(ECLParser.DisjunctionexpressionconstraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#exclusionexpressionconstraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExclusionexpressionconstraint(ECLParser.ExclusionexpressionconstraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#dottedexpressionconstraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDottedexpressionconstraint(ECLParser.DottedexpressionconstraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#dottedexpressionattribute}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDottedexpressionattribute(ECLParser.DottedexpressionattributeContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#subexpressionconstraint}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubexpressionconstraint(ECLParser.SubexpressionconstraintContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#eclfocusconcept}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEclfocusconcept(ECLParser.EclfocusconceptContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#dot}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDot(ECLParser.DotContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#memberof}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMemberof(ECLParser.MemberofContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#eclconceptreference}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEclconceptreference(ECLParser.EclconceptreferenceContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#conceptid}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConceptid(ECLParser.ConceptidContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#term}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTerm(ECLParser.TermContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#wildcard}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWildcard(ECLParser.WildcardContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#constraintoperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConstraintoperator(ECLParser.ConstraintoperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#descendantof}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescendantof(ECLParser.DescendantofContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#descendantorselfof}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDescendantorselfof(ECLParser.DescendantorselfofContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#childof}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitChildof(ECLParser.ChildofContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#ancestorof}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAncestorof(ECLParser.AncestorofContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#ancestororselfof}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAncestororselfof(ECLParser.AncestororselfofContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#parentof}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitParentof(ECLParser.ParentofContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#conjunction}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConjunction(ECLParser.ConjunctionContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#disjunction}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDisjunction(ECLParser.DisjunctionContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#exclusion}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExclusion(ECLParser.ExclusionContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#eclrefinement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEclrefinement(ECLParser.EclrefinementContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#conjunctionrefinementset}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConjunctionrefinementset(ECLParser.ConjunctionrefinementsetContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#disjunctionrefinementset}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDisjunctionrefinementset(ECLParser.DisjunctionrefinementsetContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#subrefinement}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubrefinement(ECLParser.SubrefinementContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#eclattributeset}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEclattributeset(ECLParser.EclattributesetContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#conjunctionattributeset}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitConjunctionattributeset(ECLParser.ConjunctionattributesetContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#disjunctionattributeset}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDisjunctionattributeset(ECLParser.DisjunctionattributesetContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#subattributeset}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubattributeset(ECLParser.SubattributesetContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#eclattributegroup}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEclattributegroup(ECLParser.EclattributegroupContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#eclattribute}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEclattribute(ECLParser.EclattributeContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#cardinality}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCardinality(ECLParser.CardinalityContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#minvalue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMinvalue(ECLParser.MinvalueContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#to}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTo(ECLParser.ToContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#maxvalue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMaxvalue(ECLParser.MaxvalueContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#many}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMany(ECLParser.ManyContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#reverseflag}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitReverseflag(ECLParser.ReverseflagContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#eclattributename}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEclattributename(ECLParser.EclattributenameContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#expressioncomparisonoperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpressioncomparisonoperator(ECLParser.ExpressioncomparisonoperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#numericcomparisonoperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNumericcomparisonoperator(ECLParser.NumericcomparisonoperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#stringcomparisonoperator}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStringcomparisonoperator(ECLParser.StringcomparisonoperatorContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#numericvalue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNumericvalue(ECLParser.NumericvalueContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#stringvalue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStringvalue(ECLParser.StringvalueContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#integervalue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIntegervalue(ECLParser.IntegervalueContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#decimalvalue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDecimalvalue(ECLParser.DecimalvalueContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#nonnegativeintegervalue}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonnegativeintegervalue(ECLParser.NonnegativeintegervalueContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#sctid}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSctid(ECLParser.SctidContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#ws}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWs(ECLParser.WsContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#mws}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitMws(ECLParser.MwsContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#comment}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitComment(ECLParser.CommentContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#nonstarchar}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonstarchar(ECLParser.NonstarcharContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#nonspacechar}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonspacechar(ECLParser.NonspacecharContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#starwithnonfslash}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitStarwithnonfslash(ECLParser.StarwithnonfslashContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#nonfslash}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonfslash(ECLParser.NonfslashContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#sp}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSp(ECLParser.SpContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#htab}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitHtab(ECLParser.HtabContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#cr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitCr(ECLParser.CrContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#lf}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLf(ECLParser.LfContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#qm}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitQm(ECLParser.QmContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#bs}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitBs(ECLParser.BsContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#digit}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDigit(ECLParser.DigitContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#zero}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitZero(ECLParser.ZeroContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#digitnonzero}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitDigitnonzero(ECLParser.DigitnonzeroContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#nonwsnonpipe}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNonwsnonpipe(ECLParser.NonwsnonpipeContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#anynonescapedchar}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAnynonescapedchar(ECLParser.AnynonescapedcharContext ctx);
	/**
	 * Visit a parse tree produced by {@link ECLParser#escapedchar}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitEscapedchar(ECLParser.EscapedcharContext ctx);
}