package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.OWL;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.informationmanager.common.dal.DALHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class SetRepository {
	private final Connection conn;


	public SetRepository() throws SQLException, ClassNotFoundException {
		conn= ImportUtils.getConnection();


	}

	public Set<TTEntity> getAllConceptSets(TTIriRef type) throws SQLException,ClassNotFoundException{
		Set<TTEntity> result= new HashSet<>();
		PreparedStatement getConceptSets= conn.prepareStatement("SELECT iri,name from entity\n"+
			"join entity_type et on entity.dbid = et.entity\n"+
			"where et.type='"+ type.getIri()+"'");
		try (ResultSet rs= getConceptSets.executeQuery()) {
			while (rs.next()) {
				String iri = rs.getString("iri");
				TTEntity set= getSet(iri);
				if (set.get(IM.HAS_MEMBER)!=null|(set.get(IM.HAS_SUBSET)!=null))
					result.add(set);
			}
		}
		return result;

	}
	public TTEntity getSet(String iri) throws SQLException,ClassNotFoundException {
		Set<TTIriRef> predicates= new HashSet<>();
		predicates.add(IM.HAS_MEMBER);
		predicates.add(IM.HAS_SUBSET);
		predicates.add(IM.NOT_MEMBER);
		predicates.add(IM.IS_CONTAINED_IN);
		TTEntity result= new TTEntity().setIri(iri);
		Map<String, TTNode> dbIdNodes= new HashMap<>();
		dbIdNodes.put(null,result);
		StringJoiner sql = new StringJoiner("\n")
			.add("WITH RECURSIVE triples AS (")
			.add("\tSELECT tpl.dbid, tpl.subject, tpl.blank_node AS parent, tpl.predicate, tpl.object, tpl.literal, tpl.functional")
			.add("\tFROM tpl")
			.add("\tJOIN entity e ON tpl.subject=e.dbid")
			.add("\tJOIN entity p ON p.dbid = tpl.predicate")
			.add("\tWHERE e.iri = ? ");
		if (predicates != null && !predicates.isEmpty())
			sql.add("\tAND p.iri IN " + inList(predicates.size())) ;
		sql.add("\tAND tpl.blank_node IS NULL")
			.add("UNION ALL")
			.add("\tSELECT t2.dbid, t2.subject, t2.blank_node AS parent, t2.predicate, t2.object, t2.literal, t2.functional")
			.add("\tFROM triples t")
			.add("\tJOIN tpl t2 ON t2.blank_node= t.dbid")
			.add("\tWHERE t2.dbid <> t.dbid")
			.add(")")
			.add("SELECT t.dbid, t.parent, p.iri AS predicateIri, p.name AS predicate, o.iri AS objectIri, o.name AS object, t.literal, t.functional")
			.add("FROM triples t")
			.add("JOIN entity p ON t.predicate = p.dbid")
			.add("LEFT JOIN entity o ON t.object = o.dbid")
			.add("order by parent");
		try (PreparedStatement statement = conn.prepareStatement(sql.toString())) {
			int i = 0;
			statement.setString(++i, iri);
			if (predicates != null && !predicates.isEmpty()) {
				for (TTIriRef predicate : predicates)
					statement.setString(++i, predicate.getIri());
			}
			try (ResultSet rs = statement.executeQuery()) {
				while (rs.next()) {
					String parentDbid= rs.getString("parent");
					TTNode node= dbIdNodes.get(parentDbid);
					TTIriRef predicate= 	TTIriRef.iri(rs.getString("predicateIri"));
					TTValue ttobject= node.get(predicate);
					boolean functional= rs.getInt("functional")==1;
					if (ttobject==null) {
						if (!functional)
							node.set(predicate, new TTArray());
					}
					String objectIri=rs.getString("objectIri");
					if (objectIri!=null) {
						TTIriRef rdfObject = TTIriRef.iri(objectIri, rs.getString("object"));
						if (!functional)
							node.addObject(predicate, rdfObject);
						else
							node.set(predicate, rdfObject);
					} else {
						TTNode newNode=new TTNode();
						dbIdNodes.put(rs.getString("dbid"),newNode);
						if (!functional)
							node.addObject(predicate,newNode);
						else
							node.set(predicate,newNode);
					}
				}
			}
		}

		return result;
	}

	/**
	 * Returns a TTEntity with has members consisting of a list of entities representing the TCT of the set
	 * @param iri  the iri of the concept set
	 * @param includeLegacy whether to include legacy concepts
	 * @return the entity with expanded members null if no members
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public TTEntity getExpandedSet(String iri,boolean includeLegacy) throws SQLException, ClassNotFoundException {
		//First get the definition you want to expand
		TTEntity definition= getSet(iri);
		//Now get expansion
		TTEntity expanded= getExpansion(definition,includeLegacy);
		return expanded;

	}

	/**
	 * Returns a TTEntity with has members consisting of a list of entities representing the TCT of the set
	 * @param conceptSet The set as an TT Entity
	 * @param includeLegacy whether to include legacy concepts
	 * @return the entity with expanded members null of no members
	 * @throws SQLException
	 */
	public TTEntity getExpansion(TTEntity conceptSet,boolean includeLegacy) throws SQLException {
		//No members null return
		if (conceptSet.get(IM.HAS_MEMBER)==null)
			return null;
		//build Expansion SQL first
		PreparedStatement queryExpansion= conn.prepareStatement(buildExpansionSQL(conceptSet,includeLegacy));
		TTEntity expanded=new TTEntity();
		try (ResultSet rs=queryExpansion.executeQuery()) {
			expanded.setIri(conceptSet.getIri());
			while (rs.next()){
				TTEntity member= new TTEntity();
				expanded.addObject(IM.HAS_MEMBER,member);
				member.setIri(rs.getString("iri"));
				member.setCode(rs.getString("code"));
				member.setScheme(TTIriRef.iri(rs.getString("scheme")));
				member.setName(rs.getString("name"));
				if (includeLegacy)
					member.set(IM.IM1_DBID,TTLiteral.literal("im1map.im1"));
			}
		}
		return expanded;
	}
	private String buildExpansionSQL(TTEntity conceptSet, boolean includeLegacy) {
		StringJoiner sql = new StringJoiner("\n");
		if (includeLegacy){
			//Outer CTE wrapper so it can select on itself
			sql.add("With core as (");
			//Include im1db if present
			sql.add("Select expanded.dbid as dbid,code,name,scheme,iri,im1map.im1 from (");
		}
		else {
			//Build  select with core only
			sql.add("Select expanded.dbid as dbid,code,name,scheme,iri from (");
		}

		//select the core dbid  as outer query first with the final result of entity dbids
		sql.add("select dbid from (");

		//First union all the simple iri members which most concept sets are made up from
		buildSimpleExpressions(conceptSet, sql);
		//Loops through the members that have complex expressions

		for (TTValue member : conceptSet.get(IM.HAS_MEMBER).asArray().getElements()) {
			if (member.isNode()) {
				sql.add("Union all");
				StringJoiner from = new StringJoiner("\n");
				StringJoiner where = new StringJoiner("\n");
				if (member.asNode().get(OWL.INTERSECTIONOF) != null) {
					buildIntersectionSQL(member.asNode(), sql,from, where);
				}
			}
		}
		sql.add(") as include");
		buildExcludeSQL(conceptSet,sql);
		sql.add(") as expanded");
		sql.add("inner join entity on expanded.dbid= entity.dbid");
		if (includeLegacy){
			sql.add(")");
			sql.add("select * from core")
			.add("union all")
			.add("select legacy.dbid as dbid,legacy.code as code,legacy.name as name,legacy.scheme as scheme,legacy.iri as iri,im1map.im1")
			.add("from entity legacy")
			.add("join tpl on tpl.object=legacy.dbid")
			.add("join entity matchedTo on tpl.predicate= matchedTo.dbid")
			.add("join  core on core.dbid=tpl.subject")
				.add("left join im1map on expanded.dbid=im1map.im2")
			.add("where matchedTo.iri='http://endhealth.info/im#matchedTo'");
		}

		return sql.toString();
	}

	private void buildExcludeSQL(TTEntity conceptSet, StringJoiner sql) {
		sql.add(	"left join ")
		.add("(select distinct subExclude.dbid as exdbid")
		.add("from entity cs")
		.add("join tpl on tpl.subject= cs.dbid")
		.add("join entity notMembers on tpl.predicate= notMembers.dbid")
		.add("join entity superExclude on tpl.object= superExclude.dbid")
		.add("join tct on tct.ancestor= tpl.object")
		.add("join entity subExclude on tct.descendant= subExclude.dbid")
		.add("where cs.iri='"+conceptSet.getIri()+"'")
		.add("and notMembers.iri='"+ IM.NOT_MEMBER.getIri()+"') as exclude")
			.add(" on include.dbid=exclude.exdbid")
		.add("where exdbid is null");
	}

	private void buildIntersectionSQL(TTNode member, StringJoiner sql, StringJoiner from, StringJoiner where) {
		// Sort expressions into role groups followed  by superclass iris
		List<TTIriRef> superClasses = new ArrayList<>();
		List<TTNode> roles = new ArrayList<>();
		for (TTValue ob : member.asNode().get(OWL.INTERSECTIONOF).asArray().getElements()) {
			if (ob.isIriRef())
				superClasses.add(ob.asIriRef());
			else
				roles.add(ob.asNode());
		}
		int expressionNum = 0;
		int roleNum=0;
		if (!roles.isEmpty()) {
			sql.add("select distinct tpl_e1_r1.subject as dbid from");
			from.add("tpl tpl_e1_r1");
			for (TTNode role : roles) {
				expressionNum++;
				Map<TTIriRef, TTValue> predicateMap = role.getPredicateMap();
				for (Map.Entry<TTIriRef,TTValue> entry:predicateMap.entrySet()) {
					roleNum++;
					TTIriRef attributeIri = entry.getKey();
					TTIriRef valueIri = entry.getValue().asIriRef();
					buildRoleSQL(attributeIri, valueIri,expressionNum,roleNum, from, where);
				}
			}
			if (!superClasses.isEmpty()){

				for (TTIriRef superClass:superClasses){
					expressionNum++;
					from.add("join tct focus_e"+expressionNum+"_tct on focus_e"+expressionNum+"_tct.descendant= tpl_e1_r1.subject");
					buildSuperClassSQL(superClass,expressionNum,from,where);
				}
			}
		} else {
			if (!superClasses.isEmpty()) {
				sql.add("select distinct tct.descendant from ");
				from.add("tct focus_e1_tct");
				for (TTIriRef superClass : superClasses) {
					expressionNum++;
					if (expressionNum>1)
						from.add("join tct focus_e"+expressionNum+"_tct on focus_e"+expressionNum+"_tct.descendant= tct.descendant");
					buildSuperClassSQL(superClass, expressionNum, from, where);
				}
			}
		}
		sql.add(from.toString());
		sql.add(where.toString());
	}

	private void buildSuperClassSQL(TTIriRef superClass, int expressionNum, StringJoiner from, StringJoiner where) {
	int e=expressionNum;
	 from.add("join entity focus_e"+e+" on focus_e"+e+"_tct.ancestor= focus_e"+e+".dbid");
		if (expressionNum==1)
			where.add("where");
		else
			where.add("and");
		where.add("focus_e"+e+".iri='"+superClass.getIri()+"'");
	}

	private void buildRoleSQL(TTIriRef attributeIri,TTIriRef valueIri,int e, int r,StringJoiner from, StringJoiner where) {
		String grouper="_e"+e+"_r"+r;//Indicates the expression number and role number for aliases
		String lastRole="_e"+e+"_r"+(r-1);
		if (e>1|r>1)
		 {
			from.add("");
			from.add("join tpl tpl"+grouper+ " on tpl"+grouper+".subject=tpl"+lastRole+".subject");
		}
			from.add("join tct attribute" +grouper + "_tct on tpl" + grouper+".predicate= attribute" + grouper + "_tct.descendant")
			.add("join entity attribute" + grouper + " on attribute" + grouper+ "_tct.ancestor= attribute" + grouper + ".dbid")
			.add("join tct value" + grouper + "_tct on value" + grouper + "_tct.descendant= tpl"+grouper+".object")
			.add("join entity objectEntity" + grouper + " on tpl"+grouper+".object=objectEntity" + grouper + ".dbid")
			.add("join entity value" + grouper + " on value" + grouper + "_tct.ancestor= value" + grouper + ".dbid")
			.add("join tpl tpl" + grouper + "_2 on tpl" + grouper + ".blank_node= tpl" + grouper + "_2.dbid")
			.add("join entity role" + grouper + "_2 on tpl" + grouper + "_2.predicate= role" + grouper + "_2.dbid");
		if (e == 1&r==1)
			where.add("where");
		else
			where.add("and");
		where
			.add("attribute" + grouper + ".iri='" + attributeIri.getIri() + "' and value" + grouper + ".iri='" + valueIri.getIri() + "'")
			.add("and role" + grouper + "_2.iri= 'http://endhealth.info/im#roleGroup'");
	}


	/**
	 * Simple sql on iri based set members
	 * @param conceptSet The entity representing the concept set
	 * @param sql A String builder
	 */
	private void buildSimpleExpressions(TTEntity conceptSet,StringJoiner sql) {
		//Selects the transitive closure descendents from any or all of the member iris
		sql.add("select distinct tct.descendant as dbid from");
		sql.add("entity cs")
			.add("join tpl on tpl.subject= cs.dbid")
			.add("join entity hasmembers on tpl.predicate= hasmembers.dbid")
			.add("join entity core on tpl.object= core.dbid")
			.add("join tct on tct.ancestor= tpl.object")
			.add("where cs.iri='"+ conceptSet.getIri()+ "'")
			.add("and hasmembers.iri='http://endhealth.info/im#hasMembers'");
	}



	protected String inList(int size) {
		return "(" + String.join(",", Collections.nCopies(size, "?")) + ")";
	}
}
