package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.OWL;
import org.endeavourhealth.imapi.vocabulary.RDFS;

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

	public Set<TTEntity> getAllConceptSets(TTIriRef type) throws SQLException {
		Set<TTEntity> result= new HashSet<>();
		PreparedStatement getConceptSets= conn.prepareStatement("SELECT iri from entity\n"+
			"join entity_type et on entity.dbid = et.entity\n"+
			"where et.type='"+ type.getIri()+"'");
		try (ResultSet rs= getConceptSets.executeQuery()) {
			while (rs.next()) {
				String iri = rs.getString("iri");
				TTEntity set = getSetDefinition(iri);
				if (set.get(IM.HAS_MEMBER)!=null|(set.get(IM.HAS_SUBSET)!=null))
					result.add(set);
			}
		}
		return result;

	}
	public TTEntity getSetDefinition(String iri) throws SQLException {
		Set<TTIriRef> predicates= new HashSet<>();
		predicates.add(IM.HAS_MEMBER);
		predicates.add(IM.HAS_SUBSET);
		predicates.add(IM.NOT_MEMBER);
		predicates.add(IM.IS_CONTAINED_IN);
		predicates.add(RDFS.LABEL);
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
		if (!predicates.isEmpty())
			sql.add("\tAND p.iri IN " + inList(predicates.size())) ;
		sql.add("\tAND tpl.blank_node IS NULL")
			.add("UNION ALL")
			.add("\tSELECT t2.dbid, t2.subject, t2.blank_node AS parent, t2.predicate, t2.object, t2.literal, t2.functional")
			.add("\tFROM triples t")
			.add("\tJOIN tpl t2 ON t2.blank_node= t.dbid")
			.add("\tWHERE t2.dbid <> t.dbid")
			.add(")")
			.add("SELECT t.subject,t.dbid, t.parent, p.iri AS predicateIri, p.name AS predicate, o.iri AS objectIri, o.name AS object, t.literal, t.functional")
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
					if (parentDbid==null)
						node.set(IM.DBID,TTLiteral.literal(rs.getInt("subject")));
					TTIriRef predicate= 	TTIriRef.iri(rs.getString("predicateIri"));
					TTValue ttobject= node.get(predicate);
					boolean functional= rs.getInt("functional")==1;
					if (ttobject==null) {
						if (!functional)
							node.set(predicate, new TTArray());
					}
					String objectIri=rs.getString("objectIri");
					String data = rs.getString("t.literal");
					if (data!=null) {
						node.set(predicate, TTLiteral.literal(data));
					}
					else if (objectIri!=null) {
						TTIriRef rdfObject = TTIriRef.iri(objectIri, rs.getString("object"));
						if (!functional)
							node.addObject(predicate, rdfObject);
						else
							node.set(predicate, rdfObject);
					}
					else {
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
	 * @return the entity with expanded members null if no members
	 * @throws SQLException with database access problems
	 */
	public TTEntity getExpansion(String iri) throws SQLException {
		//First get the definition you want to expand
		TTEntity definition= getSetDefinition(iri);
		//Now get expansion
		TTEntity expanded= getExpansion(definition);
		return expanded;

	}

	public TTEntity getIM1Expansion(String iri) throws SQLException{
		TTEntity definition= getSetDefinition(iri);
		return getIM1Expansion(definition);
	}

	public TTEntity getIM1Expansion(TTEntity conceptSet) throws SQLException {
		PreparedStatement queryIM1Expansion= conn.prepareStatement(buildIM1ExpansionSQL(conceptSet));
		TTEntity expanded= new TTEntity();
		ResultSet rs= queryIM1Expansion.executeQuery();
		while (rs.next()) {
			TTEntity member = new TTEntity();
			expanded.addObject(IM.HAS_MEMBER, member);
			member.setCode(rs.getString("code"));
			member.setScheme(TTIriRef.iri(rs.getString("scheme")));
			member.set(IM.DBID, TTLiteral.literal(rs.getInt("im2")));
			member.set(TTIriRef.iri(IM.NAMESPACE + "im1dbid"), TTLiteral.literal(rs.getInt("im1")));
		}
		return expanded;
	}

	private String buildIM1ExpansionSQL(TTEntity conceptSet) {
		StringJoiner sql = new StringJoiner("\n");
		//Create core expansion
		sql.add("With core as (");
		sql.add("Select distinct included.dbid as dbid from (");
		buildIncludedSQL(conceptSet,sql);
		buildExcludeSQL(conceptSet,sql);
		sql.add("on included.dbid=excluded.exdbid");
		sql.add("where excluded.exdbid is null)");
		//Select core joined to im1
		sql.add("select entity.dbid as im2,entity.code as code,entity.scheme as scheme,im1map.dbid as im1")
		.add("from core")
		.add("join entity on core.dbid= entity.dbid")
        .add("join im1_scheme_map sm ON sm.namespace = entity.scheme")
		.add("join im1_dbid_scheme_code im1map on im1map.code = entity.code AND im1map.scheme = sm.scheme");
		//Select legacy joined to im1.
		sql.add("union all")
		.add("select entity.dbid as im2,entity.code as code,entity.scheme as scheme,im1map.dbid as im1")
			.add("from core")
		.add("inner join tpl on tpl.subject= core.dbid")
			.add("inner join entity matchedTo on tpl.predicate= matchedTo.dbid")
			.add("inner join entity on tpl.object=entity.dbid")
            .add("join im1_scheme_map sm ON sm.namespace = entity.scheme")
            .add("join im1_dbid_scheme_code im1map on im1map.code = entity.code AND im1map.scheme = sm.scheme")
			.add("where matchedTo.iri='http://endhealth.info/im#matchedTo'");
		return sql.toString();
	}

	public TTEntity getLegacyExpansion(String iri) throws SQLException {
		TTEntity definition= getSetDefinition(iri);
		return getLegacyExpansion(definition);
	}
	public TTEntity getLegacyExpansion(TTEntity conceptSet) throws SQLException {
		PreparedStatement queryLegacyExpansion= conn.prepareStatement(buildLegacyExpansionSQL(conceptSet));
		return populateSet(conceptSet,queryLegacyExpansion);
	}

	private String buildLegacyExpansionSQL(TTEntity conceptSet) throws SQLException {
		StringJoiner sql = new StringJoiner("\n");
		sql.add("Select distinct legacy.dbid as dbid,legacy.code,legacy.name,legacy.scheme,legacy.iri from (");
		buildIncludedSQL(conceptSet,sql);
		buildExcludeSQL(conceptSet,sql);
		sql.add("on included.dbid=excluded.exdbid");
		sql.add("inner join tpl on tpl.subject= included.dbid")
			.add("inner join entity matchedTo on tpl.predicate= matchedTo.dbid")
			.add("inner join entity legacy on tpl.object= legacy.dbid")
			.add("where matchedTo.iri='"+IM.MATCHED_TO.getIri()+"'")
			.add("and excluded.exdbid is null");
		return sql.toString();
	}

	/**
	 * Returns a TTEntity with has members consisting of a list of entities representing the TCT of the set
	 * @param conceptSet The set as an TT Entity
	 * @return the entity with expanded members null of no members
	 * @throws SQLException with database access problems
	 */
	public TTEntity getExpansion(TTEntity conceptSet) throws SQLException {
		//No members null return
		if (conceptSet.get(IM.HAS_MEMBER) == null)
			return null;
		//build Expansion SQL first
		PreparedStatement queryExpansion = conn.prepareStatement(buildCoreExpansionSQL(conceptSet));
		return populateSet(conceptSet,queryExpansion);
	}
	private TTEntity populateSet(TTEntity conceptSet,PreparedStatement queryExpansion) throws SQLException {
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
				member.set(IM.DBID,TTLiteral.literal(rs.getInt("dbid")));
			}
		}
		return expanded;
	}
	private String buildCoreExpansionSQL(TTEntity conceptSet) {
		StringJoiner sql = new StringJoiner("\n");
		//Build  select with core only
		sql.add("Select distinct entity.dbid as dbid,entity.code,entity.name,entity.scheme,entity.iri from (");
		buildIncludedSQL(conceptSet,sql);
		buildExcludeSQL(conceptSet,sql);
		sql.add("on included.dbid=excluded.exdbid")
			.add("join entity on included.dbid= entity.dbid")
		.add("where excluded.exdbid is null");
		return sql.toString();
	}

	private void buildIncludedSQL(TTEntity conceptSet,StringJoiner sql){
		//select the core dbid  as outer query first with the final result of entity dbids

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
		sql.add(") as included");


	}

	private void buildExcludeSQL(TTEntity conceptSet, StringJoiner sql) {
		sql.add(	"left join ")
		.add("(select tct.descendant as exdbid")
		.add("from entity cs")
		.add(" join tpl on tpl.subject= cs.dbid")
		.add("join entity notMembers on tpl.predicate= notMembers.dbid")
		.add(" join tct on tct.ancestor= tpl.object")
		.add("where cs.iri='"+conceptSet.getIri()+"'")
		.add("and notMembers.iri='"+ IM.NOT_MEMBER.getIri()+"') as excluded");
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
