import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.informationmanager.common.dal.DALHelper;
import org.endeavourhealth.informationmanager.common.transform.TTToECL;
import org.endeavourhealth.informationmanager.transforms.ImportUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ConceptSetExporter {

	private final PreparedStatement getConceptSets;
	private final PreparedStatement getSetDefinition;
	private final PreparedStatement getSetExpansion;

	public ConceptSetExporter() throws SQLException, ClassNotFoundException {
		Connection conn= ImportUtils.getConnection();
		getConceptSets= conn.prepareStatement("SELECT iri,name from entity\n"+
			"join entity_type et on entity.dbid = et.entity\n"+
			"where et.type='"+ IM.CONCEPT_SET.getIri()+"'");
		getSetDefinition= conn.prepareStatement("WITH recursive triples AS (\n" +
			"select tpl.dbid,tpl.functional,tpl.subject, tpl.blank_node as parent,tpl.predicate,tpl.object,tpl.literal as data\n" +
			"from tpl\n" +
			"join entity e on tpl.subject=e.dbid\n" +
			"join entity p on tpl.predicate= p.dbid\n" +
			"where e.iri =? and tpl.blank_node is null\n" +
			"and p.iri in('http://endhealth.info/im#hasMembers','http://endhealth.info/im#hasSubsets','http://endhealth.info/im#isContainedIn','"+
			RDFS.SUBCLASSOF.getIri() +"')\n" +
			"\n" +
			"union all\n" +
			"-- the recursive steps down children joining the child blank node on the parent dbid\n" +
			"select t2.dbid,t2.functional,t2.subject, t2.blank_node as parent,t2.predicate,t2.object,t2.literal as data\n" +
			"from triples\n" +
			"join tpl t2 on t2.blank_node= triples.dbid\n" +
			"where t2.dbid<> triples.dbid\n" +
			"\n" +
			")\n" +
			"-- Just selects a few things to show it.\n" +
			"select triples.dbid as dbid,triples.functional,triples.parent,p.iri as predicateIri,p.name as predicateName,o.iri as objectIri,o.name as objectName, triples.data\n" +
			"from triples\n" +
			"join entity p on triples.predicate= p.dbid\n" +
			"left join entity o on triples.object= o.dbid\n" +
			"-- left join term_code tc2 on tc2.entity= triples.object\n" +
			"order by parent");
		getSetExpansion= conn.prepareStatement("select im1.concept.dbid as im1bid, subsetIri,im1.concept.code, im1.concept.name,im2.memberscheme,im2.membercode, im2.membername,im2.iri,im2.memberdbid\n"+
		"from (\n"+
			"select subset.iri as subsetIri,leafmembers.dbid as memberdbid,"+
			"leafmembers.code  as membercode,leafmembers.scheme as memberscheme,leafmembers.name as membername,leafmembers.iri as iri\n"+
			"from entity cs\n"+
			"join tpl on tpl.subject= cs.dbid\n"+
			"join entity hassubset on tpl.predicate= hassubset.dbid\n"+
			"left join entity subset on tpl.object= subset.dbid\n"+
			"join tpl tplm on tplm.subject= subset.dbid\n"+
			"join entity hasmembers on tplm.predicate= hasmembers.dbid\n"+
			"join entity members on tplm.object=members.dbid\n"+
			"left join tct on tct.ancestor= members.dbid\n"+
			"left join entity leafmembers on tct.descendant= leafmembers.dbid\n"+
			"where cs.iri=?\n"+
			"and hassubset.iri='http://endhealth.info/im#hasSubsets'\n"+
			"and hasmembers.iri='http://endhealth.info/im#hasMembers'\n"+
			"union all\n"+
			"select subset.iri as subsetIri,legacy.dbid as memberdbid,legacy.code as membercode,"+
			"legacy.scheme as memberscheme,legacy.name as membername, legacy.iri as iru\n"+
			"from entity cs\n"+
			"join tpl on tpl.subject= cs.dbid\n"+
			"join entity hassubset on tpl.predicate= hassubset.dbid\n"+
			"left join entity subset on tpl.object= subset.dbid\n"+
			"join tpl tplm on tplm.subject= subset.dbid\n"+
			"join entity hasmembers on tplm.predicate= hasmembers.dbid\n"+
			"join entity members on tplm.object=members.dbid\n"+
			"left join tct on tct.ancestor= members.dbid\n"+
			"left join entity leafmembers on tct.descendant= leafmembers.dbid\n"+
			"left join tpl mtpl on mtpl.subject=leafmembers.dbid\n"+
			"left join entity matchedto on mtpl.predicate=matchedto.dbid\n"+
			"left join entity legacy on mtpl.object= legacy.dbid\n"+
			"where cs.iri=?\n"+
			"and hassubset.iri='http://endhealth.info/im#hasSubsets'\n"+
			"and hasmembers.iri='http://endhealth.info/im#hasMembers'\n"+
			"and hasmembers.iri='http://endhealth.info/im#hasMembers'\n"+
			"and matchedto.iri='http://endhealth.info/im#matchedTo') as im2\n"+
		"left join im1.concept on im2.membercode=im1.concept.code\n"+
		"order by subsetIri,scheme");

	}

	public void exportAll(String path) throws SQLException, ClassNotFoundException, IOException {
		FileWriter definitions= new FileWriter(path+"\\ConceptSetDefinitions.txt");
		FileWriter expansions= new FileWriter(path+"\\ConceptSetExpansions.txt");
		FileWriter folders= new FileWriter(path+"\\ConceptSetFolders.txt");
		FileWriter subsets= new FileWriter(path+"\\ConceptSetSubsets.txt");
		Map<String,TTEntity> conceptSets= new HashMap<>();

		try (ResultSet rs= getConceptSets.executeQuery()){
			while (rs.next()){
				String iri=rs.getString("iri");
				String name= rs.getString("name");
				DALHelper.setString(getSetDefinition,1,iri);
				try (ResultSet sr= getSetDefinition.executeQuery()){
					TTEntity conceptSet= buildEntity(iri,name,sr);
					if (!conceptSet.getName().contains("Test header"))
						conceptSets.put(iri,conceptSet);
				}
			}
		}
		folders.write("Concept set iri \tConcept set name\tFolder iri\tFolder name\n");
		subsets.write("Concept set iri\tConcept set name\tSubset iri\tSubset name\n");
		definitions.write("Concept set iri\tConcept set name\tSet definition ECL\tSet definition json-LD\n");
		for (Map.Entry<String,TTEntity> entry:conceptSets.entrySet()){

			String setIri= entry.getKey();
			TTEntity conceptSet= entry.getValue();
			System.out.println("Exporting "+setIri+": "+conceptSet.getName()+"..");
			if (conceptSet.get(IM.IS_CONTAINED_IN)!=null)
				for (TTValue value:conceptSet.get(IM.IS_CONTAINED_IN).asArray().getElements()) {
					folders.write(conceptSet.getIri() + "\t" + conceptSet.getName() + "\t" + value.asIriRef().getIri() + "\t" + value.asIriRef().getName()+"\n");
				}
			if (conceptSet.get(RDFS.SUBCLASSOF)!=null)
				for (TTValue value:conceptSet.get(RDFS.SUBCLASSOF).asArray().getElements()) {
					folders.write(conceptSet.getIri() + "\t" + conceptSet.getName() + "\t" + value.asIriRef().getIri() + "\t" + value.asIriRef().getName()+"\n");
				}
			if (conceptSet.get(IM.HAS_SUBSET)!=null||conceptSet.get(IM.HAS_MEMBER)!=null){
				DALHelper.setString(getSetExpansion,1,setIri);
				DALHelper.setString(getSetExpansion,2,setIri);
				try (ResultSet rs= getSetExpansion.executeQuery()){
					while (rs.next()){
						expansions.write(conceptSet.getIri()+"\t"+conceptSet.getName());
						expansions.write(rs.getString("subsetIri"));
						expansions.write(rs.getString("im1dbid"));
						expansions.write(rs.getString("code"));
						expansions.write(rs.getString("membername"));
						expansions.write(rs.getString("membersscheme"));
						expansions.write("\n");
					}
				}
			}

			if (conceptSet.get(IM.HAS_SUBSET)!=null)
				for (TTValue value:conceptSet.get(IM.HAS_SUBSET).asArray().getElements()) {
					subsets.write(conceptSet.getIri() + "\t" + conceptSet.getName() + "\t" + value.asIriRef().getIri() + "\t" + value.asIriRef().getName()+"\n");
				}
			if (conceptSet.get(IM.HAS_MEMBER)!=null){
				ObjectMapper objectMapper = new ObjectMapper();
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
				String json=objectMapper.writeValueAsString(conceptSet);
				TTToECL eclConverter= new TTToECL();
				String ecl= eclConverter.getConceptSetECL(conceptSet,null);
				definitions.write(conceptSet.getIri() + "\t" + conceptSet.getName() + "\t" + ecl + "\t" + json+"\n");
			}


		}
		folders.flush();
		folders.close();
		definitions.flush();
		definitions.close();
		subsets.flush();
		subsets.close();
		expansions.flush();
		expansions.close();

	}

	private TTEntity buildEntity(String iri, String name, ResultSet sr) throws SQLException {
		Map<String,TTNode> dbIdNodes= new HashMap<>();
		TTEntity cs= new TTEntity()
			.setIri(iri)
			.setName(name);
		dbIdNodes.put(null,cs);
		while (sr.next()){
			String parentDbid= sr.getString("parent");
			TTNode node= dbIdNodes.get(parentDbid);
			TTIriRef predicate= 	TTIriRef.iri(sr.getString("predicateIri"));
			TTValue ttobject= node.get(predicate);
			boolean functional= sr.getInt("functional")==1;
			if (ttobject==null) {
				if (!functional)
					node.set(predicate, new TTArray());
			}
			String objectIri=sr.getString("objectIri");
			if (objectIri!=null) {
				TTIriRef rdfObject = TTIriRef.iri(objectIri, sr.getString("objectName"));
				if (!functional)
					node.addObject(predicate, rdfObject);
				else
					node.set(predicate, rdfObject);
			} else {
				TTNode newNode=new TTNode();
				dbIdNodes.put(sr.getString("dbid"),newNode);
				if (!functional)
					node.addObject(predicate,newNode);
				else
					node.set(predicate,newNode);
			}

		}
		return cs;
	}

}
