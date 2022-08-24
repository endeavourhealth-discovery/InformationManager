package org.endeavourhealth.informationmanager.transforms.authored;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.endeavourhealth.imapi.model.sets.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ModelShapes {

	private static final TTIriRef IMT= TTIriRef.iri(IM.NAMESPACE+"InformationModelTopClasses");
	private static final TTIriRef IMO= TTIriRef.iri(IM.NAMESPACE+"InformationModelOtherClasses");
	private static final TTManager manager= new TTManager();


	public static void main(String[] args) throws Exception {
		String sourcePath= args[0].replace("%"," ");
		createShapes(sourcePath);
	}


	public static void createShapes(String sourcePath) throws IOException {
		loadDocument(sourcePath+"\\CoreOntology.json");
		String[] shapes= {"QueryRequest"};
		for (String iri:shapes){
			TTEntity shape= getEntity(IM.NAMESPACE+iri);
			shape.addType(SHACL.NODESHAPE);
			shape.addType(OWL.CLASS);
			shape.addObject(IM.IS_CONTAINED_IN,IMT);
			switch (iri) {
				case "QueryRequest" :
					queryRequest(shape);
					break;
			}
		}
		saveDocument(sourcePath+"\\CoreOntology.json");

	}



	private static TTManager loadDocument(String file) throws IOException {
		manager.loadDocument(new File(file));
		return manager;
	}

	private static TTEntity getEntity(String iri){
		TTEntity entity= manager.getEntity(iri);
		if (entity==null){
			entity= new TTEntity()
				.setIri(iri)
					.setName(localName(iri));
			manager.getDocument().addEntity(entity);
		}
		entity.getPredicateMap().remove(SHACL.PROPERTY);
		entity.getPredicateMap().remove(IM.IS_CONTAINED_IN);
		entity.getPredicateMap().remove(RDFS.SUBCLASSOF);
		return entity;
	}

	private static void saveDocument(String file) throws JsonProcessingException {
		manager.saveDocument(new File(file));
	}

	private static void query(TTEntity shape) {
		shape.addType(SHACL.NODESHAPE);
		shape.addType(OWL.CLASS);
		shape.addObject(IM.IS_CONTAINED_IN,IMT);
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"ModelHeading"));

		addProperty(shape,"resultFormat",SHACL.DATATYPE,XSD.STRING,0,1,
			"Whether the result set is required flat select style json or a nested graphql json object style 'RELATIONAL' or 'OBJECT."+
			" Default is OBJECT");
		addProperty(shape,"usePrefixes",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"true if you want the results to use IRI prefixes");
		addProperty(shape,"activeOnly",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether only active entities are included in the match clauses or select clauses");
		addProperty(shape,"select",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"SelectClause"),1,1,
			"Select query clause logically similar to SQL/SPARQL select but with GraphQL nesting ability");

		addProperty(shape,"ask",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"MatchClause"),0,1,"If the query is a boolean true or false use match clauses as an ask");
		addProperty(shape,"mainEntity",SHACL.CLASS,SHACL.NODESHAPE,0,1,"The main entity to which all matches must be related e.g. Patient or organisation"+
			". i.e. the IRI of a data model entity (SHACL shape)");
		setOrs(shape,List.of("select","ask"),1,1);
	}

	private static void queryRequest(TTEntity shape) {

		shape.setName("Query Request")
				.setDescription("A request containing a query to be passed as a payload to the queryIM API.");
		addProperty(shape,"page",SHACL.NODE, TTIriRef.iri(IM.NAMESPACE+"PageInformation"),null,1,"optional page number and size if the client is looking for paged results");
		addProperty(shape,"textSearch",SHACL.DATATYPE, XSD.STRING,null,1,"If a free text search is part of the query");
		addProperty(shape,"argument",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"Argument"),null,null,"arguments to pass in to the query as parameters. Parameter name and value pairs. Values ,may be strings, numbers or IRIs");
		addProperty(shape,"query",SHACL.NODE,IM.QUERY,1,1,"The query definition for an inline dynamic query. If the query is already defined, use the queryIri. Arguments may be used in eoither approach");
		addProperty(shape,"queryIri",SHACL.NODE_KIND,SHACL.IRI,1,1,"The IRI of a predefined query in the information model. i.e. request the system to run query X");
		addProperty(shape,"referenceDate",SHACL.DATATYPE,TTIriRef.iri(IM.NAMESPACE+"DateTime"),null,1,"reference date for date ranges that use reference date. Note that it could be passed in as an argeument");
		setOrs(shape,List.of("query","queryIri"),1,1);
		pageInformation(getEntity(IM.NAMESPACE+"PageInformation"));
		argument(getEntity(IM.NAMESPACE+"Argument"));
		heading(getEntity(IM.NAMESPACE+"ModelHeading"));
		query(getEntity(IM.NAMESPACE+"Query"));
		select(getEntity(IM.NAMESPACE+"SelectClause"));
		match(getEntity(IM.NAMESPACE+"MatchClause"));
		function(getEntity(IM.NAMESPACE+"Function"));
		parameter(getEntity(IM.NAMESPACE+"Parameter"));
		functionClause(getEntity(IM.NAMESPACE+"FunctionClause"));
		propertySelect(getEntity(IM.NAMESPACE+"PropertySelectClause"));
		orderLimit(getEntity(IM.NAMESPACE+"OrderLimitClause"));
		pathTarget(getEntity(IM.NAMESPACE+"PathTargetClause"));
		conceptReference(getEntity(IM.NAMESPACE+"ConceptReference"));
	}

	private static void parameter(TTEntity shape) {
		setLabels(shape);
		addProperty(shape,RDFS.LABEL,SHACL.DATATYPE,XSD.STRING,1,1,"The name of the parameter");
		addProperty(shape,SHACL.DATATYPE,SHACL.CLASS,RDFS.RESOURCE,1,1,"The iri of The data type of the parameter when the data type is a literal");
		addProperty(shape,SHACL.CLASS,SHACL.CLASS,RDFS.RESOURCE,1,1,"The iri of the class of the parameter when the argument is an object");
		setOrs(shape,List.of("datatype","class"),1,1);
	}

	private static void functionClause(TTEntity shape) {
		setLabels(shape);
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"IriRef"));
		addProperty(shape,"argument",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"Argument"),0,null,"Arguments to pass into the function. They should match the "+
			"parameter definitions of the function");
	}

	private static void function(TTEntity shape) {
		setLabels(shape);
		addProperty(shape,SHACL.PARAMETER,SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"Parameter"),0,null,"A list of parameters and data types used in this function");
	}


	private static void conceptReference(TTEntity shape) {
		setLabels(shape);
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"IriRef"));
		addProperty(shape,"includeSubtypes",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether to include the subtypes of the entites selected or matched");
		addProperty(shape,"includeSupertypes",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether to include the supertypes of the entity in a match clause."+
			" Used for ascending the hierarch to look for properties inherited");
		addProperty(shape,"excludeSelf",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether to exclude this entity when either include supbtypes or include suprtypes is set."+
			" Rarely used but sometimes found in ECL definitions");
		addProperty(shape,"alias",SHACL.DATATYPE,XSD.STRING,0,1,"The column name in a select clause for this property, or a variable used to reference the result set "+
			"of the values matched");
	}

	private static void pathTarget(TTEntity entity) {
	}

	private static void orderLimit(TTEntity entity) {
	}

	private static void propertySelect(TTEntity shape) {
		setLabels(shape);
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"ConceptReference"));
		addProperty(shape,"sum",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether the result is a summation of this property's values");
		addProperty(shape,"average",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether the result is an average of this property's values");
		addProperty(shape,"max",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether the result is the maximum of this property's values");
		addProperty(shape,"select",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"SelectClause"),0,1,"Nested select clause for graphql style results format");
		addProperty(shape,"inverseOf",SHACL.DATATYPE, XSD.BOOLEAN,0,1,"Indicates if true that the selected property is an inverse relationship with the target object");
		addProperty(shape,"function",SHACL.CLASS,IM.FUNCTION,0,1,"The iri of a function indicating that"+
				" the result is the result of a function operating on the property values, and any arguments passed in");
		addProperty(shape,"argument",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"Argument"),0,null,"Arguments to pass into the function");
		setOrs(shape,List.of("sum","average","max"),0,1);

	}

	private static void select(TTEntity shape){
		setLabels(shape);
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"ModelHeading"));
		addProperty(shape,"count",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"If the query result is simply a sum of the main entities found. equivalent to COUNT(id)");
		addProperty(shape,"property",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"PropertySelectClause"),1,null,"Information about a  property or field to include"+
			"in the results. Property select supports nesting with selects for the objects that may be values of the property");
		addProperty(shape,"match",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"MatchClause"),0,null,"The match pattern to which the select clause must comply."+
			" Equivalent to a where/filter in SPARQL and JOIN/WHERE in SQL");
		addProperty(shape,"distinct",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether the entity objects returned should be distinct");
		addProperty(shape,"entityType",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"ConceptReference"),1,1,"The entity type for instances this select clause operates on."+
			" Options include including subtypes.");
		addProperty(shape,"entityId",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"ConceptReference"),1,1,"An instance of an enttu for which this select clause operates." +
			" As in entity type, optionally to include subtypes, where the entity id is a concept");
		addProperty(shape,"entityIn",SHACL.CLASS,RDFS.RESOURCE,1,1,"The consept set, value set, or query result set that forms the base population of instances "+
			"on which the select clause operates. e.g. a parent cohort.");
		addProperty(shape,"groupBy",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"PropertySelectClause"),0,null,"If the results need to be grouped, the grouping properties.");
		addProperty(shape,"orderLimit",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"OrderLimitClause"),0,null,
			"Ordering of instances via a property value and limiting th enumber returned.");
		addProperty(shape,"pathToTarget",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"PathTargetClause"),1,1,"Special function for path query."+
			" Information about the target entity when the query is looking to return paths between a source and a target. Both are likely to be passed in as parameters");
		addProperty(shape,"subselect",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"SelectClause"),0,null,
			"For a query with column groups such as a list report, the select query for each group");
		setOrs(shape,List.of("property","pathToTarget"),0,1);
		setOrs(shape,List.of("entityType","entityId","entityIn"),0,1);


	}
	private static void match(TTEntity shape){
		setLabels(shape);

	}

	private static void setLabels(TTEntity shape){
		shape.addType(SHACL.NODESHAPE);
		shape.addType(OWL.CLASS);
		shape.addObject(IM.IS_CONTAINED_IN,IMO);
		shape.setName(shape.getIri().substring(shape.getIri().lastIndexOf("#")+1));

	}

	private static void heading(TTEntity shape) {
		setLabels(shape);
		shape.setDescription("General headings such as name description and alias");
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"Entity"));
		addProperty(shape,"alias",SHACL.DATATYPE,XSD.STRING,1,1,"An alias or reference term that can be used throughout a query as shorthand for the result set");
	}

	private static void argument(TTEntity shape) {
		setLabels(shape);
		shape.setDescription("A named parameter and a value for passing into a function");
		addProperty(shape,SHACL.PARAMETER,SHACL.DATATYPE,XSD.STRING,1,1,"Parameter name for a function or the parameter name for an argument");
		addProperty(shape,"valueData",SHACL.DATATYPE,XSD.STRING,1,1,"Vaue that is a literal such as a string or number");
		addProperty(shape,"valueVariable",SHACL.DATATYPE,XSD.STRING,1,1,"argumenT value which is a variable name to be resolved at run time");
		addProperty(shape,"valueSelect",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"SelectClause"),1,1,"Argument value that is the result of a select query");
		addProperty(shape,"valueIri",SHACL.NODE_KIND,SHACL.IRI,1,1,"Argument value that is an iri");
		setOrs(shape,List.of("valueData","valueVariable","valueSelect","valueIri"),1,1);
	}

	private static void pageInformation(TTEntity shape) {
		setLabels(shape);
		shape.setDescription("Information about paging if the client wishes to page results, including page number and page size");
		addProperty(shape,"pageNumber",SHACL.DATATYPE,XSD.INTEGER,1,1,"Page number (starting at 1)");
		addProperty(shape,"pageSize",SHACL.DATATYPE,XSD.INTEGER,1,1,"number of entries per page");
	}

	private static void setOrs(TTEntity shape,List<String> orProperties,Integer min, Integer max){
		List<TTNode> toRemove= new ArrayList<>();
		Integer addAt=null;
		for (TTValue prop:shape.get(SHACL.PROPERTY).getElements()) {
			if (prop.asNode().get(SHACL.OR)==null) {
				if (orProperties.contains(prop.asNode().get(SHACL.NAME).asLiteral().getValue())) {
					toRemove.add(prop.asNode());
					if (addAt == null)
						addAt = prop.asNode().get(SHACL.ORDER).asLiteral().intValue();
					else if (prop.asNode().get(SHACL.ORDER).asLiteral().intValue() < addAt)
						addAt = prop.asNode().get(SHACL.ORDER).asLiteral().intValue();
				}
			}
		}
		Collections.sort(toRemove, Comparator.comparing((TTNode p) -> p.get(SHACL.ORDER).asLiteral().intValue()));
		for (TTNode remove:toRemove)
				shape.get(SHACL.PROPERTY).remove(remove);
			TTNode newProperty= new TTNode();
			shape.addObject(SHACL.PROPERTY,newProperty);
			newProperty.set(SHACL.ORDER,TTLiteral.literal(addAt));
			if (min!=null)
				newProperty.set(SHACL.MINCOUNT,TTLiteral.literal(min));
			if (max!=null)
				newProperty.set(SHACL.MAXCOUNT,TTLiteral.literal(max));
			int order=0;
			for (TTNode remove:toRemove){
				order++;
				newProperty.addObject(SHACL.OR,remove);
				remove.set(SHACL.ORDER,TTLiteral.literal(order));
			}
	}

	private static void setPropertyEntity(TTIriRef path, TTIriRef target,String comment){
		TTEntity pEntity= manager.getEntity(path.getIri());
		String propType;
		if (target.equals(SHACL.NODE))
			propType="dataModelObjectProperty";
		else
			propType= "dataModelDataProperty";
			if (pEntity==null){
				pEntity= new TTEntity()
					.setIri(path.getIri())
					.addType(RDF.PROPERTY)
					.setName(localName(path.getIri()))
					.setDescription(comment);
				pEntity.addObject(RDFS.SUBPROPERTYOF,TTIriRef.iri(IM.NAMESPACE+propType));
				manager.getDocument().addEntity(pEntity);
			}
			pEntity.setDescription(comment);
	}
	private static String localName(String iri){
		return iri.substring(iri.lastIndexOf("#")+1);
	}
	private static void addProperty(TTEntity shape, String path, TTIriRef target,TTIriRef type,
																	Integer min,Integer max,String comment) {
		addProperty(shape,TTIriRef.iri(IM.NAMESPACE+path),target,type,min,max,comment);

	}

	private static void addProperty(TTEntity shape, TTIriRef path, TTIriRef target,TTIriRef type,
													 Integer min,Integer max,String comment){
		setPropertyEntity(path,target,comment);
		TTNode property=null;
		if (shape.get(SHACL.PROPERTY)!=null){
			for (TTValue prop: shape.get(SHACL.PROPERTY).getElements()){
				if (prop.asNode().get(SHACL.PATH).equals(path))
					property= prop.asNode();
			}
		}
		if (property==null)
			property= new TTNode();
		shape.addObject(SHACL.PROPERTY,property);
		property.set(SHACL.NAME,TTLiteral.literal(localName(path.getIri())));
		property.set(SHACL.ORDER, TTLiteral.literal(shape.get(SHACL.PROPERTY).size()));
		property.set(SHACL.PATH,path);
		property.set(target,type);
		if (min!=null)
			property.set(SHACL.MINCOUNT,TTLiteral.literal(min));
		if (max!=null)
			property.set(SHACL.MAXCOUNT,TTLiteral.literal(max));
		if (comment!=null)
			property.set(RDFS.COMMENT,TTLiteral.literal(comment));


	}
}
