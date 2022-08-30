package org.endeavourhealth.informationmanager.transforms.authored;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.endeavourhealth.imapi.model.sets.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.transforms.TTToObjectNode;
import org.endeavourhealth.imapi.vocabulary.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ModelShapes {

	private static final TTIriRef IMA= TTIriRef.iri(IM.NAMESPACE+"InformationModelAllShapes");

	private static final TTManager manager= new TTManager();


	public static void main(String[] args) throws Exception {
		String sourcePath= args[0].replace("%"," ");
		createShapes(sourcePath);
	}


	public static void createShapes(String sourcePath) throws IOException {
		loadDocument(sourcePath+"\\CoreOntology.json");
		queryRequest(getEntity(IM.NAMESPACE+"QueryRequest"));
		pageInformation(getEntity(IM.NAMESPACE+"PageInformation"));
		argument(getEntity(IM.NAMESPACE+"Argument"));
		heading(getEntity(IM.NAMESPACE+"ClauseHeading"));
		queryDef(getEntity(IM.NAMESPACE+"QueryDefinition"));
		query(getEntity(IM.NAMESPACE+"QueryShape"));
		select(getEntity(IM.NAMESPACE+"SelectClause"));
		match(getEntity(IM.NAMESPACE+"MatchClause"));
		propertyValue(getEntity(IM.NAMESPACE+"PropertyValueClause"));
		compare(getEntity(IM.NAMESPACE+"CompareClause"));
		range(getEntity(IM.NAMESPACE+"RangeClause"));
		function(getEntity(IM.NAMESPACE+"FunctionShape"));
		parameter(getEntity(IM.NAMESPACE+"Parameter"));
		functionClause(getEntity(IM.NAMESPACE+"FunctionClause"));
		propertySelect(getEntity(IM.NAMESPACE+"PropertySelectClause"));
		orderLimit(getEntity(IM.NAMESPACE+"OrderLimitClause"));
		pathTarget(getEntity(IM.NAMESPACE+"PathTargetClause"));
		alias(getEntity(IM.NAMESPACE+"IriAlias"));
		conceptReference(getEntity(IM.NAMESPACE+"ConceptReference"));
		propertyNode(getEntity(IM.NAMESPACE+"PropertyNodeShape"));
		transactionEntity(getEntity(IM.NAMESPACE+"EntityFileTransaction"));
		transactionDocument(getEntity(IM.NAMESPACE+"EntityDocument"));
		saveDocument(sourcePath+"\\CoreOntology.json");
	}

	private static void query(TTEntity shape) {
		setLabels(shape);
		shape.setDescription("A query that is stored as an entity in a query library");
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"EntityShape"));
		shape.addObject(IM.IS_CONTAINED_IN, TTIriRef.iri(IM.NAMESPACE+"QueryShapes"));
		shape.set(IM.ORDER,TTLiteral.literal(1));
		shape.set(SHACL.TARGETCLASS,IM.QUERY);
		addProperty(shape,"query",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"QueryDefinition"),0,1,"The query definition itself");
	}

	private static void transactionDocument(TTEntity shape) {
		setLabels(shape);
		shape.setDescription("A document containing any number of triples to file as a batch."+
			"<br>Note that if the document is sent as Json-LD with prefixed iris, a @context object will be required");
		shape.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"TransactionalShapes"));
		shape.set(IM.ORDER,TTLiteral.literal(1));
		addProperty(shape,"crud",SHACL.CLASS, TTIriRef.iri(IM.NAMESPACE+"CrudOperation"),1,1,"Indicates the nature of the default CRUD transaction for entities in this"+
		"document. Thes can be overridden in each entity. Must be  one of:"+
			" im:DeleteAll, im:AddQuads (adds in additional triples), im:UpdateAll (replaces all the predicates for this entity in the graph with te ones submitted), im:UpdatePredicates "+
			"( replaces all the objects of these submitted predicates for this graph");
		addProperty(shape,"graph",SHACL.CLASS, TTIriRef.iri(IM.NAMESPACE+"Graph"),1,1,"The graph to which these entities apply by default. "+
			"<br>This may be overridden by the entities"+
			"<br>This means you can add predicates to any entity without affecting the original authored entity, those predicates belonging only to this module or graph");
		addProperty(shape,"entities",SHACL.NODE, TTIriRef.iri(IM.NAMESPACE+"EntityShape"),1,null,"Set of entities to file. If the entities do not have crud or graphs of"+
			"their own then the default from the document are used");

	}

	private static void transactionEntity(TTEntity shape) {
		setLabels(shape);
		shape.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"TransactionalShapes"));
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"EntityShape"));
		shape.set(IM.ORDER,TTLiteral.literal(1));
		shape.setDescription("An entity with the additional CRUD indicators to enable deletes updates, adding quads etc");
		addProperty(shape,"crud",SHACL.CLASS, TTIriRef.iri(IM.NAMESPACE+"CrudOperation"),1,1,"Indicates the nature of the CRUD transaction which must be one of"+
			" im:DeleteAll, im:AddQuads (adds in additional triples), im:UpdateAll (replaces all the predicates for this entity in the graph with te ones submitted), im:UpdatePredicates "+
			"( replaces all the objects of these submitted predicates for this graph");
		addProperty(shape,"graph",SHACL.CLASS, TTIriRef.iri(IM.NAMESPACE+"Graph"),1,1,"The graph to which these triples apply. "+
			"<br>This means you can add predicates to any entity without affecting the original authored entity, those predicates belonging only to this module or graph");
	}

	private static void propertyNode(TTEntity shape) {
		setLabels(shape);
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"EntityShape"));
		shape.setDescription("The data model of a shacl node shape describing a property of a data model entity");
		addProperty(shape,SHACL.PATH,SHACL.CLASS,RDF.PROPERTY,1,1,"The iri of the property");
		addProperty(shape,SHACL.MINCOUNT,SHACL.DATATYPE,XSD.INTEGER,0,1,"Minimum cardinality,if null assuming any number including zero");
		addProperty(shape,SHACL.MAXCOUNT,SHACL.DATATYPE,XSD.INTEGER,0,1,"The maximum number allowed. If null then any number");
		addProperty(shape,SHACL.CLASS,SHACL.CLASS,RDFS.RESOURCE,1,1,"The iri for the class range of the property. The range of this property is a subclass of this class");
		addProperty(shape,SHACL.DATATYPE,SHACL.CLASS,RDFS.RESOURCE,1,1,"The range of the property is a data type of this type");
		addProperty(shape,SHACL.NODE,SHACL.CLASS,RDFS.RESOURCE,1,1,"The property points to a node shape");
		addProperty(shape,RDFS.COMMENT,SHACL.DATATYPE,XSD.STRING,0,1,"Description of the property");
		addProperty(shape,SHACL.NAME,SHACL.DATATYPE,XSD.STRING,0,1,"The name of the property used for ease of recognition");
		addProperty(shape,SHACL.ORDER,SHACL.DATATYPE,XSD.INTEGER,0,1,"The property order for display");
		addProperty(shape,IM.INHERITED_FROM,SHACL.NODE,SHACL.NODESHAPE,0,1,"The shape that this property is inherited from (used in the inferred instance of the information model");
		setOrs(shape,List.of("class","datatype","node"),1,1);
	}

	private static void alias(TTEntity shape) {
		setLabels(shape);
		shape.setDescription("An IRI with a name and an alias (for use as variables for reference");
		shape.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"BasicShapes"));
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"IriRef"));
		shape.set(IM.ORDER,TTLiteral.literal(2));
		addProperty(shape,"alias",SHACL.DATATYPE,XSD.STRING,0,1,"The column name in a select clause for this property, or a variable used to reference the result set "+
			"of the values matched");

	}

	private static void propertyValue(TTEntity shape) {
		setLabels(shape);
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"ConceptReference"));
		shape.setDescription("A property (as a concept reference) and value filter supporting ranges sets and functions,"+
			"<br>As a result of the concept references, supports sub properties as well as inferred values");
		addProperty(shape,"pathTo",SHACL.CLASS,RDFS.RESOURCE,0,1,"A convenient way of traversing properties and objects to get to a property, avoiding excessive nesting."+
			"<br>The properties are listed and thus traversed in strict order");
		addProperty(shape,"inverseOf",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Tests the property as an inverse property (inbound relationship)");
		addProperty(shape,"notExist",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"The property match must not exist. This is negatation at a more granular level than the match clause."+
			"<br>For example to test for a null field or absent property");
		addProperty(shape,"optional",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Optional match for this property, used to enable IF a present then A must have X,Y."+
			"<br> N.B for SPARQL experts the OPTIONAL is generally generated automatically from SELECT,  so optional need only be set when a test is to be applied to a linked object");
		addProperty(shape,"function",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"FunctionClause"),1,1,"A function that operates on the property value (and other parameters) "+
			"prior to a compare or range or inclusion test. For example a time difference function operating on the date and a reference date."+
			"<br>Note that properties that ARE functions do not need functions included for example age. For these simply supply the arguments.");

		addProperty(shape,"argument",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"Argument"),0,1,"Arguments to pass into a function when the property is a function property."+
			"<br>Note that if the test is a function then the argument list would be in the function clause");
		addProperty(shape,"value",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"CompareClause"),1,1,"If testing a property value as equal greater than ete. use compare");
		addProperty(shape,"inSet",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"ConceptReference"),1,null,"The value of the property must be in the concept set IRI."+
			"<br>For teesting simple lists or single concepts use isConcept");
		addProperty(shape,"notInSet",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"ConceptReference"),1,null,"The value of the property must NOT be in the concept set IRI."+
			"<br> Note that this is not negation of the entire match, only the negation of this entry instance when tested. Equivalent to SPARQL filter not in."+
			"<br> For testing simple lists of concepts use isNotConcept");
		addProperty(shape,"isConcept",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"ConceptReference"),1,null,"The value of the property be this concept (and otionally its sub types)."+
			"<br> Used for inline sets or single concepts");
		addProperty(shape,"isConcept",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"ConceptReference"),1,null,"The value of the property be this concept (and otionally its sub types)."+
			"<br> Used for inline sets or single concepts");
		addProperty(shape,"isNotConcept",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"ConceptReference"),1,null,"The value of the property must NOT be this concept (and otionally its sub types)."+
			"<br> Used for inline sets or single concepts."+
			"<br>Note that this is not negation of the entire match, only the negation of this entry instance when tested. Equivalent to SPARQL filter not in.");

		addProperty(shape,"inRange",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"RangeClause"),1,1,"Test foe a value being between two absolute or relative values");
		addProperty(shape,"valueMatch",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"MatchClause"),1,1,"The match clause operating on the property value object."+
			"<br> Equivalent to a join in SQL or '.' in sparql");

		addProperty(shape,"displayText",SHACL.DATATYPE,XSD.STRING,0,1,"Optional text for display in query viewer if dfferent from the match clause name");
		setOrs(shape,List.of("value","inSet","notInSet","isConcept","isNotConcept","inRange","valueMatch"),0,1);



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
	private static void compare(TTEntity shape) {
		setLabels(shape);
		shape.setDescription("Tests a value, whether equal, greater than, less than etc.");
		addProperty(shape,"comparison",SHACL.DATATYPE,XSD.STRING,1,1,"Comparison operators : EQUAL," +
			" LESS_THAN," +
			" LESS_THAN_OR_EQUAL," +
			" GREATER_THAN," +
			" NOT_EQUAL");
		addProperty(shape,"valueData",SHACL.DATATYPE,XSD.STRING,1,1,"The value to compare against. This is a string which will be cast to a number or date, depending "+
			"on the data type of the property value");
		addProperty(shape,"valueVariable",SHACL.DATATYPE,XSD.STRING,1,1,"The variable (alias) to compare against. The variable may be an argument to the query (e.g. $reference date"+
			" or an alias in this query or another query. Use the $ prefix to make it clear");
		addProperty(shape,"valueSelect",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"SelectClause"),1,1,"The result to compare the property value against is derived from a"+
				" select sub query");
		setOrs(shape,List.of("valueData","valueVariable","valueSelect"),1,1);

	}
	private static void range(TTEntity shape) {
		setLabels(shape);
		shape.setDescription("A range for use in property value testing");
		addProperty(shape,"from",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"CompareClause"),1,1,"The value comparison for lower end of the range");
		addProperty(shape,"to",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"CompareClause"),1,1,"The value comparison of upper end of the range");

	}

	private static void queryDef(TTEntity shape) {
		shape.addType(SHACL.NODESHAPE);
		shape.addType(OWL.CLASS);
		shape.addObject(IM.IS_CONTAINED_IN,IMA);
		shape.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"QueryShapes"));
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"ClauseHeading"));
		shape.set(IM.ORDER,TTLiteral.literal(2));
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
		setLabels(shape);
		shape.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"TransactionalShapes"));
		shape.setDescription("A request for data sent as a  body (json in local name format) to the /queryIM API.<br>Contains either a query or query iri with run time variable values for use in the query");
		shape.setName("Query Request");
		shape.set(IM.ORDER,TTLiteral.literal(3));
		addProperty(shape,"page",SHACL.NODE, TTIriRef.iri(IM.NAMESPACE+"PageInformation"),null,1,"optional page number and size if the client is looking for paged results");
		addProperty(shape,"textSearch",SHACL.DATATYPE, XSD.STRING,null,1,"If a free text search is part of the query");
		addProperty(shape,"argument",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"Argument"),null,null,"arguments to pass in to the query as parameters. Parameter name and value pairs. Values ,may be strings, numbers or IRIs");
		addProperty(shape,"query",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"QueryDefinition"),1,1,"The query definition for an inline dynamic query. If the query is already defined, use the queryIri. Arguments may be used in eoither approach");
		addProperty(shape,"queryIri",SHACL.CLASS,IM.QUERY,1,1,"The IRI of a predefined query in the information model. i.e. request the system to run query X");
		addProperty(shape,"referenceDate",SHACL.DATATYPE,TTIriRef.iri(IM.NAMESPACE+"DateTime"),null,1,"reference date for date ranges that use reference date. Note that it could be passed in as an argeument");
		setOrs(shape,List.of("query","queryIri"),1,1);

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
		shape.setDescription("A function used in a query consisting of the function iri and one or more arguments to pass in at run time");
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"IriRef"));
		addProperty(shape,"argument",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"Argument"),0,null,"Arguments to pass into the function. They should match the "+
			"parameter definitions of the function");
	}

	private static void function(TTEntity shape) {
		setLabels(shape);
		shape.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"QueryShapes"));
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"EntityShape"));
		shape.set(IM.ORDER,TTLiteral.literal(2));
		shape.set(SHACL.TARGETCLASS,IM.FUNCTION);
		addProperty(shape,SHACL.PARAMETER,SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"Parameter"),0,null,"A list of parameters and data types used in this function");
	}


	private static void conceptReference(TTEntity shape) {
		setLabels(shape);
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"IriAlias"));
		addProperty(shape,"includeSubtypes",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether to include the subtypes of the entites selected or matched");
		addProperty(shape,"includeSupertypes",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether to include the supertypes of the entity in a match clause."+
			" Used for ascending the hierarch to look for properties inherited");
		addProperty(shape,"excludeSelf",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether to exclude this entity when either include supbtypes or include suprtypes is set."+
			" Rarely used but sometimes found in ECL definitions");

	}

	private static void pathTarget(TTEntity shape) {
		setLabels(shape);
		shape.setDescription("Information about the target of a path query, including the IRI of the target and the number of hops processed.");
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"IriRef"));
		addProperty(shape,"depth",SHACL.DATATYPE,XSD.INTEGER,0,1,"How many hops to be taken in the graph between source and target");
	}

	private static void orderLimit(TTEntity shape) {
		setLabels(shape);
		shape.setDescription("Orders the value of the property (property as represented by the iri and alias)");
		addProperty(shape,"orderBy",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"IriAlias"),1,1,"The property or property variable to order the results by");
		addProperty(shape,"direction",SHACL.DATATYPE,XSD.STRING,1,1,"ASCENDING or DESCENDING to indicate the order direction");
		addProperty(shape,"limit",SHACL.DATATYPE,XSD.INTEGER,0,1,"The count of instances to return after ordering");

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
		shape.setDescription("Defines the objects and properties to retrieve from a graph, subject to a mach clause. Supports graphql type nesting and subselsects for column groups");
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"ClauseHeading"));
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
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"ClauseHeading"));
		shape.setDescription("A clause containing criteria which the objects must conform to. Equivalent to a from/join where clause in SQL and where/filter in sparql."+
			"<br>Supports graph traversal filtering and inference for subsumption query");
		addProperty(shape,"pathTo",SHACL.CLASS,RDFS.RESOURCE,0,1,"A convenient way of traversing properties and objects to get to the match object, avoiding excessive nesting."+
			"<br>The properties are listed and thus traversed in strict order");
		addProperty(shape,"or",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"MatchClause"),2,null,"Boolean operator OR on subclauses");
		addProperty(shape,"and",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"MatchClause"),2,null,"Boolean operator AND on subclauses");
		addProperty(shape,"entityType",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"ConceptReference"),1,1,"The entity type that matched instances must be (including subtypes)."+
			" Options include including subtypes.");
		addProperty(shape,"entityId",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"ConceptReference"),1,1,"An instance of an entitu for which this match clause operates on." +
			"  As in entity type, optionally to include subtypes, where the entity id is a concept");
		addProperty(shape,"entityInSet",SHACL.CLASS,RDFS.RESOURCE,1,1,"The concept set, value set, or query result set that the instances must be in to match."+
			"<br>on which the select clause operates. e.g. a parent cohort.");
		addProperty(shape,"entityNotInSet",SHACL.CLASS,RDFS.RESOURCE,1,1,"The concept set, value set, or query result set that the instances must NOT be in to match."+
			"<br>on which the select clause operates. e.g. a parent cohort.");

		addProperty(shape,"graph",SHACL.CLASS,IM.GRAPH,0,1,"The iri of a graph if the query is limied to a particular data set");
		addProperty(shape,"property",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"PropertyValueClause"),0,null,
			"Properties and their values required to match (or not match), including any nesting of objects (joins)."+
			"<br>The default assumption is the AND operator on the properties. Use orProperty for or operator");
		addProperty(shape,"orProperty",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"PropertyValueClause"),2,null,
			"Properties and their values one of which is required to match (or not match), including any nesting of objects (joins).");
		addProperty(shape,"notExist",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Negation indicator for this entire clause. i.e. for the instance to be included it must NOT match.");
		addProperty(shape,"orderLimit",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"OrderLimitClause"),0,1,"Ability to order and limit the match before the select or matchstatement operates."
		+"<br> Crucially, this is processed before the application of a test or check, enabling things like latest or earliest, max or min");
		addProperty(shape,"testProperty",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"PropertyValueClause"),0,null,"Further test applied to the result of an ordered limited match."+
			"<br> The rest of the match clause then becomes a sub select query on which these tests operate. Implicit operator is AND");
		addProperty(shape,"displayText",SHACL.DATATYPE,XSD.STRING,0,1,"Optional text for display in query viewer if dfferent from the match clause name");

		setOrs(shape,List.of("or","and"),0,1);
		setOrs(shape,List.of("entityType","entityId"),0,1);
		setOrs(shape,List.of("entityInSet","entityNotInSet"),0,1);

	}

	private static void setLabels(TTEntity shape){
		shape.getPredicateMap().clear();
		shape.addType(SHACL.NODESHAPE);
		shape.addType(OWL.CLASS);
		shape.addObject(IM.IS_CONTAINED_IN,IMA);
		shape.setName(shape.getIri().substring(shape.getIri().lastIndexOf("#")+1));

	}

	private static void heading(TTEntity shape) {
		setLabels(shape);
		shape.setDescription("General headings such as name description and alias");
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"EntityShape"));
		addProperty(shape,"alias",SHACL.DATATYPE,XSD.STRING,1,1,"An alias or reference term that can be used throughout a query as shorthand for the result set");
	}

	private static void argument(TTEntity shape) {
		setLabels(shape);
		shape.setDescription("A named parameter and a value for passing into a function");
		addProperty(shape,SHACL.PARAMETER,SHACL.DATATYPE,XSD.STRING,1,1,"Parameter name for a function or the parameter name for an argument");
		addProperty(shape,"valueData",SHACL.DATATYPE,XSD.STRING,1,1,"Vaue that is a literal such as a string or number");
		addProperty(shape,"valueVariable",SHACL.DATATYPE,XSD.STRING,1,1,"argumenT value which is a variable name to be resolved at run time");
		addProperty(shape,"valueSelect",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"SelectClause"),1,1,"Argument value that is the result of a select query");
		addProperty(shape,"valueIri",SHACL.CLASS,TTIriRef.iri(IM.NAMESPACE+"IriRef"),1,1,"Argument value that is an iri");
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
