package org.endeavourhealth.informationmanager.transforms.authored;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.zip.DataFormatException;

public class ModelShapes {

	private static final TTIriRef IMA= TTIriRef.iri(IM.NAMESPACE+"InformationModelAllShapes");

	private static final TTManager manager= new TTManager();
	
	private String sourcePath;


	public static void main(String[] args) throws Exception {
		String sourcePath= args[0].replace("%"," ");
		new ModelShapes().createShapes(sourcePath);
	}


	public void createShapes(String sourcePath) throws IOException, DataFormatException {
		this.sourcePath= sourcePath;
		loadDocument(sourcePath+"\\CoreOntology.json");
		queryRequest(getEntity(IM.NAMESPACE+"QueryRequest"));
		pageInformation(getEntity(IM.NAMESPACE+"PageInformation"));
		argument(getEntity(IM.NAMESPACE+"Argument"));
		queryDef(getEntity(IM.NAMESPACE+"QueryDefinition"));
		from(getEntity(IM.NAMESPACE+"FromClause"));
		query(getEntity(IM.NAMESPACE+"QueryShape"));
		select(getEntity(IM.NAMESPACE+"SelectClause"));
		where(getEntity(IM.NAMESPACE+"WhereClause"));
		compare(getEntity(IM.NAMESPACE+"CompareClause"));
		value(getEntity(IM.NAMESPACE+"ValueClause"));
		range(getEntity(IM.NAMESPACE+"RangeClause"));
		function(getEntity(IM.NAMESPACE+"FunctionShape"));
		parameter(getEntity(IM.NAMESPACE+"Parameter"));
		functionClause(getEntity(IM.NAMESPACE+"FunctionClause"));
		alias(getEntity(IM.NAMESPACE+"IriAlias"));
		conceptReference(getEntity(IM.NAMESPACE+"ConceptReference"));
		propertyNode(getEntity(IM.NAMESPACE+"PropertyNodeShape"));
		transactionEntity(getEntity(IM.NAMESPACE+"EntityFileTransaction"));
		transactionDocument(getEntity(IM.NAMESPACE+"EntityDocument"));
		pathQuery(getEntity(IM.NAMESPACE+"PathQueryShape"));
		prefix(getEntity(IM.NAMESPACE+"PrefixShape"));
		saveDocument(sourcePath+"\\CoreOntology.json");
	}

	private void prefix(TTEntity shape) throws JsonProcessingException {
		setLabels(shape);
		shape.setName("Prefix shape");
		shape.set(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"InformationModelAllShapes"));
		shape.setDescription("The model of a class containing an iri prefix map");
		addProperty(shape,"prefix",SHACL.DATATYPE,XSD.STRING,1,1,"Prefixe used in the rest of the document");
		addProperty(shape,"iri",SHACL.DATATYPE,XSD.STRING,1,1,"The iri or namespace or graph iri this prefix refers to");

	}

	private void pathQuery(TTEntity shape) throws JsonProcessingException {
		setLabels(shape);
		shape.set(SHACL.TARGETCLASS,TTIriRef.iri(IM.NAMESPACE+"PathQuery"));
		shape.set(IM.ORDER,TTLiteral.literal(4));
		shape.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"QueryShapes"));
		shape.setDescription("A query that returns a set pf paths between a source and target entity, traversing to a certain level");
		addProperty(shape,"source",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"IriRef"),1,1,"the source entity at the start of the path.");
		addProperty(shape,"target",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"IriRef"),1,1,"the target entity at the end of the path.");
		addProperty(shape,"depth",SHACL.DATATYPE,XSD.INTEGER,0,1,"How many hops to be taken in the graph between source and target");
	}

	private void query(TTEntity shape) throws JsonProcessingException {
		setLabels(shape);
		shape.setName("Query /Set shape");
		shape.setDescription("A query that is stored as an entity in a query library");
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"EntityShape"));
		shape.addObject(IM.IS_CONTAINED_IN, TTIriRef.iri(IM.NAMESPACE+"QueryShapes"));
		shape.set(IM.ORDER,TTLiteral.literal(1));
		shape.set(SHACL.TARGETCLASS,IM.QUERY);
		addProperty(shape,"definition",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"QueryDefinition"),0,1,"The query definition itself");
	}

	private void transactionDocument(TTEntity shape) throws JsonProcessingException {
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

	private void transactionEntity(TTEntity shape) throws JsonProcessingException {
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

	private void propertyNode(TTEntity shape) throws JsonProcessingException, DataFormatException {
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

	private void alias(TTEntity shape) throws JsonProcessingException {
		setLabels(shape);
		shape.setDescription("An IRI with a name and an optional alias  and a variable name when the iri is passed in as an argument (e.g. $this");
		shape.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"BasicShapes"));
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"IriRef"));
		shape.set(IM.ORDER,TTLiteral.literal(2));
		addProperty(shape,"alias",SHACL.DATATYPE,XSD.STRING,0,1,"The column name in a select clause for this property, or a variable used to reference the result set "+
			"of the values matched");
		addProperty(shape,"variable",SHACL.DATATYPE,XSD.STRING,0,1,"The name of a variable, passed as an argument to the query, which is resolved to the IRI");
		addProperty(shape,"inverse",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"When  used as a property, whether this is an inverse object property i.e. an inbound connection to this entity");
		addProperty(shape,"includeSubtypes",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"When used in a with or where clause, whether to include the subtypes of this entity");
		addProperty(shape,"includeSupertypes",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"When used in a with or where clause, whether to include the supertypes of this entity e.g. when ascending a hierrchy to look for a property");

	}


	private static void loadDocument(String file) throws IOException {
		manager.loadDocument(new File(file));
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

	private void saveDocument(String file) throws JsonProcessingException {
		manager.saveDocument(new File(file));
	}
	private void value(TTEntity shape) throws JsonProcessingException {
		setLabels(shape);
		shape.setDescription("Tests a value, whether equal, greater than, less than etc. optionally including a value to compare against");
		addProperty(shape,"comparison",SHACL.DATATYPE,XSD.STRING,1,1,"Comparison operators : =," +
			" <," +
			" <=," +
			" >," +
			" >=");
		addProperty(shape,"value",SHACL.DATATYPE,XSD.STRING,1,1,"The value of the property used in a test. This is a string which will be cast to a number or date, depending "+
			"on the data type of the property value");
		addProperty(shape,"relativeTo",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"CompareClause"),0,1,"The result to compare the property value against is derived from a"+
				" variable value or property of an object (identified by its alias) defined another where clause");

	}
	private void compare(TTEntity shape) throws JsonProcessingException {
		setLabels(shape);
		addProperty(shape,"alias",SHACL.DATATYPE,XSD.STRING,0,1,"The alias of the result set from another where clause");
		addProperty(shape,"property",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"IriAlias"),0,1,"The property of the objects in the result set (as indicated by the alias) to test");
		addProperty(shape,"variable",SHACL.DATATYPE,XSD.STRING,0,1,"If a value is being compared against a variable passed in as an argument, the name of the variable");



	}
	private void range(TTEntity shape) throws JsonProcessingException {
		setLabels(shape);
		shape.setDescription("A range for use in property value testing");
		addProperty(shape,"from",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"ValueClause"),1,1,"The value comparison for lower end of the range");
		addProperty(shape,"to",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"ValueClause"),1,1,"The value comparison of upper end of the range");
		addProperty(shape,"relativeTo",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"CompareClause"),1,1,"When the range values are relative to another value , information about the other value (e.g. variable"+
			" or property of the results defined in another clause");

	}

	private void queryDef(TTEntity shape) throws JsonProcessingException {
		shape.addType(SHACL.NODESHAPE);
		shape.addType(OWL.CLASS);
		shape.setName("Query /Set definition");
		shape.setDescription("A set definition holding the logical definition of a set. Usually referred to as a Query as these are used to retrieve data."+
			"<br>Includes most of  the main logical query constructs used in mainstream query languages, thus is a constrained version of mainstream languages that is schema independent.");
		shape.addObject(IM.IS_CONTAINED_IN,IMA);
		shape.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"QueryShapes"));
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"IriAlias"));
		shape.set(IM.ORDER,TTLiteral.literal(2));
		addProperty(shape,"description",SHACL.DATATYPE,XSD.STRING,0,1,"Optional description of the query definition for support purposes.");

		addProperty(shape,"from",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"FromClause"),0,null,"The base cohort/ set or type, or instance on which all the subsequent where or filter clauses operate. If more than one"+
			" this is treated as an OR list.");
		addProperty(shape,"where",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"WhereClause"),0,1,
			"Tests properties and property paths and applies filters. Equivalent to SQL Join/ Where and SPARQL Where"+
			"<br>Bollean where clauses supported.");
		addProperty(shape,"select",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"SelectClause"),1,1,
			"Select query clause logically similar to SQL/SPARQL select but with GraphQL nesting ability");
		addProperty(shape,"groupBy",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"IriAlias"),0,null,"If the results need to be grouped, the grouping properties.");
		addProperty(shape,"orderBy",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"IriAlias"),0,null,
			"Ordering of instances via a property value returned.");
		addProperty(shape,"direction",SHACL.DATATYPE,XSD.STRING,0,1,
			"direction of ordering (DESC or ASC) .");
		addProperty(shape,"limit",SHACL.DATATYPE,XSD.INTEGER,0,1,
			"Number of entities to return. Normally used with order by");
		addProperty(shape,"subQuery",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"QueryDefinition"),0,null,"SubQueries used to group columns in multi group reports. The sub queries are all subsets of the main query clauses");
		addProperty(shape,"prefix",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"PrefixShape"),0,0,"list of prefix to namespace expansion to enable readability of iris");
		addProperty(shape,"usePrefixes",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"true if you want the results to use IRI prefixes");
		addProperty(shape,"activeOnly",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether only active entities are included in the match clauses or select clauses");

	}

	private void queryRequest(TTEntity shape) throws JsonProcessingException, DataFormatException {
		setLabels(shape);
		shape.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"TransactionalShapes"));
		shape.setDescription("A request for data sent as a  body (json in local name format) to the /queryIM API.<br>Contains a query as an iri or inline query with run time variable values as arguments for use in the query");
		shape.setName("Query Request");
		shape.set(IM.ORDER,TTLiteral.literal(3));
		addProperty(shape,"name",SHACL.DATATYPE, XSD.STRING,null,1,"optional name for debugging purposes. Is not used in the query process");
		addProperty(shape,"page",SHACL.NODE, TTIriRef.iri(IM.NAMESPACE+"PageInformation"),null,1,"optional page number and size if the client is looking for paged results");
		addProperty(shape,"textSearch",SHACL.DATATYPE, XSD.STRING,null,1,"If a free text search is part of the query");
		addProperty(shape,"argument",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"Argument"),null,null,"arguments to pass in to the query as parameters. Parameter name and value pairs. Values ,may be strings, numbers or IRIs");
		addProperty(shape,"query",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"QueryDefinition"),1,1,"The requested query, either by iri reference or inline definition If the query is already defined, use the queryIri. Arguments may be used in eoither approach");
		addProperty(shape,"referenceDate",SHACL.DATATYPE,TTIriRef.iri(IM.NAMESPACE+"DateTime"),null,1,"reference date for date ranges that use reference date. (Note that it could be passed in as an argument)");
		setOrs(shape,List.of("query","queryIri"),1,1);

	}

	private void parameter(TTEntity shape) throws JsonProcessingException, DataFormatException {
		setLabels(shape);
		addProperty(shape,RDFS.LABEL,SHACL.DATATYPE,XSD.STRING,1,1,"The name of the parameter");
		addProperty(shape,SHACL.DATATYPE,SHACL.CLASS,RDFS.RESOURCE,1,1,"The iri of The data type of the parameter when the data type is a literal");
		addProperty(shape,SHACL.CLASS,SHACL.CLASS,RDFS.RESOURCE,1,1,"The iri of the class of the parameter when the argument is an object");
		setOrs(shape,List.of("datatype","class"),1,1);
	}

	private void functionClause(TTEntity shape) throws JsonProcessingException {
		setLabels(shape);
		shape.setDescription("A function used in a query consisting of the function iri and one or more arguments to pass in at run time");
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"IriRef"));
		addProperty(shape,"argument",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"Argument"),0,null,"Arguments to pass into the function. They should match the "+
			"parameter definitions of the function");
	}

	private void function(TTEntity shape) throws JsonProcessingException {
		setLabels(shape);
		shape.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"QueryShapes"));
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"EntityShape"));
		shape.set(IM.ORDER,TTLiteral.literal(2));
		shape.set(SHACL.TARGETCLASS,IM.FUNCTION);
		addProperty(shape,SHACL.PARAMETER,SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"Parameter"),0,null,"A list of parameters and data types used in this function");
	}


	private void conceptReference(TTEntity shape) throws JsonProcessingException {
		setLabels(shape);
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"IriAlias"));
		addProperty(shape,"includeSubtypes",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether to include the subtypes of the entities selected or matched");
		addProperty(shape,"includeSupertypes",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether to include the supertypes of the entity in a match clause."+
			" Used for ascending the hierarch to look for properties inherited");
		addProperty(shape,"excludeSelf",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether to exclude this entity when either include subtypes or include supertypes is set."+
			" Rarely used but sometimes found in ECL definitions");

	}





	private void select(TTEntity shape) throws JsonProcessingException, DataFormatException {
		setLabels(shape);
		shape.setDescription("Defines the objects and properties to retrieve from a graph, subject to a mach clause. Supports graphql type nesting and sub selects for column groups");
		shape.getPredicateMap().remove(RDFS.SUBCLASSOF);
		addProperty(shape,"count",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"If the query result is simply a sum of the main entities found. equivalent to COUNT(id)");
		addProperty(shape,"path",SHACL.CLASS,RDFS.RESOURCE,0,1,"A property path made up of space delimited iri strings, from the outer entity to the entity on which this clause operates."+
				"<br> Shortcut for nested selects");
		addProperty(shape,"property",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"IriAlias"),1,1,"Information about a  property or field to include"+
			" in the results. Property select supports nesting with selects for the objects that may be values of the property");
		addProperty(shape,"select",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"SelectClause"),0,null,"Nested select property clauses from the objects that are values of this select's property"+
			" Note that if the value is null then this select would be absent");
		addProperty(shape,"where",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"WhereClause"),0,null,"Nested where clause operating on the values of this select's property"+
			"<br>Enables multi- level filtering as used in JOIN where clauses in SQL");
		addProperty(shape,"sum",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether the result is a summation of this property's values");
		addProperty(shape,"average",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether the result is an average of this property's values");
		addProperty(shape,"max",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Whether the result is the maximum of this property's values");
		addProperty(shape,"function",SHACL.CLASS,IM.FUNCTION,0,1,"The iri of a function indicating that"+
			" the result is the result of a function operating on the property values, and any arguments passed in");
		addProperty(shape,"argument",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"Argument"),0,null,"Arguments to pass into the function");
		setOrs(shape,List.of("sum","average","max"),0,1);


	}

	private void from(TTEntity shape) throws JsonProcessingException, DataFormatException {
		setLabels(shape);
		shape.setName("From clause");
		shape.set(RDFS.SUBCLASSOF,TTIriRef.iri(IM.NAMESPACE+"IriAlias"));
		shape.setDescription("A clause defining a base set , or a base type(s), or an object instance, on which the where clause operates."+
			"<br>Set the alias if derived from a result set in the same query. Set the iri if derived from an external result. by default the iri refers to an instance. For querying instances of a type set the is type property as true and for a base cohort, set isSet as true");
		addProperty(shape,"isType",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"If the query results are derived from instances of certain type (or types) then set this flag to true.");
		addProperty(shape,"isSet",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"If the query results derived the result set of a concept set, value set or query result then set this flag to true.");
		setOrs(shape,List.of("isType","isSet"),1,1);

	}
	private void where(TTEntity shape) throws JsonProcessingException {
		setLabels(shape);
		shape.setDescription("A clause containing criteria which the objects must conform to. Equivalent to a from/join where clause in SQL and where/filter in sparql."+
			"<br>Supports graph traversal filtering and inference for subsumption query");
		addProperty(shape,"alias",SHACL.DATATYPE,XSD.STRING,0,1,"Used to define the clause with a readable term and also used in other clauses for further refinement");
		addProperty(shape,"description",SHACL.DATATYPE,XSD.STRING,0,1,"Optional description for clause");
		addProperty(shape,"from",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"FromClause"),0,null,"Refers to the alias of another where clause to indicate the set of objects defined by the referenced clause, which will be further refined by this where clause."+
			"<br>Equivalent to accessing a temporary or derived table in SQL."+
			"<br>Also used to refer to an instance object, or instances of a certain type, or external result set (e.g. a base cohort population)"+
			"<br>If more than one it is considered an OR List");
		addProperty(shape,"graph",SHACL.CLASS,IM.GRAPH,0,1,"The iri of a graph if the query is limited to a particular data set");
		addProperty(shape,"path",SHACL.CLASS,RDFS.RESOURCE,0,1,"A property path made up of space delimited iri strings, from the outer entity to the entity on which this clause operates."+
			"<br>Equivalent to an inner join in SQL");
		addProperty(shape,"notExist",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"WhereClause"),0,1,"Points to a nested where clause. If the nested clause returns results then the entity referenced by the outer where clause is excluded."+
			"<br>In other words, for the outer entity to be included, the nested entity must have no results. This contrasts with 'not'");
		addProperty(shape,"or",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"WhereClause"),2,null,"Boolean operator OR on subclauses");
		addProperty(shape,"and",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"WhereClause"),2,null,"Boolean operator AND on subclauses");

		addProperty(shape,"property",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"IriAlias"),0,null,
			"a property tgo test. Note that this is applied to the entity after the path has been resolved.");

		addProperty(shape,"in",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"IriRef"),0,null,
			"Whether the value is in this list of sets or concepts (including any sub types)");
		addProperty(shape,"is",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"IriAlias"),0,null,
			"Whether the value is this concept (and optionally if set to include sub or supertypes)");
		addProperty(shape,"not",SHACL.DATATYPE,XSD.BOOLEAN,0,1,"Testing for the value as a not. Note that this filters out the instances but does not exclude the outer entity (see not exist)");
		addProperty(shape,"function",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"FunctionClause"),1,1,"A function that operates on the property value (and other parameters) "+
			"prior to a compare or range or inclusion test. For example a time difference function operating on the date and a reference date."+
			"<br>Note that properties that ARE functions do not need functions included for example age. For these simply supply the arguments.");
		addProperty(shape,"argument",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"Argument"),0,1,"Arguments to pass into a function when the property is a function property."+
			"<br>Note that if the test is a function then the argument list would be in the function clause");
		addProperty(shape,"value",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"ValueClause"),1,1,"If testing a property value as equal greater than ete. use compare");
		addProperty(shape,"range",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"RangeClause"),1,1,"Test foe a value being between two absolute or relative values");
		addProperty(shape,"where",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"WhereClause"),1,1,"A chained where clause operating on the property value object."+
			"<br> Equivalent to a join in SQL or '.' in sparql");
		addProperty(shape,"orderBy",SHACL.NODE,TTIriRef.iri(IM.NAMESPACE+"IriAlias"),0,null,
			"Ordering of instances via a property value in order to further test in another where clause.");
		addProperty(shape,"direction",SHACL.DATATYPE,XSD.STRING,0,1,
			"direction of ordering (DESC or ASC) .");
		addProperty(shape,"limit",SHACL.DATATYPE,XSD.INTEGER,0,1,
			"Number of entities to return. Normally used with order by");


	}

	private void setLabels(TTEntity shape){
		shape.getPredicateMap().clear();
		shape.addType(SHACL.NODESHAPE);
		shape.addType(OWL.CLASS);
		shape.addObject(IM.IS_CONTAINED_IN,IMA);
		shape.setName(shape.getIri().substring(shape.getIri().lastIndexOf("#")+1));

	}

	private void argument(TTEntity shape) throws JsonProcessingException, DataFormatException {
		setLabels(shape);
		shape.setDescription("A named parameter and a value for passing into a function");
		addProperty(shape,SHACL.PARAMETER,SHACL.DATATYPE,XSD.STRING,1,1,"Parameter name for a function or the parameter name for an argument");
		addProperty(shape,"valueData",SHACL.DATATYPE,XSD.STRING,1,1,"Value that is a literal such as a string or number");
		addProperty(shape,"valueVariable",SHACL.DATATYPE,XSD.STRING,1,1,"argument value which is a variable name to be resolved at run time");
		addProperty(shape,"valueFrom",SHACL.DATATYPE,XSD.STRING,1,1,"Passes in the result set from a previous where clause");

		setOrs(shape,List.of("valueData","valueVariable","valueSelect","valueIri"),1,1);
	}

	private void pageInformation(TTEntity shape) throws JsonProcessingException {
		setLabels(shape);
		shape.setDescription("Information about paging if the client wishes to page results, including page number and page size");
		addProperty(shape,"pageNumber",SHACL.DATATYPE,XSD.INTEGER,1,1,"Page number (starting at 1)");
		addProperty(shape,"pageSize",SHACL.DATATYPE,XSD.INTEGER,1,1,"number of entries per page");
	}

	private void setOrs(TTEntity shape,List<String> orProperties,Integer min, Integer max) throws DataFormatException {
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
		if (addAt==null){
			throw new DataFormatException("null position");
		}
		toRemove.sort(Comparator.comparing((TTNode p) -> p.get(SHACL.ORDER).asLiteral().intValue()));
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

	private void setPropertyEntity(TTIriRef path, TTIriRef target,String comment) throws JsonProcessingException {
		if (path.getIri().contains("/im#")) {
			TTEntity pEntity = manager.getEntity(path.getIri());
			String propType;
			if (target.equals(SHACL.NODE))
				propType = "dataModelObjectProperty";
			else
				propType = "dataModelDataProperty";
			if (pEntity == null) {
				pEntity = new TTEntity()
					.setIri(path.getIri())
					.addType(RDF.PROPERTY)
					.setName(localName(path.getIri()))
					.setDescription(comment);
				pEntity.addObject(RDFS.SUBPROPERTYOF, TTIriRef.iri(IM.NAMESPACE + propType));
				manager.getDocument().addEntity(pEntity);
				saveDocument(sourcePath+"\\CoreOntology.json");
			}
			pEntity.setDescription(comment);
		}
	}
	private static String localName(String iri){
		return iri.substring(iri.lastIndexOf("#")+1);
	}
	private void addProperty(TTEntity shape, String path, TTIriRef target,TTIriRef type,
																	Integer min,Integer max,String comment) throws JsonProcessingException {
		addProperty(shape,TTIriRef.iri(IM.NAMESPACE+path),target,type,min,max,comment);

	}

	private void addProperty(TTEntity shape, TTIriRef path, TTIriRef target,TTIriRef type,
													 Integer min,Integer max,String comment) throws JsonProcessingException {
		setPropertyEntity(path,target,comment);
		TTNode property=null;
		if (shape.get(SHACL.PROPERTY)!=null){
			for (TTValue prop: shape.get(SHACL.PROPERTY).getElements()){
				if (prop.asNode().get(SHACL.PATH).asIriRef().equals(path))
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
