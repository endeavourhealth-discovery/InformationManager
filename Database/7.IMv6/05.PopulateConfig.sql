USE im6;
DELETE FROM config WHERE name = 'definition';
INSERT INTO config (name, config) VALUES ('definition', '[
  {
    "size": "100%",
    "type": "TextSectionHeader",
    "label": "Summary",
    "order": 100,
    "predicate": "None"
  },
  {
    "size": "50%",
    "type": "TextWithLabel",
    "label": "Name",
    "order": 101,
    "predicate": "http://www.w3.org/2000/01/rdf-schema#label"
  },
  {
    "size": "50%",
    "type": "TextWithLabel",
    "label": "Iri",
    "order": 102,
    "predicate": "@id"
  },
  {
    "size": "50%",
    "type": "TextWithLabel",
    "label": "Code",
    "order": 103,
    "predicate": "http://endhealth.info/im#code"
  },
  {
    "size": "50%",
    "type": "ObjectNameTagWithLabel",
    "label": "Status",
    "order": 103,
    "predicate": "http://endhealth.info/im#status"
  },
  {
    "size": "50%",
    "type": "ArrayObjectNamesToStringWithLabel",
    "label": "Types",
    "order": 104,
    "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
  },
  {
    "size": "100%",
    "type": "TextHTMLWithLabel",
    "label": "Description",
    "order": 105,
    "predicate": "http://www.w3.org/2000/01/rdf-schema#comment"
  },
  {
    "size": "100%",
    "type": "SectionDivider",
    "label": "SummaryDefinitionDivider",
    "order": 200,
    "predicate": "None"
  },
  {
    "size": "100%",
    "type": "TextDefinition",
    "label": "Definition",
    "order": 201,
    "predicate": "inferred"
  },
  {
    "size": "100%",
    "type": "ArrayObjectNameListboxWithLabel",
    "label": "Has sub types",
    "order": 202,
    "predicate": "subtypes"
  },
  {
    "size": "100%",
    "type": "ArrayObjectNameListboxWithLabel",
    "label": "Is child of",
    "order": 203,
    "predicate": "http://endhealth.info/im#isChildOf"
  },
  {
    "size": "100%",
    "type": "ArrayObjectNameListboxWithLabel",
    "label": "Has children",
    "order": 204,
    "predicate": "http://endhealth.info/im#hasChildren"
  },
  {
    "size": "100%",
    "type": "SectionDivider",
    "label": "DefinitionTermsDivider",
    "order": 300,
    "predicate": "None"
  },
  {
    "size": "100%",
    "type": "TermsTable",
    "label": "Terms",
    "order": 301,
    "predicate": "termCodes"
  }
]');
DELETE FROM config WHERE name = 'filterDefaults';
INSERT INTO config (name, config) VALUES ('filterDefaults', '{
	"schemeOptions": [
		"Discovery namespace",
		"Snomed-CT namespace"
	],
    "statusOptions": [
		"Active",
		"Draft"
	],
    "typeOptions": [
        "Concept",
        "Concept Set",
        "Folder",
        "Node shape",
        "ObjectProperty",
        "Property",
        "Query template",
        "Record type",
        "Value set"
    ]
}');

DELETE FROM config WHERE name = 'inferredPredicates';
INSERT INTO config (name, config) VALUES ('inferredPredicates', '[
    "http://www.w3.org/2000/01/rdf-schema#subClassOf",
    "http://endhealth.info/im#roleGroup",
    "http://endhealth.info/im#isContainedIn"
]');

DELETE FROM config WHERE name = 'axiomPredicates';

DELETE FROM config WHERE name = 'inferredExcludePredicates';
INSERT INTO config (name, config) VALUES ('inferredExcludePredicates', '[
    "http://www.w3.org/2000/01/rdf-schema#label",
    "http://endhealth.info/im#status",
    "http://endhealth.info/im#Status",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
    "http://www.w3.org/2000/01/rdf-schema#comment",
    "http://endhealth.info/im#isChildOf",
    "http://endhealth.info/im#hasChildren",
    "http://endhealth.info/im#isContainedIn"
  ]');

DELETE FROM config WHERE name = 'conceptDashboard';
INSERT INTO config (name, config) VALUES ('conceptDashboard', '[
  {
    "type": "ReportTable",
    "order": 100,
    "iri": "http://endhealth.info/im#ontologyOverview"
  },
  {
    "type": "PieChartDashCard",
    "order": 200,
    "iri": "http://endhealth.info/im#ontologyConceptTypes"
  },
  {
    "type": "PieChartDashCard",
    "order": 300,
    "iri": "http://endhealth.info/im#ontologyConceptSchemes"
  },
  {
    "type": "PieChartDashCard",
    "order": 400,
    "iri": "http://endhealth.info/im#ontologyConceptStatus"
  }
]');

DELETE FROM config WHERE name = 'defaultPredicateNames';
INSERT INTO config (name, config) VALUES ('defaultPredicateNames', '{
    "http://www.w3.org/2000/01/rdf-schema#subClassOf": "Is subclass of",
    "http://endhealth.info/im#roleGroup": "Where",
    "http://www.w3.org/2002/07/owl#equivalentClass": "Is equivalent to",
    "http://www.w3.org/2002/07/owl#intersectionOf": "Combination of",
    "http://www.w3.org/2002/07/owl#someValuesFrom": "With a value",
    "http://www.w3.org/2002/07/owl#onProperty": "On property"
  }');

DELETE FROM config WHERE name = 'xlmSchemaDataTypes';
INSERT INTO config (name, config) VALUES ('xlmSchemaDataTypes', '[
    "http://www.w3.org/2001/XMLSchema#string",
    "http://www.w3.org/2001/XMLSchema#boolean",
    "http://www.w3.org/2001/XMLSchema#float",
    "http://www.w3.org/2001/XMLSchema#double",
    "http://www.w3.org/2001/XMLSchema#decimal",
    "http://www.w3.org/2001/XMLSchema#dateTime",
    "http://www.w3.org/2001/XMLSchema#duration",
    "http://www.w3.org/2001/XMLSchema#hexBinary",
    "http://www.w3.org/2001/XMLSchema#base64Binary",
    "http://www.w3.org/2001/XMLSchema#anyURI",
    "http://www.w3.org/2001/XMLSchema#ID",
    "http://www.w3.org/2001/XMLSchema#IDREF",
    "http://www.w3.org/2001/XMLSchema#ENTITY",
    "http://www.w3.org/2001/XMLSchema#NOTATION",
    "http://www.w3.org/2001/XMLSchema#normalizedString",
    "http://www.w3.org/2001/XMLSchema#token",
    "http://www.w3.org/2001/XMLSchema#language",
    "http://www.w3.org/2001/XMLSchema#IDREFS",
    "http://www.w3.org/2001/XMLSchema#ENTITIES",
    "http://www.w3.org/2001/XMLSchema#NMTOKEN",
    "http://www.w3.org/2001/XMLSchema#NMTOKENS",
    "http://www.w3.org/2001/XMLSchema#Name",
    "http://www.w3.org/2001/XMLSchema#QName",
    "http://www.w3.org/2001/XMLSchema#NCName",
    "http://www.w3.org/2001/XMLSchema#integer",
    "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
    "http://www.w3.org/2001/XMLSchema#positiveInteger",
    "http://www.w3.org/2001/XMLSchema#nonPositiveInteger",
    "http://www.w3.org/2001/XMLSchema#negativeInteger",
    "http://www.w3.org/2001/XMLSchema#byte",
    "http://www.w3.org/2001/XMLSchema#int",
    "http://www.w3.org/2001/XMLSchema#long",
    "http://www.w3.org/2001/XMLSchema#short",
    "http://www.w3.org/2001/XMLSchema#unsignedByte",
    "http://www.w3.org/2001/XMLSchema#unsignedInt",
    "http://www.w3.org/2001/XMLSchema#unsignedLong",
    "http://www.w3.org/2001/XMLSchema#unsignedShort",
    "http://www.w3.org/2001/XMLSchema#date",
    "http://www.w3.org/2001/XMLSchema#time",
    "http://www.w3.org/2001/XMLSchema#gYearMonth",
    "http://www.w3.org/2001/XMLSchema#gYear",
    "http://www.w3.org/2001/XMLSchema#gMonthDay",
    "http://www.w3.org/2001/XMLSchema#gDay",
    "http://www.w3.org/2001/XMLSchema#gMonth"
]');