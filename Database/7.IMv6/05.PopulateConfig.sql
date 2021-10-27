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
    "label": "SummaryInferredDivider",
    "order": 200,
    "predicate": "None"
  },
  {
    "size": "100%",
    "type": "TextDefinition",
    "label": "Inferred",
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
    "label": "InferredStatedDivider",
    "order": 300,
    "predicate": "None"
  },
  {
    "size": "100%",
    "type": "TextDefinition",
    "label": "Axioms",
    "order": 301,
    "predicate": "axioms"
  },
  {
    "size": "100%",
    "type": "SectionDivider",
    "label": "StatedTermsDivider",
    "order": 400,
    "predicate": "None"
  },
  {
    "size": "100%",
    "type": "TermsTable",
    "label": "Terms",
    "order": 402,
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
    "http://endhealth.info/im#isA",
    "http://endhealth.info/im#roleGroup",
    "http://endhealth.info/im#isContainedIn"
]');

DELETE FROM config WHERE name = 'axiomPredicates';
INSERT INTO config (name, config) VALUES ('axiomPredicates', '[
  "http://www.w3.org/2000/01/rdf-schema#subPropertyOf",
  "http://www.w3.org/2000/01/rdf-schema#subClassOf",
  "http://www.w3.org/2002/07/owl#equivalentClass"
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
    "http://endhealth.info/im#isA": "Is a",
    "http://endhealth.info/im#roleGroup": "Where",
    "http://www.w3.org/2002/07/owl#equivalentClass": "Is equivalent to",
    "http://www.w3.org/2002/07/owl#intersectionOf": "Combination of",
    "http://www.w3.org/2002/07/owl#someValuesFrom": "With a value",
    "http://www.w3.org/2002/07/owl#onProperty": "On property"
  }');
