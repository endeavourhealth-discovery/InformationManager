DELETE FROM config WHERE name = 'definition';
INSERT INTO config (name, config) VALUES ('definition', '[
          {
            "label": "Name",
            "predicate": "http://www.w3.org/2000/01/rdf-schema#label",
            "type": "TextWithLabel",
            "size": "50%",
            "order": 0
          },
          {
            "label": "Iri",
            "predicate": "@id",
            "type": "TextWithLabel",
            "size": "50%",
            "order": 1
          },
          {
            "label": "Status",
            "predicate": "http://endhealth.info/im#status",
            "type": "ObjectNameWithLabel",
            "size": "50%",
            "order": 2
          },
          {
            "label": "Types",
            "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
            "type": "ArrayObjectNamesToStringWithLabel",
            "size": "50%",
            "order": 3
          },
          {
            "label": "Description",
            "predicate": "http://www.w3.org/2000/01/rdf-schema#comment",
            "type": "TextHTMLWithLabel",
            "size": "100%",
            "order": 4
          },
          {
            "label": "Divider",
            "predicate": "None",
            "type": "Divider",
            "size": "100%",
            "order": 5
          },
          {
            "label": "Is a",
            "predicate": "http://endhealth.info/im#isA",
            "type": "ListboxWithLabel",
            "size": "50%",
            "order": 6
          },
          {
            "label": "Has sub types",
            "predicate": "subtypes",
            "type": "ListboxWithLabel",
            "size": "50%",
            "order": 7
          },
          {
            "label": "Divider",
            "predicate": "None",
            "type": "Divider",
            "size": "100%",
            "order": 8
          },
          {
            "label": "Semantic properties",
            "predicate": "semanticProperties",
            "type": "SemanticProperties",
            "size": "100%",
            "order": 9
          },
          {
            "label": "Divider",
            "predicate": "None",
            "type": "Divider",
            "size": "100%",
            "order": 10
          },
          {
            "label": "Data model properties",
            "predicate": "dataModelProperties",
            "type": "DataModelProperties",
            "size": "100%",
            "order": 11
          }
        ]');
