-- Create MODEL
INSERT INTO model (iri, version)
VALUES ('InformationModel/dm/EmisLocal', '1.0.0');

SET @model = LAST_INSERT_ID();


-- Code scheme
INSERT INTO concept (model, data)
VALUES (@model, JSON_OBJECT(
        'status', 'CoreActive',
        'id', 'EMIS_LOCAL',
        'name', 'EMIS Local',
        'description', 'EMIS local codes scheme'));

SET @scheme := LAST_INSERT_ID();

INSERT INTO concept_definition (concept, data)
VALUES (@scheme, JSON_OBJECT(
        'status', 'CoreActive',
        'subtypeOf', JSON_ARRAY(
                JSON_OBJECT('concept', 'CodeScheme')
            )
    ));

-- Concepts
INSERT INTO concept (model, data)
SELECT @model, JSON_OBJECT(
        'status', 'CoreActive',
        'id', CONCAT('EMLOC_', local_code),
        'name', local_term,
        'codeScheme', 'EMIS_LOCAL',
        'code', local_code)
FROM emis_local_codes
GROUP BY local_code;

INSERT INTO concept_definition (concept, data)
SELECT c.dbid, JSON_OBJECT(
        'status', 'CoreActive',
        'subtypeOf', JSON_ARRAY(
                JSON_OBJECT('concept', 'CodeableConcept')
            )
    )
FROM emis_local_codes e
JOIN concept c ON c.id = CONCAT('EMLOC_', local_code)
GROUP BY local_code;

INSERT INTO concept_synonym (dbid, synonym)
SELECT c.dbid, e.local_term
FROM emis_local_codes e
JOIN concept c ON c.id = CONCAT('EMLOC_', local_code) AND c.name <> e.local_term;