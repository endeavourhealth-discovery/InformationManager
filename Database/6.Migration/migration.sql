SELECT *
FROM im_live.entity
WHERE dbid IN (712259, 1302012, 1319954);
SELECT COUNT(*)
FROM im_live.entity
WHERE iri IS NULL
  AND scheme NOT IN (712259, 1302012, 1319954);
SELECT COUNT(*)
FROM im_live.entity
WHERE iri IS NOT NULL
  AND scheme NOT IN (712259, 1302012, 1319954);
SELECT *
FROM im_live.entity
WHERE iri IS NULL
  AND scheme NOT IN (712259, 1302012, 1319954);

SELECT @scheme := dbid
FROM im_live.entity
WHERE id = 'SNOMED';
UPDATE im_live.entity
SET iri = CONCAT('sn:', code)
WHERE scheme = @scheme;

SELECT @scheme := dbid
FROM im_live.entity
WHERE id = 'EMIS_LOCAL';
UPDATE im_live.entity
SET iri = CONCAT('emis:', code)
WHERE scheme = @scheme;

SELECT @scheme := dbid
FROM im_live.entity
WHERE id = 'TPP_LOCAL';
UPDATE im_live.entity
SET iri = CONCAT('tpp:', code)
WHERE scheme = @scheme;

SELECT @scheme := dbid
FROM im_live.entity
WHERE id = 'VISION_LOCAL';
UPDATE im_live.entity
SET iri = CONCAT('vis:', code)
WHERE scheme = @scheme;

SELECT @scheme := dbid
FROM im_live.entity
WHERE id = 'READ2';
UPDATE im_live.entity
SET iri = CONCAT('r2:', code)
WHERE scheme = @scheme;

SELECT @scheme := dbid
FROM im_live.entity
WHERE id = 'CTV3';
UPDATE im_live.entity
SET iri = CONCAT('ctv3:', code)
WHERE scheme = @scheme;

SELECT @scheme := dbid
FROM im_live.entity
WHERE id = 'ICD10';
UPDATE im_live.entity
SET iri = CONCAT('icd10:', code)
WHERE scheme = @scheme;

SELECT @scheme := dbid
FROM im_live.entity
WHERE id = 'OPCS4';
UPDATE im_live.entity
SET iri = CONCAT('opcs4:', code)
WHERE scheme = @scheme;

SELECT @scheme := dbid
FROM im_live.entity
WHERE id = 'BartsCerner';
UPDATE im_live.entity
SET iri = CONCAT('bc:', code)
WHERE scheme = @scheme;

SELECT @scheme := dbid
FROM im_live.entity
WHERE id = 'LE_TYPE';
UPDATE im_live.entity
SET iri = CONCAT(':LE_', code)
WHERE scheme = @scheme;

-- FHIR Valuesets
INSERT IGNORE INTO module (iri)
VALUES ('http://www.EndeavourHealth.org/InformationModel/Module/Legacy/FHIR');
SELECT @fhirmodule := dbid
FROM module
WHERE iri = 'http://www.EndeavourHealth.org/InformationModel/Module/Legacy/FHIR';

INSERT IGNORE INTO namespace (prefix, iri)
VALUES ('fhirvs:', 'http://hl7.org/fhir/ValueSet');
SELECT @fhirvs := dbid
FROM namespace
WHERE prefix = 'fhirvs:';

INSERT IGNORE INTO entity (namespace, module, iri, name, type)
VALUES (@fhirvs, @fhirmodule, 'fhirvs:administrative-gender', 'FHIR Administrative Gender value set', 0);

SELECT @dbid := dbid
FROM entity
WHERE iri = 'fhirvs:administrative-gender';


SELECT *
FROM entity
WHERE dbid = 1526923;

SELECT c.id, c.name, s.name as scheme, c.code, c.use_count
FROM entity c
       LEFT JOIN entity s ON s.dbid = c.scheme
WHERE c.id LIKE 'FHIR_%'
  AND c.code IS NULL;
SELECT *
FROM entity
WHERE id LIKE 'DMD_%'
  AND use_count > 0;
