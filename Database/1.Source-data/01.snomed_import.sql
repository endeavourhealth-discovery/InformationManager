USE im_source;

-- ********************* entity *********************

DROP TABLE IF EXISTS snomed_entity;
CREATE TABLE snomed_entity
(
  id                 BIGINT     NOT NULL COMMENT '',
  effectiveTime      VARCHAR(8) NOT NULL COMMENT '',
  active             BOOLEAN    NOT NULL COMMENT '',
  moduleId           BIGINT     NOT NULL COMMENT '',
  definitionStatusId BIGINT     NOT NULL COMMENT '',

  PRIMARY KEY snomed_entity_pk (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\SNOMED\\SnomedCT_InternationalRF2_PRODUCTION_20180731T120000Z\\Snapshot\\Terminology\\sct2_Concept_Snapshot_INT_20180731.txt'
  INTO TABLE snomed_entity
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\SNOMED\\SnomedCT_UKClinicalRF2_PRODUCTION_20200401T000001Z\\Snapshot\\Terminology\\sct2_Concept_Snapshot_GB1000000_20200401.txt'
  INTO TABLE snomed_entity
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\SNOMED\\SnomedCT_UKDrugRF2_PRODUCTION_20200415T000001Z\\Snapshot\\Terminology\\sct2_Concept_Snapshot_GB1000001_20200415.txt'
  INTO TABLE snomed_entity
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;
-- ********************* DESCRIPTION *********************

DROP TABLE IF EXISTS snomed_description;
CREATE TABLE snomed_description
(
  id                 BIGINT       NOT NULL COMMENT '',
  effectiveTime      VARCHAR(8)   NOT NULL COMMENT '',
  active             BOOLEAN      NOT NULL COMMENT '',
  moduleId           BIGINT       NOT NULL COMMENT '',
  entityId           BIGINT       NOT NULL COMMENT '',
  languageCode       VARCHAR(2) COMMENT '',
  typeId             BIGINT       NOT NULL COMMENT '',
  term               VARCHAR(400) NOT NULL COMMENT '',
  caseSignificanceId BIGINT       NOT NULL COMMENT '',

  PRIMARY KEY snomed_description_pk (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\SNOMED\\SnomedCT_InternationalRF2_PRODUCTION_20180731T120000Z\\Snapshot\\Terminology\\sct2_Description_Snapshot-en_INT_20180731.txt'
  INTO TABLE snomed_description
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\SNOMED\\SnomedCT_UKClinicalRF2_PRODUCTION_20200401T000001Z\\Snapshot\\Terminology\\sct2_Description_Snapshot-en_GB1000000_20200401.txt'
  INTO TABLE snomed_description
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\SNOMED\\SnomedCT_UKDrugRF2_PRODUCTION_20200415T000001Z\\Snapshot\\Terminology\\sct2_Description_Snapshot-en_GB1000001_20200415.txt'
  INTO TABLE snomed_description
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;
ALTER TABLE snomed_description
  ADD INDEX snomed_description_active_typeId_idx (active, typeId);

-- ********************* RELATIONSHIP *********************

DROP TABLE IF EXISTS snomed_relationship;
CREATE TABLE snomed_relationship
(
  id                   BIGINT      NOT NULL COMMENT '',
  effectiveTime        VARCHAR(8)  NOT NULL COMMENT '',
  active               BOOLEAN     NOT NULL COMMENT '',
  moduleId             BIGINT      NOT NULL COMMENT '',
  sourceId             VARCHAR(20) NOT NULL COMMENT '',
  destinationId        VARCHAR(20) NOT NULL COMMENT '',
  relationshipGroup    BIGINT      NOT NULL COMMENT '',
  typeId               BIGINT      NOT NULL COMMENT '',
  characteristicTypeId BIGINT      NOT NULL COMMENT '',
  modifierId           BIGINT      NOT NULL COMMENT '',

  PRIMARY KEY snomed_relationship_pk (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\SNOMED\\SnomedCT_InternationalRF2_PRODUCTION_20180731T120000Z\\Snapshot\\Terminology\\sct2_Relationship_Snapshot_INT_20180731.txt'
  INTO TABLE snomed_relationship
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\SNOMED\\SnomedCT_UKClinicalRF2_PRODUCTION_20200401T000001Z\\Snapshot\\Terminology\\sct2_Relationship_Snapshot_GB1000000_20200401.txt'
  INTO TABLE snomed_relationship
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\SNOMED\\SnomedCT_UKDrugRF2_PRODUCTION_20200415T000001Z\\Snapshot\\Terminology\\sct2_Relationship_Snapshot_GB1000001_20200415.txt'
  INTO TABLE snomed_relationship
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;
ALTER TABLE snomed_relationship
  ADD INDEX snomed_relationship_active_group_idx (active, relationshipGroup);

-- ********************* LANGUAGE (CONTENT) REFSETS *********************

DROP TABLE IF EXISTS snomed_refset;
CREATE TABLE snomed_refset
(
  id                    VARCHAR(40) NOT NULL COMMENT '',
  effectiveTime         VARCHAR(8)  NOT NULL COMMENT '',
  active                BOOLEAN     NOT NULL COMMENT '',
  moduleId              BIGINT      NOT NULL COMMENT '',
  refsetId              BIGINT      NOT NULL COMMENT '',
  referencedComponentId BIGINT      NOT NULL COMMENT '',
  acceptabilityId       BIGINT      NOT NULL COMMENT '',
  PRIMARY KEY snomed_refset_pk (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\SNOMED\\SnomedCT_InternationalRF2_PRODUCTION_20180731T120000Z\\Snapshot\\Refset\\Language\\der2_cRefset_LanguageSnapshot-en_INT_20180731.txt'
  INTO TABLE snomed_refset
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\SNOMED\\SnomedCT_UKClinicalRF2_PRODUCTION_20200401T000001Z\\Snapshot\\Refset\\Language\\der2_cRefset_LanguageSnapshot-en_GB1000000_20200401.txt'
  INTO TABLE snomed_refset
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\SNOMED\\SnomedCT_UKDrugRF2_PRODUCTION_20200415T000001Z\\Snapshot\\Refset\\Language\\der2_cRefset_LanguageSnapshot-en_GB1000001_20200415.txt'
  INTO TABLE snomed_refset
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;


-- ********************* DRUG REFSETS *********************
LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\SNOMED\\SnomedCT_UKDrugRF2_PRODUCTION_20200415T000001Z\\Snapshot\\Refset\\Content\\der2_Refset_SimpleSnapshot_GB1000001_20200415.txt'
  INTO TABLE snomed_refset
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;

ALTER TABLE snomed_refset
  ADD INDEX snomed_refset_acceptabilityId_refsetId_active_idx (refsetId, active, acceptabilityId);

-- ********************* HISTORY SUBSTITUTION *********************

DROP TABLE IF EXISTS snomed_history;
CREATE TABLE snomed_history
(
  oldEntityId             BIGINT NOT NULL,
  oldEntityStatus         BIGINT NOT NULL,
  newEntityId             BIGINT NOT NULL,
  newEntityStatus         BIGINT NOT NULL,
  path                    VARCHAR(255),
  isAmbiguous             TINYINT,
  iterations              TINYINT,
  oldEntityFsn            VARCHAR(255),
  oldConcpetFsnTagCount   TINYINT,
  newEntityFsn            VARCHAR(255),
  newEntityFsnStatus      BIGINT,
  tlhIdenticalFlag        BOOL,
  fsnTaglessIdenticalFlag BOOL,
  fsnTagIdenticalFlag     BOOL,
  INDEX snomed_history_idx (oldEntityId)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\SNOMED\\SnomedCT_UKClinicalRF2_PRODUCTION_20200401T000001Z\\Resources\\HistorySubstitutionTable\\xres2_HistorySubstitutionTable_Entities_GB1000000_20200401.txt'
  INTO TABLE snomed_history
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;


-- ********************* SNOMED RANGE *********************
DROP TABLE IF EXISTS snomed_range;
CREATE TABLE snomed_range
(
  id                    VARCHAR(40) NOT NULL COMMENT '',
  effectiveTime         VARCHAR(8)  NOT NULL COMMENT '',
  active                BOOLEAN     NOT NULL COMMENT '',
  moduleId              BIGINT      NOT NULL COMMENT '',
  refsetId              BIGINT      NOT NULL COMMENT '',
  referencedComponentId BIGINT      NOT NULL COMMENT '',
  rangeConstraint       VARCHAR(1024) COMMENT '',
  attributeRule         VARCHAR(1024) COMMENT '',
  ruleStrengthId        BIGINT      NOT NULL COMMENT '',
  contentTypeId         BIGINT      NOT NULL COMMENT '',
  PRIMARY KEY snomed_range_pk (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\SNOMED\\SnomedCT_InternationalRF2_PRODUCTION_20180731T120000Z\\Snapshot\\Refset\\Metadata\\der2_ssccRefset_MRCMAttributeRangeSnapshot_INT_20180731.txt'
  INTO TABLE snomed_range
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;


-- ********************* OPTIMISED ACTIVE PREFERRED/SPECIFIED TABLES *********************
DROP TABLE IF EXISTS snomed_description_filtered;
CREATE TABLE snomed_description_filtered
SELECT DISTINCT c.id, d.term, c.active
FROM snomed_entity c
       JOIN snomed_description d
            ON d.entityId = c.id
              AND d.active = 1
              AND d.typeId = 900000000000003001 -- Fully specified name
              AND d.moduleId = c.moduleId
       JOIN snomed_refset r
            ON r.referencedComponentId = d.id
              AND r.active = 1
              AND r.refsetId IN (999001261000000100, 999000691000001104)
-- Clinical part & pharmacy part
-- WHERE c.active = 1               -- ********** NOTE : NOW ACTIVE AND INACTIVE entityS TO BE IMPORTED
;
ALTER TABLE snomed_description_filtered
  ADD PRIMARY KEY snomed_description_filtered_pk (id);
