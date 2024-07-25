USE im_source;

-- ********************* MAPS *********************
DROP TABLE IF EXISTS icd10_opcs4_maps;
CREATE TABLE icd10_opcs4_maps
(
  id                    VARCHAR(40) COMMENT '',
  effectiveTime         VARCHAR(8) COMMENT '',
  active                BOOLEAN     NOT NULL DEFAULT 1 COMMENT '',
  moduleId              BIGINT COMMENT '',
  refsetId              BIGINT      NOT NULL COMMENT '',
  referencedComponentId BIGINT      NOT NULL COMMENT '',
  mapGroup              INT COMMENT '',
  mapPriority           INT COMMENT '',
  mapRule               VARCHAR(250) COMMENT '',
  mapAdvice             VARCHAR(500) COMMENT '',
  mapTarget             VARCHAR(10) NOT NULL COMMENT '',
  correlationId         INT COMMENT '',
  mapCategoryId         INT COMMENT ''
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\SNOMED\\SnomedCT_UKClinicalRF2_PRODUCTION_20200401T000001Z\\Snapshot\\Refset\\Map\\der2_iisssciRefset_ExtendedMapSnapshot_GB1000000_20200401.txt'
  INTO TABLE icd10_opcs4_maps
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;

ALTER TABLE icd10_opcs4_maps
  ADD INDEX icd10_opcs4_maps_idx (refsetId, mapTarget, active);
