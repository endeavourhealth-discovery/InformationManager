USE im_source;

-- ********************* READ V2 *********************

DROP TABLE IF EXISTS read_v2;
CREATE TABLE read_v2
(
  code VARCHAR(6) COLLATE utf8_bin NOT NULL COMMENT 'The READ code id',
  term VARCHAR(250)                NOT NULL COMMENT 'The term',

  PRIMARY KEY read_v2_code_pk (code)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\READ2\\Unified\\Corev2.all'
  INTO TABLE read_v2
  FIELDS ENCLOSED BY '"' TERMINATED BY ','
  LINES TERMINATED BY '\r\n'
  (code, term);

-- !!!!!! ADD IN ROOT TERM - MISSING FROM FILE BUT REFERENCED BY HIERARCHY! !!!!!!
INSERT INTO read_v2 (code, term)
VALUES ('.....', 'READ V2 Hierarchy');

DROP TABLE IF EXISTS read_v2_key;
CREATE TABLE read_v2_key
(
  readKey    VARCHAR(40),
  uniquifier CHAR(2)                  NOT NULL,
  term30     VARCHAR(30)              NOT NULL,
  term60     VARCHAR(60),
  term198    VARCHAR(198),
  termCode   CHAR(2)                  NOT NULL,
  langCode   CHAR(2)                  NOT NULL,
  readCode   CHAR(6) COLLATE utf8_bin NOT NULL,
  discFlag   CHAR(1)                  NOT NULL
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\READ2\\Unified\\Keyv2.all'
  INTO TABLE read_v2_key
  FIELDS ENCLOSED BY '"' TERMINATED BY ','
  LINES TERMINATED BY '\r\n'
  (readKey, uniquifier, term30, @term60, @term198, termCode, langCode, readCode, discFlag)
  SET term60 = nullif(@term60, ''),
    term198 = nullif(@term198, '');

-- ********************* READ V2 -> SNOMED MAP *********************

DROP TABLE IF EXISTS read_v2_map;
CREATE TABLE read_v2_map
(
  id            VARCHAR(40)                 NOT NULL,
  readCode      VARCHAR(6) COLLATE utf8_bin NOT NULL,
  termCode      VARCHAR(2)                  NOT NULL,
  entityId      BIGINT                      NOT NULL,
  descriptionId BIGINT,
  assured       BOOLEAN                     NOT NULL,
  effectiveDate VARCHAR(10)                 NOT NULL,
  status        BOOLEAN                     NOT NULL
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\nhs_datamigration_29.0.0_20200401000001\\Mapping Tables\\Updated\\Clinically Assured\\rcsctmap2_uk_20200401000001.txt'
  INTO TABLE read_v2_map
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES
  (id, readCode, termCode, entityId, @descriptionId, assured, effectiveDate, status)
  SET descriptionId = nullif(@descriptionId, '');

ALTER TABLE read_v2_map
  ADD INDEX read_v2_map_assured_idx (assured, status, termCode);

DROP TABLE IF EXISTS read_v2_alt_map;
CREATE TABLE read_v2_alt_map
(
  readCode      VARCHAR(6) COLLATE utf8_bin NOT NULL,
  termCode      VARCHAR(2)                  NOT NULL,
  entityId      BIGINT,
  descriptionId BIGINT,
  useAlt        VARCHAR(1),
  PRIMARY KEY read_v2_alt_map_pk (readCode, termCode)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\nhs_datamigration_29.0.0_20200401000001\\Mapping Tables\\Updated\\Clinically Assured\\codesWithValues_AlternateMaps_READ2_20200401000001.txt'
  INTO TABLE read_v2_alt_map
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES
  (readCode, termCode, @entityId, @descriptionId, @use)
  SET entityId = nullif(@entityId, ''),
    descriptionId = nullif(@descriptionId, ''),
    useAlt = nullif(@use, '');

DROP TABLE IF EXISTS read_v2_snomed_map;
CREATE TABLE read_v2_snomed_map
(
  readCode VARCHAR(6) COLLATE utf8_bin NOT NULL,
  termCode VARCHAR(2)                  NOT NULL,
  entityId BIGINT                      NOT NULL,
  PRIMARY KEY read_v2_snomed_map_pk (readCode, termCode)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

REPLACE INTO read_v2_snomed_map
  (readCode, termCode, entityId)
SELECT m.readCode, m.termCode, IFNULL(a.entityId, m.entityId)
FROM read_v2_map m
       LEFT JOIN read_v2_alt_map a ON a.readCode = m.readCode AND a.termCode = m.termCode AND a.useAlt = 'Y'
WHERE m.status = 1
ORDER BY m.readCode, m.termCode, m.assured DESC, m.effectiveDate -- Ensure latest for each row
