USE im_source;

-- ********************* entityS *********************

DROP TABLE IF EXISTS encounter_types;

CREATE TABLE encounter_types
(
  count      INT,
  legacy_iri VARCHAR(140) COLLATE utf8_bin NOT NULL,
  code       VARCHAR(40),
  term       VARCHAR(255),
  core_iri   VARCHAR(140) COLLATE utf8_bin NOT NULL,
  core_term  VARCHAR(255),

  PRIMARY KEY encounter_types_pk (legacy_iri)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\Core\\IMMapEncounters.txt'
  INTO TABLE encounter_types
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;

-- ********************* RELATIONSHIPS *********************

DROP TABLE IF EXISTS encounter_cpo;

CREATE TABLE encounter_cpo
(
  entity   VARCHAR(140) COLLATE utf8_bin NOT NULL,
  `group`  INT                           NOT NULL DEFAULT 0,
  property VARCHAR(140) COLLATE utf8_bin NOT NULL,
  object   VARCHAR(140) COLLATE utf8_bin NOT NULL,
  min      INT,
  max      INT,
  operator VARCHAR(50)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\Core\\IMCore_EntityPropertyObject.txt'
  INTO TABLE encounter_cpo
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\Core\\IMENC_EntityPropertyObject.txt'
  INTO TABLE encounter_cpo
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;
