USE im_source;

-- ********************* entityS *********************

DROP TABLE IF EXISTS tpp_local_codes;
CREATE TABLE tpp_local_codes
(
  rowIdentifier           INT,
  IDOrganisationVisibleTo VARCHAR(10),
  ctv3Code                VARCHAR(6)   NOT NULL COLLATE utf8_bin,
  ctv3Text                VARCHAR(400) NOT NULL,

  PRIMARY KEY tpp_local_codes_pk (ctv3Code)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\TPP\\SRCtv3.csv'
  INTO TABLE tpp_local_codes
  FIELDS ENCLOSED BY '"' TERMINATED BY ','
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;

-- ********************* MAPS *********************
DROP TABLE IF EXISTS tpp_snomed_map;
CREATE TABLE tpp_snomed_map
(
  ctv3Code   VARCHAR(6) NOT NULL COLLATE utf8_bin,
  term       VARCHAR(256),
  snomedCode BIGINT     NOT NULL,

  PRIMARY KEY tpp_snomed_map (ctv3Code)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\TPP\\tpp_to_snomed_maps.csv'
  INTO TABLE tpp_snomed_map
  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES;
