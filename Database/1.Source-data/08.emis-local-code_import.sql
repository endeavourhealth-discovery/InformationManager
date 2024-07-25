USE im_source;

-- ********************* entityS *********************

DROP TABLE IF EXISTS emis_codes;
CREATE TABLE emis_codes
(
  read_code        VARCHAR(50)  NOT NULL COLLATE utf8_bin,
  read_term        VARCHAR(300) NOT NULL,
  snomed_entity_id BIGINT,
  is_emis_code     BOOLEAN,
  code_id          BIGINT,
  parent_code_id   BIGINT,
  INDEX emis_codes_idx (read_code)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\code_export_emis.txt'
  INTO TABLE emis_codes
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES
  (read_code, read_term, snomed_entity_id, is_emis_code, code_id, parent_code_id);
