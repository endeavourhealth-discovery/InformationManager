USE supplier_maps;

-- ********************* entity *********************

DROP TABLE IF EXISTS emis_maps;
CREATE TABLE emis_maps
(
  read_code        VARCHAR(20) COLLATE utf8_bin NOT NULL,
  read_term        VARCHAR(400),
  snomed_entity_id BIGINT,
  is_emis_code     BOOLEAN,
  code_id          BIGINT,
  parent_code_id   BIGINT
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\SupplierMaps\\emis_maps.csv'
  INTO TABLE emis_maps
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;
