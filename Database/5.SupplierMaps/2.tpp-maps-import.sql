USE supplier_maps;

-- ********************* entity *********************

DROP TABLE IF EXISTS tpp_maps;
CREATE TABLE tpp_maps
(
  ctv3_code        VARCHAR(20) COLLATE utf8_bin NOT NULL,
  ctv3_term        VARCHAR(400),
  snomed_entity_id BIGINT,
  is_tpp_code      BOOLEAN
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\SupplierMaps\\tpp_maps.csv'
  INTO TABLE tpp_maps
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;
