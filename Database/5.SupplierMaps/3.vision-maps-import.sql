USE supplier_maps;

-- ********************* entity *********************

DROP TABLE IF EXISTS vision_maps;
CREATE TABLE vision_maps
(
  read_code        VARCHAR(20) COLLATE utf8_bin NOT NULL,
  read_term        VARCHAR(400),
  snomed_entity_id BIGINT,
  is_vision_code   BOOLEAN
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\SupplierMaps\\vision_maps.csv'
  INTO TABLE vision_maps
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES;
