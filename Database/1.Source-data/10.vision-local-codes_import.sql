USE im_source;

-- ********************* entityS *********************
DROP TABLE IF EXISTS vision_local_codes;
CREATE TABLE vision_local_codes
(
  readCode   VARCHAR(50) COLLATE utf8_bin,
  readTerm   VARCHAR(300) NOT NULL,
  snomedCode BIGINT
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
