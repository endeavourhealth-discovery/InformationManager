USE im_source;

-- ********************* entity *********************

DROP TABLE IF EXISTS icd10;
CREATE TABLE icd10
(
  code             VARCHAR(10) NOT NULL,
  alt_code         VARCHAR(10) NOT NULL,
  `usage`          VARCHAR(10) NOT NULL,
  usage_uk         TINYINT     NOT NULL,
  description      VARCHAR(200),
  modifier_4       VARCHAR(100),
  modifier_5       VARCHAR(75),
  qualifiers       VARCHAR(400),
  gender_mask      TINYINT,
  min_age          TINYINT,
  max_age          TINYINT,
  tree_description VARCHAR(120),

  PRIMARY KEY icd10_pk (code)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\ICD10_Edition5_20160401\\Content\\ICD10_Edition5_CodesAndTitlesAndMetadata_GB_20160401.txt'
  INTO TABLE icd10
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
  IGNORE 1 LINES
  (code, alt_code, `usage`, usage_uk, description, @modifier_4, @modifier_5, @qualifiers, @gender_mask, @min_age,
   @max_age, @tree_description)
  SET modifier_4 = nullif(@modifier_4, ''),
    modifier_5 = nullif(@modifier_5, ''),
    qualifiers = nullif(@qualifiers, ''),
    gender_mask = nullif(@gender_mask, ''),
    min_age = nullif(@min_age, ''),
    max_age = nullif(@max_age, ''),
    tree_description = nullif(@tree_description, '');
