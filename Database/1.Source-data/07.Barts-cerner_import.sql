USE im_source;

-- ********************* Barts/cerner maps *********************

DROP TABLE IF EXISTS barts_cerner;
CREATE TABLE barts_cerner
(
  count             INT         NOT NULL DEFAULT 0,
  code              VARCHAR(50) NOT NULL,
  term              VARCHAR(150),
  algorithm         VARCHAR(5),
  snomed_expression VARCHAR(50),
  snomed_term       VARCHAR(250),
  author            VARCHAR(50),

  PRIMARY KEY barts_cerner_pk (code)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\Barts\\Barts_Cerner_Procedures_Map.csv'
  INTO TABLE barts_cerner
  FIELDS ENCLOSED BY '"' TERMINATED BY ','
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES
  (count, code, term, algorithm, @snomed_expression, @snomed_term, @author)
  SET snomed_expression = nullif(@snomed_expression, ''),
    snomed_term = nullif(@snomed_term, ''),
    author = nullif(@author, '');


UPDATE barts_cerner
SET snomed_expression = REPLACE(snomed_expression, '|:', ':')
WHERE snomed_expression LIKE '%|:%';

UPDATE barts_cerner
SET snomed_expression = REPLACE(snomed_expression, ':,', ':')
WHERE snomed_expression LIKE '%:,%';

-- Build worktable
DROP TABLE IF EXISTS barts_cerner_snomed_expressions;
CREATE TABLE barts_cerner_snomed_expressions
(
  dbid       INT AUTO_INCREMENT,
  expression VARCHAR(50)  NOT NULL,
  term       VARCHAR(250) NOT NULL,
  PRIMARY KEY barts_cerner_snomed_expressions_pk (dbid),
  UNIQUE INDEX barts_cerner_snomed_expressions_exp (expression)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

INSERT INTO barts_cerner_snomed_expressions (expression, term)
SELECT DISTINCT snomed_expression, snomed_term
FROM barts_cerner
WHERE snomed_expression LIKE '%:%'
   OR snomed_expression LIKE '%+%';


-- ********************* Barts/cerner term maps *********************
DROP TABLE IF EXISTS barts_cerner_term_maps;
CREATE TABLE barts_cerner_term_maps
(
  term   varchar(250) NOT NULL,
  target VARCHAR(150) NOT NULL
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

INSERT INTO barts_cerner_term_maps
  (target, term)
VALUES ('BC_ddOYGVC0cM33G', 'SARS-CoV-2 RNA NOT detected'),
       ('BC_ddOYGVC0cM33GsbmH', 'SARS-CoV-2 RNA NOT detected'),
       ('BC_Fr1gvtFQJAIPq', 'SARS-CoV-2 RNA DETECTED'),
       ('BC_Fr1gvtFQJAIPqpoHK', 'SARS-CoV-2 RNA DETECTED');
