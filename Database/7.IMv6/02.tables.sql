
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

USE im6 ;

drop table if exists im1map;

CREATE TABLE IF NOT EXISTS im1map (
    id INT auto_increment NOT null,
    im2 INT NOT NULL,
    im1 INT NOT NULL,
    PRIMARY KEY (id),
    INDEX map_s_c (im2,im1)
                                  )
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4;

-- -----------------------------------------------------
DROP TABLE IF EXISTS im_schema;
CREATE TABLE im_schema
   (
   dbid int,
   version int,
    PRIMARY KEY (dbid)
   );
INSERT INTO im_schema
(dbid, version)
VALUES
(1,1);



-- -----------------------------------------------------
DROP TABLE IF EXISTS namespace ;

CREATE TABLE IF NOT EXISTS namespace (
  dbid INT NOT NULL AUTO_INCREMENT COMMENT 'Unique prefix DBID',
  iri VARCHAR(255) NOT NULL COMMENT 'Namespace iri',
  prefix VARCHAR(50) NOT NULL COMMENT 'Namespace default prefix (alias)',
  name VARCHAR(255) NULL COMMENT 'name of namespace',
  updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (dbid),
  UNIQUE INDEX ns_iri_uq (iri ASC) ,
  UNIQUE INDEX ns_prefix_uq (prefix ASC) )
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;

-- ------------------------------------------------------
DROP TABLE IF EXISTS entity_type ;

CREATE TABLE IF NOT EXISTS entity_type (
  dbid BIGINT NOT NULL AUTO_INCREMENT,
  entity INT NOT NULL,
  type VARCHAR(140) NOT NULL,
  graph INT NOT NULL,
  PRIMARY KEY (dbid),
  INDEX ct_c_t (entity ASC, type ASC),
  INDEX ct_t_c (type ASC, entity ASC)
  )
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
DROP TABLE IF EXISTS entity ;

CREATE TABLE IF NOT EXISTS entity (
  dbid INT NOT NULL AUTO_INCREMENT,
  iri VARCHAR(140) CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_bin' NOT NULL,
  name VARCHAR(256) NULL DEFAULT NULL,
  description TEXT NULL DEFAULT NULL,
  code VARCHAR(50) CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_bin' NULL DEFAULT NULL,
  scheme VARCHAR(140) NULL DEFAULT NULL,
  status VARCHAR(140) NOT NULL DEFAULT 'http://endhealth.info/im#Draft',
  PRIMARY KEY (dbid),
  UNIQUE INDEX entity_iri_uq (iri ASC) ,
  UNIQUE INDEX entity_scheme_code_uq (scheme ASC, code ASC) ,
  index entity_name_idx (name(80) ASC) )
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
DROP TABLE IF EXISTS tct ;

CREATE TABLE IF NOT EXISTS tct (
  dbid INT NOT NULL AUTO_INCREMENT,
  ancestor INT NOT NULL,
  descendant INT NOT NULL,
  type INT NOT NULL,
  level INT NOT NULL,
  PRIMARY KEY (dbid),
  INDEX tct_anc_dec_idx (ancestor ASC,descendant ASC,type ASC) ,
  INDEX tct_descendent_idx (descendant ASC, ancestor,type ASC) )
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;
-- ------------------------------

DROP TABLE IF EXISTS term_code ;

CREATE TABLE IF NOT EXISTS term_code (
  dbid INT NOT NULL AUTO_INCREMENT,
  entity INT NOT NULL,
  term VARCHAR(256) NULL DEFAULT NULL,
  code VARCHAR(50) NULL DEFAULT NULL,
  graph INT not null,
  PRIMARY KEY (dbid),
  INDEX ct_tcs_idx (term(50),entity,graph ASC) ,
  INDEX ct_code_idx(code ASC),
  INDEX ct_eg_idx(entity,graph)
  )
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;




-- -----------------------------------------------------
DROP TABLE IF EXISTS tpl ;

CREATE TABLE IF NOT EXISTS tpl (
  dbid bigint  NOT NULL auto_increment,
  subject INT  NOT NULL,
  blank_node BIGINT NULL DEFAULT NULL,
  graph INT NULL DEFAULT NULL,
  predicate INT NOT NULL,
  object INT NULL,
  literal VARCHAR(16000) NULL,
  functional TINYINT NOT NULL DEFAULT 0,
  PRIMARY KEY (dbid),
   INDEX tpl_pred_sub_idx (predicate ASC,subject ASC,blank_node) ,
   INDEX tpl_pred_oc_idx (predicate ASC,object ASC) ,
  INDEX tpl_sub_graph_idx (subject ASC,graph ASC) ,
   INDEX tpl_sub_pred_obj (subject ASC, predicate, object,blank_node),
   INDEX tpl_ob_pred_sub (object ASC, predicate,subject,blank_node),
  INDEX tpl_l_pred_sub (literal(50) ASC, predicate,subject,blank_node),
   CONSTRAINT tpl_blank_fk
   FOREIGN KEY (blank_node)
   REFERENCES tpl (dbid)
   ON DELETE CASCADE
   ON UPDATE NO ACTION,
  CONSTRAINT tpl_sub_fk
      FOREIGN KEY (subject)
          REFERENCES entity (dbid)
          ON DELETE CASCADE
          ON UPDATE NO ACTION,
  CONSTRAINT tpl_pred_fk
      FOREIGN KEY (predicate)
          REFERENCES entity (dbid)
          ON DELETE CASCADE
          ON UPDATE NO ACTION,
  CONSTRAINT tpl_ob_fk
      FOREIGN KEY (object)
          REFERENCES entity (dbid)
          ON DELETE CASCADE
          ON UPDATE NO ACTION
    )
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;

-- -----------------------------------------------------
DROP TABLE IF EXISTS inst_entity ;

CREATE TABLE IF NOT EXISTS inst_entity (
    dbid INT NOT NULL AUTO_INCREMENT,
    iri VARCHAR(140) CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_bin' NOT NULL,
    name VARCHAR(256) NULL DEFAULT NULL,
    description TEXT NULL DEFAULT NULL,
    code VARCHAR(50) CHARACTER SET 'utf8mb4' COLLATE 'utf8mb4_bin' NULL DEFAULT NULL,
    scheme VARCHAR(140) NULL DEFAULT NULL,
    status VARCHAR(140) NOT NULL DEFAULT 'http://endhealth.info/im#Draft',
    PRIMARY KEY (dbid),
    UNIQUE INDEX inst_entity_iri_uq (iri ASC) ,
    UNIQUE INDEX inst_entity_scheme_code_uq (scheme ASC, code ASC) ,
    index inst_entity_name_idx (name(80) ASC) )
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;

-- -----------------------------------------------------
DROP TABLE IF EXISTS inst_tpl ;

CREATE TABLE IF NOT EXISTS inst_tpl (
   dbid bigint  NOT NULL auto_increment,
   subject INT  NOT NULL,
   blank_node BIGINT NULL DEFAULT NULL,
   graph INT NULL DEFAULT NULL,
   predicate INT NOT NULL,
   instance INT NULL,
   object INT NULL,
   literal VARCHAR(16000) NULL,
   functional TINYINT NOT NULL DEFAULT 0,
   PRIMARY KEY (dbid),
   INDEX inst_tpl_pred_sub_idx (predicate ASC,subject ASC,blank_node) ,
   INDEX inst_tpl_pred_oc_idx (predicate ASC,object ASC) ,
   INDEX inst_tpl_sub_graph_idx (subject ASC,graph ASC) ,
   INDEX inst_tpl_sub_pred_obj (subject ASC, predicate, object,blank_node),
   INDEX inst_tpl_ob_pred_sub (object ASC, predicate,subject,blank_node),
   INDEX inst_tpl_l_pred_sub (literal(50) ASC, predicate,subject,blank_node),
   CONSTRAINT inst_tpl_blank_fk
       FOREIGN KEY (blank_node)
           REFERENCES inst_tpl (dbid)
           ON DELETE CASCADE
           ON UPDATE NO ACTION,
   CONSTRAINT inst_tpl_sub_fk
       FOREIGN KEY (subject)
           REFERENCES inst_entity (dbid)
           ON DELETE CASCADE
           ON UPDATE NO ACTION,
   CONSTRAINT inst_tpl_pred_fk
       FOREIGN KEY (predicate)
           REFERENCES entity (dbid)
           ON DELETE CASCADE
           ON UPDATE NO ACTION,
   CONSTRAINT inst_tpl_inst_fk
       FOREIGN KEY (instance)
           REFERENCES inst_entity (dbid)
           ON DELETE CASCADE
           ON UPDATE NO ACTION,
   CONSTRAINT inst_tpl_ob_fk
       FOREIGN KEY (object)
           REFERENCES entity (dbid)
           ON DELETE CASCADE
           ON UPDATE NO ACTION
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
DROP TABLE IF EXISTS entity_search ;

CREATE TABLE IF NOT EXISTS entity_search(
    dbid INT NOT NULL AUTO_INCREMENT,
    term VARCHAR(256) NULL DEFAULT NULL,
    entity_dbid INT NOT NULL,
    weighting INT NOT NULL DEFAULT 0,
    PRIMARY KEY(dbid),
    UNIQUE INDEX entity_search_term_entity_uq (term, entity_dbid),
    CONSTRAINT entity_dbid_fk
        FOREIGN KEY (entity_dbid)
            REFERENCES entity (dbid)
            ON DELETE CASCADE
            ON UPDATE NO ACTION,
    FULLTEXT INDEX entity_search_term_ftx (term)
    )
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;
-- ---------------------------------
DROP TABLE IF EXISTS config ;

CREATE TABLE IF NOT EXISTS config
(
    dbid   INT         NOT NULL AUTO_INCREMENT,
    name   VARCHAR(50) NOT NULL,
    config JSON        NULL,
    PRIMARY KEY (dbid),
    UNIQUE INDEX cf_name_uq (name ASC)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4;



-- -----------------------------------------------------

DROP TABLE IF EXISTS workflow;

CREATE TABLE IF NOT EXISTS workflow (
    dbid INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(50) NOT NULL,
    config JSON NOT NULL,

    PRIMARY KEY workflow_pk (dbid),
    UNIQUE INDEX workflow_uq (name ASC)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4;

-- -----------------------------------------------------

DROP TABLE IF EXISTS task;

CREATE TABLE IF NOT EXISTS task (
    dbid INT NOT NULL AUTO_INCREMENT,
    workflow INT NOT NULL,
    id VARCHAR(200) NOT NULL COLLATE utf8_bin,
    name TEXT not null,
    state VARCHAR(100) NOT NULL,

    PRIMARY KEY task_pk (dbid),

    UNIQUE INDEX task_uq (workflow, id)
)
    ENGINE = InnoDB
    DEFAULT CHARACTER SET = utf8mb4;

-- -----------------------------------------------------

DROP TABLE IF EXISTS im1_dbid_scheme_code;

CREATE TABLE im1_dbid_scheme_code (
    dbid INT PRIMARY KEY,
    scheme VARCHAR(150) NOT NULL,
    code VARCHAR(40) NOT NULL COLLATE utf8mb4_bin,

    UNIQUE KEY im1_dbid_scheme_code_uq (scheme, code)

) ENGINE = InnoDB
    DEFAULT CHAR SET = utf8mb4;

-- LOAD DATA LOCAL INFILE 'H:/ImportData/IMv1/IMv1DbidSchemeCode.txt' INTO TABLE im1_dbid_scheme_code FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES;

-- -----------------------------------------------------

DROP TABLE IF EXISTS im1_scheme_map;

CREATE TABLE im1_scheme_map (
    scheme VARCHAR(150) NOT NULL,
    namespace VARCHAR(255) NOT NULL,

    INDEX im1_scheme_map_ns_idx (namespace)
) ENGINE = InnoDB
  DEFAULT CHAR SET = utf8mb4;

INSERT INTO im1_scheme_map
(scheme, namespace)
VALUES
('SNOMED', 'http://snomed.info/sct#'),
('READ2', 'http://endhealth.info/vis#'),
('READ2', 'http://endhealth.info/emis#'),
('EMIS_LOCAL', 'http://endhealth.info/emis#'),
('TPP_LOCAL', 'http://endhealth.info/tpp#'),
('CTV3', 'http://endhealth.info/tpp#'),
('OPCS4', 'http://endhealth.info/opcs4#'),
('VISION_LOCAL', 'http://endhealth.info/opcs4#'),
('ICD10', 'http://endhealth.info/icd10#'),
('BartsCerner', 'http://endhealth.info/bc#'),
('ImperialCerner', 'http://endhealth.info/impc#'),
('LE_TYPE', 'http://endhealth.info/im#'),
('CM_DiscoveryCode', 'http://endhealth.info/im#');

-- SET SQL_MODE=@OLD_SQL_MODE;
-- SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
-- SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

-- -----------------------------------------------------

DROP TABLE IF EXISTS map_document;

CREATE TABLE map_document (
                              dbid int NOT NULL AUTO_INCREMENT,
                              document BLOB NOT NULL,
                              filename VARCHAR(255) UNIQUE NOT NULL,
                              PRIMARY KEY (dbid)
);

