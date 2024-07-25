use im_live_2021;

LOAD DATA LOCAL INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 5.7\\Uploads\\closure_v1.csv'
  INTO TABLE entity_tct
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\r\n'
  (@source, @target, @level)
  SET source = @source,
    property = 92842,
    level = @level,
    target = @target;
