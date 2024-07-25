USE im_source;

-- ########################### NOTE: #############################
-- ## USE INSERT IGNORE TO LEAVE SOURCE FILE MAPS GOING FORWARD ##
-- ###############################################################

-- #################### ICD10 ####################
INSERT IGNORE INTO icd10_opcs4_maps
  (refsetId, mapTarget, referencedComponentId)
VALUES (999002271000000101, 'B34.2', '1240751000000100'), -- 'Coronavirus infection, unspecified site'
       (999002271000000101, 'B97.2',
        '1240751000000100'),                              -- 'Coronavirus as the cause of diseases classified to other chapters'
       (999002271000000101, 'U07.1', '1240751000000100');
-- 'Diagnosis of COVID-19'

-- #################### EMIS ####################
INSERT IGNORE INTO emis_read_snomed
  (readTermId, readCode, codeTerm, snomedCTEntityId)
VALUES ('EMISNQCO303', 'EMISNQCO303', 'Confirmed 2019-nCoV (Wuhan) infection', 1240751000000100),
       ('EMISNQEX58', 'EMISNQEX58', 'Exposure to 2019-nCoV (Wuhan) infection', 1240431000000104),
       ('EMISNQEX59', 'EMISNQEX59', 'Excluded 2019-nCoV (Wuhan) infection', 1240591000000102),
       ('EMISNQSU106', 'EMISNQSU106', 'Suspected 2019-nCoV (Wuhan) infection', 1240761000000102),
       ('EMISNQTE31', 'EMISNQTE31', 'Tested for 2019-nCoV (Wuhan) infection', 1240461000000109),
       ('^ESCT1299035', '^ESCT1299035', 'Exposure to 2019-nCoV (novel coronavirus) infection', 1240431000000104),
       ('^ESCT1299038', '^ESCT1299038', 'Close exposure to 2019-nCoV (novel coronavirus) infection', 1240441000000108),
       ('^ESCT1299041', '^ESCT1299041', 'Telephone consultation for suspected 2019-nCoV (novel coronavirus)',
        1240451000000106),
       ('^ESCT1299053', '^ESCT1299053',
        'Detection of 2019-nCoV (novel coronavirus) using polymerase chain reaction technique', 1240511000000106),
       ('^ESCT1299059', '^ESCT1299059', 'Myocarditis caused by 2019-nCoV (novel coronavirus)', 1240531000000103),
       ('^ESCT1299065', '^ESCT1299065', 'Pneumonia caused by 2019-nCoV (novel coronavirus)', 1240551000000105),
       ('^ESCT1299074', '^ESCT1299074', '2019-nCoV (novel coronavirus) detected', 1240581000000104),
       ('^ESCT1299077', '^ESCT1299077', '2019-nCoV (novel coronavirus) not detected', 1240591000000102),
       ('^ESCT1299101', '^ESCT1299101', 'Educated about 2019-nCoV (novel coronavirus) infection', 1240711000000104),
       ('^ESCT1299104', '^ESCT1299104', 'Advice given about 2019-nCoV (novel coronavirus) infection', 1240721000000105),
       ('^ESCT1299105', '^ESCT1299105', 'Advice given about Wuhan 2019-nCoV (novel coronavirus) infection',
        1240721000000105),
       ('^ESCT1299107', '^ESCT1299107', 'Advice given about 2019-nCoV (novel coronavirus) by telephone',
        1240731000000107),
       ('^ESCT1299113', '^ESCT1299113', 'Disease caused by 2019-nCoV (novel coronavirus)', 1240751000000100),
       ('^ESCT1299116', '^ESCT1299116', 'Suspected disease caused by 2019-nCoV (novel coronavirus)', 1240761000000102),
       ('^ESCT1299117', '^ESCT1299117', 'Suspected disease caused by 2019 novel coronavirus', 1240761000000102),
       ('^ESCT1299118', '^ESCT1299118', 'Suspected disease caused by Wuhan 2019-nCoV (novel coronavirus)',
        1240761000000102),
       ('^ESCT1299119', '^ESCT1299119',
        '2019-nCoV (novel coronavirus) vaccination invitation short message service text message sent',
        1240781000000106),
       ('^ESCT1300222', '^ESCT1300222', 'High risk category for developing complication from COVID-19 infection',
        1300561000000107),
       ('^ESCT1300223', '^ESCT1300223', 'Moderate risk category for developing complication from COVID-19 infection',
        1300571000000100),
       ('^ESCT1300224', '^ESCT1300224', 'Low risk category for developing complication from COVID-19 infection',
        1300591000000101),
       ('^ESCT1300228', '^ESCT1300228', 'COVID-19 confirmed by laboratory test', 1300721000000109),
       ('^ESCT1300229', '^ESCT1300229', 'COVID-19 confirmed using clinical diagnostic criteria', 1300731000000106),
       ('^ESCT1300240', '^ESCT1300240', 'Signposting to NHS online isolation note service', 1321061000000100),
       ('^ESCT1300242', '^ESCT1300242', 'Self-isolation note issued to patient', 1321081000000109),
       ('^ESCT1300245', '^ESCT1300245', 'COVID-19 excluded by laboratory test', 1321111000000101),
       ('^ESCT1300247', '^ESCT1300247', 'Self-isolation to prevent exposure of community to contagion',
        1321131000000109),
       ('^ESCT1300249', '^ESCT1300249', 'Shielding of household to prevent exposure of uninfected subject to contagion',
        1321141000000100),
       ('^ESCT1300251', '^ESCT1300251', 'Shielding of uninfected subject to prevent exposure to contagion',
        1321151000000102),
       ('^ESCT1300253', '^ESCT1300253', 'Household isolation to prevent exposure of community to contagion',
        1321161000000104),
       ('^ESCT1300255', '^ESCT1300255', 'Provision of advice, assessment or treatment limited due to COVID-19 pandemic',
        1321171000000106),
       ('^ESCT1300260', '^ESCT1300260', 'Consultation via video conference not available', 1321221000000103),
       ('^ESCT1300261', '^ESCT1300261', 'Signposting to CHMS (COVID-19 Home Management Service)', 1321231000000101),
       ('^ESCT1301218', '^ESCT1301218',
        'Close exposure to SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2) infection', 1240441000000108),
       ('^ESCT1301219', '^ESCT1301219',
        'Telephone consultation for suspected SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2)',
        1240451000000106),
       ('^ESCT1301227', '^ESCT1301227',
        'Pneumonia caused by SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2)', 1240551000000105),
       ('^ESCT1301230', '^ESCT1301230', 'SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2) detected',
        1240581000000104),
       ('^ESCT1301232', '^ESCT1301232',
        'High priority for SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2) vaccination', 1240601000000108),
       ('^ESCT1301239', '^ESCT1301239',
        'Educated about SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2) infection', 1240711000000104),
       ('^ESCT1301240', '^ESCT1301240',
        'Advice given about SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2) infection', 1240721000000105),
       ('^ESCT1301241', '^ESCT1301241',
        'Advice given about SARS-CoV-2 (severe acute respiratory syndrome coronavirus 2) by telephone',
        1240731000000107),
       ('^ESCT1301243', '^ESCT1301243', 'COVID-19', 1240751000000100),
       ('^ESCT1301245', '^ESCT1301245', 'Suspected COVID-19', 1240761000000102);

-- #################### TPP ####################
INSERT IGNORE INTO tpp_local_codes
  (ctv3Text, ctv3Code, snomedCode)
VALUES ('Exposure to 2019-nCoV (Wuhan) infection', 'Y20ce', 1240431000000104),
       ('Suspected 2019-nCoV (Wuhan) infection', 'Y20cf', 1240761000000102),
       ('Tested for 2019-nCoV (Wuhan) infection', 'Y20d0', 1240511000000106),
       ('Confirmed 2019-nCoV (Wuhan) infection', 'Y20d1', 1240581000000104),
       ('Excluded 2019-nCoV (Wuhan) infection', 'Y20d2', 1240591000000102),
       ('Disease caused by 2019-nCoV (novel coronavirus)', 'Y20fa', 1240751000000100),
       ('Detection of 2019-nCoV (novel coronavirus) using polymerase chain reaction technique', 'Y210e',
        1240511000000106),
       ('Telephone consultation for suspected 2019-nCoV (novel coronavirus)', 'Y211b', 1240451000000106),
       ('Advice given about 2019-nCoV (novel coronavirus) by telephone', 'Y212c', 1240731000000107),
       ('Advice given about 2019-nCoV (novel coronavirus) infection', 'Y212d', 1240721000000105),
       ('Educated about 2019-nCoV (novel coronavirus) infection', 'Y212e', 1240711000000104),
       ('Provision of advice, assessment or treatment limited due to coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 pandemic (situation)',
        'Y22b6', 1240721000000105);

-- #################### VISION ####################
INSERT IGNORE INTO vision_local_codes
  (readTerm, readCode, snomedCode)
VALUES ('Suspected disease caused by 2019-nCoV (novel coronavirus)', '1JX1.', 1240761000000102),
       ('2019-nCoV (novel coronavirus) serology', '4J3R.', 1240741000000103),
       ('2019-nCoV (novel coronavirus) detected', '4J3R1', 1240581000000104),
       ('2019-nCoV (novel coronavirus) detected', '4J3R100', 1240581000000104),
       ('2019-nCoV (novel coronavirus) not detected', '4J3R2', 1240591000000102),
       ('2019-nCoV (novel coronavirus) vaccination', '65F0.', null),
       ('Exposure to 2019-nCoV (novel coronavirus) infection', '65PW1', 1240431000000104),
       ('Exposure to 2019-nCoV (novel coronavirus) infection', '65PW100', 1240431000000104),
       ('Advice given about 2019-nCoV (novel coronavirus) infection', '8CAO.', 1240721000000105),
       ('Advice given about 2019-nCoV (novel coronavirus) by telephone', '8CAO1', 1240731000000107),
       ('2019-nCoV (novel coronavirus) vaccination contraindicated', '8I23R', null),
       ('2019-nCoV (novel coronavirus) vaccination not indicated', '8I6t.', null),
       ('2019-nCoV (novel coronavirus) vaccination declined', '8IAI.', null),
       ('Telephone consultation for suspected 2019-nCoV (novel coronavirus)', '9N312', 1240731000000107),
       ('Did not attend 2019-nCoV (novel coronavirus) vaccination', '9Niq.', 1240631000000102),
       ('2019-nCoV (novel coronavirus) vaccination vacc invitation SMS sent', '9mb.', null),
       ('Disease caused by 2019-nCoV (novel coronavirus)', 'A7951', 1240751000000100);

-- #################### Barts ####################
INSERT IGNORE INTO barts_cerner
  (code, term, snomed_expression)
VALUES ('687309281', 'Coronavirus (2019-nCoV)', '1240511000000106'),
       ('700750587', 'Coronavirus (2019-nCoV) with Flu RSV', null),
       ('ddOYGVC0cM33G', 'SARS-CoV-2 RNA NOT detected', '1240591000000102'),
       ('ddOYGVC0cM33GsbmH', 'SARS-CoV-2 RNA NOT detected', '1240591000000102'),
       ('Fr1gvtFQJAIPq', 'SARS-CoV-2 RNA DETECTED', '1240581000000104'),
       ('Fr1gvtFQJAIPqpoHK', 'SARS-CoV-2 RNA DETECTED', '1240581000000104');
