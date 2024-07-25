USE im_source;

DROP TABLE IF EXISTS fhir_scheme;
CREATE TABLE fhir_scheme
(
  id   VARCHAR(50)  NOT NULL,
  name VARCHAR(255) NOT NULL
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

DROP TABLE IF EXISTS fhir_scheme_value;
CREATE TABLE fhir_scheme_value
(
  scheme VARCHAR(50)  NOT NULL,
  code   VARCHAR(20) COLLATE utf8_bin,
  term   VARCHAR(255) NOT NULL,
  map    VARCHAR(50)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

-- Define the schemes

INSERT INTO fhir_scheme
  (id, name)
VALUES ('FHIR_AG', 'FHIR Administrative Gender'),
       ('FHIR_EC', 'FHIR Ethnic Category'),
       ('FHIR_RT', 'FHIR Registration Type'),
       ('FHIR_RS', 'FHIR Registration Status'),
       ('FHIR_AS', 'FHIR Appointment Status'),
       ('FHIR_MSAT', 'FHIR Medication statement authorisation type'),
       ('FHIR_PRS', 'FHIR Procedure request status'),
       ('FHIR_RFP', 'FHIR Referral priority'),
       ('FHIR_RFT', 'FHIR Referral type'),

       ('FHIR_AU', ' FHIR Address use'),
       ('FHIR_CPS', 'FHIR Contact point system'),
       ('FHIR_CPU', 'FHIR Contact point use'),
       ('FHIR_CEP', 'FHIR Condition episodicity')
;

-- Create the scheme values

INSERT INTO fhir_scheme_value
  (scheme, code, term)
VALUES ('FHIR_AG', 'male', 'Male'),
       ('FHIR_AG', 'female', 'Female'),
       ('FHIR_AG', 'other', 'Other'),
       ('FHIR_AG', 'unknown', 'Unknown'),

       ('FHIR_EC', 'A', 'British'),
       ('FHIR_EC', 'B', 'Irish'),
       ('FHIR_EC', 'C', 'Any other White background'),
       ('FHIR_EC', 'D', 'White and Black Caribbean'),
       ('FHIR_EC', 'E', 'White and Black African'),
       ('FHIR_EC', 'F', 'White and Asian'),
       ('FHIR_EC', 'G', 'Any other mixed background'),
       ('FHIR_EC', 'H', 'Indian'),
       ('FHIR_EC', 'J', 'Pakistani'),
       ('FHIR_EC', 'K', 'Bangladeshi'),
       ('FHIR_EC', 'L', 'Any other Asian background'),
       ('FHIR_EC', 'M', 'Caribbean'),
       ('FHIR_EC', 'N', 'African'),
       ('FHIR_EC', 'P', 'Any other Black baground'),
       ('FHIR_EC', 'R', 'Chinese'),
       ('FHIR_EC', 'S', 'Any other ethnic group'),
       ('FHIR_EC', 'Z', 'Not stated'),

       ('FHIR_RT', 'E', 'Emergency'),
       ('FHIR_RT', 'IN', 'Immediately Necessary'),
       ('FHIR_RT', 'R', 'Regular/GMS'),
       ('FHIR_RT', 'T', 'Temporary'),
       ('FHIR_RT', 'P', 'Private'),
       ('FHIR_RT', 'O', 'Other'),
       ('FHIR_RT', 'D', 'Dummy/Synthetic'),
       ('FHIR_RT', 'C', 'Community'),
       ('FHIR_RT', 'W', 'Walk-In'),
       ('FHIR_RT', 'MS', 'Minor Surgery'),
       ('FHIR_RT', 'CHS', 'Child Health Services'),
       ('FHIR_RT', 'N', 'Contraceptive Services'),
       ('FHIR_RT', 'Y', 'Yellow Fever'),
       ('FHIR_RT', 'M', 'Maternity Services'),
       ('FHIR_RT', 'PR', 'Pre-Registration'),
       ('FHIR_RT', 'SH', 'Sexual Health'),
       ('FHIR_RT', 'V', 'Vasectomy'),
       ('FHIR_RT', 'OH', 'Out of Hours'),

       ('FHIR_RS', 'PR1', 'Patient has presented'),
       ('FHIR_RS', 'PR2', 'Medical card received'),
       ('FHIR_RS', 'PR3', 'Application Form FP1 submitted'),
       ('FHIR_RS', 'R1', 'Registered'),
       ('FHIR_RS', 'R2', 'Medical record sent by FHSA'),
       ('FHIR_RS', 'R3', 'Record Received'),
       ('FHIR_RS', 'R4', 'Left practice. Still registered'),
       ('FHIR_RS', 'R5', 'Correctly registered'),
       ('FHIR_RS', 'R6', 'Short stay'),
       ('FHIR_RS', 'R7', 'Long stay'),
       ('FHIR_RS', 'R8', 'Services'),
       ('FHIR_RS', 'R9', 'Service dependant'),
       ('FHIR_RS', 'D1', 'Death'),
       ('FHIR_RS', 'D2', 'Dead (Practice notification)'),
       ('FHIR_RS', 'D3', 'Record Requested by FHSA'),
       ('FHIR_RS', 'D4', 'Removal to New HA/HB'),
       ('FHIR_RS', 'D5', 'Internal transfer'),
       ('FHIR_RS', 'D6', 'Mental hospital'),
       ('FHIR_RS', 'D7', 'Embarkation'),
       ('FHIR_RS', 'D8', 'New HA/HB - same GP'),
       ('FHIR_RS', 'D9', 'Adopted child'),
       ('FHIR_RS', 'R10', 'Removal from Residential Institute'),
       ('FHIR_RS', 'D10', 'Deduction at GPs request'),
       ('FHIR_RS', 'D11', 'Registration cancelled'),
       ('FHIR_RS', 'D12', 'Deduction at patients request'),
       ('FHIR_RS', 'D13', 'Other reason'),
       ('FHIR_RS', 'D14', 'Returned undelivered'),
       ('FHIR_RS', 'D15', 'Internal transfer - address change'),
       ('FHIR_RS', 'D16', 'Internal transfer within partnership'),
       ('FHIR_RS', 'D17', 'Correspondence states \'gone away\''),
       ('FHIR_RS', 'D18', 'Practice advise outside of area'),
       ('FHIR_RS', 'D19', 'Practice advise patient no longer resident'),
       ('FHIR_RS', 'D20', 'Practice advise removal via screening system'),
       ('FHIR_RS', 'D21', 'Practice advise removal via vaccination data'),
       ('FHIR_RS', 'D22', 'Records sent back to FHSA'),
       ('FHIR_RS', 'D23', 'Records received by FHSA'),
       ('FHIR_RS', 'D24', 'Registration expored'),

       ('FHIR_AS', 'proposed', 'Proposed'),
       ('FHIR_AS', 'pending', 'Pending'),
       ('FHIR_AS', 'booked', 'Booked'),
       ('FHIR_AS', 'arrived', 'Arrived'),
       ('FHIR_AS', 'fulfilled', 'Fulfilled'),
       ('FHIR_AS', 'cancelled', 'Cancelled'),
       ('FHIR_AS', 'noshow', 'No Show'),

       ('FHIR_MSAT', 'acute', 'Acute'),
       ('FHIR_MSAT', 'repeat', 'Repeat'),
       ('FHIR_MSAT', 'repeatDispensing', 'Repeat dispensing'),
       ('FHIR_MSAT', 'automatic', 'Automatic'),

       ('FHIR_PRS', 'proposed', 'Proposed'),
       ('FHIR_PRS', 'draft', 'Draft'),
       ('FHIR_PRS', 'requested', 'Requested'),
       ('FHIR_PRS', 'received', 'Received'),
       ('FHIR_PRS', 'accepted', 'Accepted'),
       ('FHIR_PRS', 'in-progress', 'In Progress'),
       ('FHIR_PRS', 'completed', 'Completed'),
       ('FHIR_PRS', 'suspended', 'Suspended'),
       ('FHIR_PRS', 'rejected', 'Rejected'),
       ('FHIR_PRS', 'aborted', 'Aborted'),


       ('FHIR_RFP', '0', 'Routine'),
       ('FHIR_RFP', '1', 'Urgent'),
       ('FHIR_RFP', '2', 'Two week wait'),
       ('FHIR_RFP', '3', 'Soon'),

       ('FHIR_RFT', 'A', 'Assessment'),
       ('FHIR_RFT', 'C', 'Community care'),
       ('FHIR_RFT', 'D', 'Day care'),
       ('FHIR_RFT', 'E', 'Assessment & Education'),
       ('FHIR_RFT', 'I', 'Investigation'),
       ('FHIR_RFT', 'M', 'Management advice'),
       ('FHIR_RFT', 'N', 'Admission'),
       ('FHIR_RFT', 'O', 'Outpatient'),
       ('FHIR_RFT', 'P', 'Performance of a procedure/operation'),
       ('FHIR_RFT', 'R', 'Patient assurance'),
       ('FHIR_RFT', 'S', 'Self referral'),
       ('FHIR_RFT', 'T', 'Treatment'),
       ('FHIR_RFT', 'U', 'Unknown'),

       ('FHIR_AU', 'home', 'Home'),
       ('FHIR_AU', 'work', 'Work'),
       ('FHIR_AU', 'temp', 'Temporary'),
       ('FHIR_AU', 'old', 'Old / Incorrect'),

       ('FHIR_CPS', 'phone', 'Phone'),
       ('FHIR_CPS', 'fax', 'Fax'),
       ('FHIR_CPS', 'email', 'Email'),
       ('FHIR_CPS', 'pager', 'Pager'),
       ('FHIR_CPS', 'other', 'URL'),

       ('FHIR_CPU', 'home', 'Home'),
       ('FHIR_CPU', 'work', 'Work'),
       ('FHIR_CPU', 'temp', 'Temporary'),
       ('FHIR_CPU', 'old', 'Old'),
       ('FHIR_CPU', 'mobile', 'Mobile'),

       ('FHIR_CEP', 'First', 'First'),
       ('FHIR_CEP', 'New', 'New'),
       ('FHIR_CEP', 'Other', 'Other'),
       ('FHIR_CEP', 'Cause of Death', 'Cause of Death'),
       ('FHIR_CEP', 'Ongoing Episode', 'Ongoing Episode')
;

-- Create the scheme values with map overrides
INSERT INTO fhir_scheme_value
  (scheme, code, term, map)
VALUES ('FHIR_CEP', 'New Episode', 'New Episode', 'DS_FHIR_CEP_New'),
       ('FHIR_CEP', 'Other Episode', 'Other Episode', 'DS_FHIR_CEP_Other')
;
