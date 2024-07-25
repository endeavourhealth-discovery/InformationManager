use im6;
INSERT INTO workflow (name, config)
VALUES ('Concept',
        '{"type":"Concept","initialState":"Editing","validStates":["Draft","Released","Editing"],"validEvents":["COMPLETE","EDIT","APPROVE"],"transitions":{"Draft":{"EDIT":"Editing","APPROVE":"Released"},"Released":{"EDIT":"Editing"},"Editing":{"COMPLETE":"Draft","EDIT":"Editing"}}}'),
       ('UPRN',
        '{"type":"UPRN","initialState":"uploading","validStates":["processing","uploading","failed","complete","downloaded"],"validEvents":["COMPLETE","PROCESS","DOWNLOAD","FAIL"],"transitions":{"processing":{"COMPLETE":"complete"},"uploading":{"PROCESS":"processing","FAIL":"failed"},"complete":{"DOWNLOAD":"downloaded"}}}');

/*INSERT INTO task (workflow, id, state)
VALUES
    (1, 'http://endhealth.info/im#27371000252113', 'Editing'),
    (1, 'http://endhealth.info/im#30661000252111', 'Editing'),
    (1, 'http://endhealth.info/im#26951000252119', 'Draft'),
    (1, 'http://endhealth.info/im#29481000252113', 'Released'),

    (2, 'UPRN1', 'uploading'),
    (2, 'UPRN2', 'failed'),
    (2, 'UPRN3', 'downloaded'),
    (2, 'UPRN4', 'downloaded'),
    (2, 'UPRN5', 'complete'),
    (2, 'UPRN6', 'processing')
;*/
