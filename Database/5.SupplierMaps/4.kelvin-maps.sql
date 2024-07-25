USE im3;

SELECT l.dbid, l.iri, l.name, s.dbid, s.iri, s.name
FROM entity l
       JOIN entity s
WHERE (l.iri = 'tpp:Y11c5' AND s.iri = 'sn:160734000')
   OR (l.iri = 'tpp:Y11c4' AND s.iri = 'sn:248171000000108')
   OR (l.iri = 'tpp:YA744' AND s.iri = 'sn:SN_160734000')
   OR (l.iri = 'tpp:Y15ca' AND s.iri = 'sn:160734000')
   OR (l.iri = 'emis:ALLERGY138185NEMIS' AND s.iri = 'sn:1324661000000105')
;
