-- PIVOT TABLE: SHOW NUMBER OF RECORDS PER DAG RUN DATE:
SELECT 
  a.data_dt,
  a.n_entries as 'encounters', 
  b.n_entries as 'medications',
  c.n_entries as 'conditions', 
  d.n_entries as 'patients'
FROM
  (select data_dt, count(data_dt) as n_entries from ehr_db.encounters group by data_dt) a
INNER JOIN 
  (select data_dt, count(data_dt) as n_entries from ehr_db.medications group by data_dt) b
ON a.data_dt = b.data_dt
INNER JOIN
  (select data_dt, count(data_dt) as n_entries from ehr_db.conditions group by data_dt) c
ON a.data_dt = c.data_dt
INNER JOIN 
  (select data_dt, count(data_dt) as n_entries from ehr_db.patients group by data_dt) d
ON a.data_dt = d.data_dt;

