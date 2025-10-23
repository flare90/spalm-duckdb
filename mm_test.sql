.timer on

set threads = 1;

set disabled_optimizers = 'compressed_materialization, statistics_propagation';
PRAGMA explain_output='all';


CREATE TABLE R (ri INTEGER, rk INTEGER, rv DOUBLE);
INSERT INTO R VALUES (3, -2, 10), (0, 1, 3), (1, 8, 2), (2, 3, 1);
CREATE TABLE S (si INTEGER, sk INTEGER, sv DOUBLE);
INSERT INTO S VALUES (4, -1, 10), (0, 3, 8), (1, 1, 4), (3, 8, 5);

SELECT ri, si, SUM(rv * sv)
FROM R,S WHERE R.rk = S.sk
GROUP BY ri, si;
