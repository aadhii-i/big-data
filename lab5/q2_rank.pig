ranked = LOAD '/user/aadhi/rank_output/part-r-00000'
USING PigStorage('\t')
AS (roll_no:int, rank:int);

DUMP ranked;
