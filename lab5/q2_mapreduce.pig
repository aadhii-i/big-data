students = LOAD 'students.csv'
USING PigStorage(',')
AS (roll_no:int, name:chararray, department:chararray, marks:int);

rank_input = FOREACH students GENERATE roll_no, marks;

ranked = MAPREDUCE 'legacy/legacy-rank.jar'
    STORE rank_input INTO '/user/aadhi/pig_rank_input' USING PigStorage(',')
    LOAD '/user/aadhi/pig_rank_output' USING PigStorage('\t')
    AS (roll_no:int, rank:int)
    `com.college.LegacyRankJob`;

DUMP ranked;
