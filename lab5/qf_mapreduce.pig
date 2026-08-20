students = LOAD '/user/aadhi/students.csv'
USING PigStorage(',')
AS (roll_no:int, name:chararray, age:int, marks:int);

ranked = RANK students BY marks DESC;

DUMP ranked;
