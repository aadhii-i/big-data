students = LOAD '/user/aadhi/pig-q1-new/input/students.csv'
USING PigStorage(',')
AS (id:int, name:chararray, department:chararray, marks:int);

high_marks = FILTER students BY marks > 80;

DUMP high_marks;
