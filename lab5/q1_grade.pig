REGISTER 'grade-udf.jar';
DEFINE Grade com.college.Grade();

students = LOAD 'students.csv' USING PigStorage(',')
AS (roll_no:int, name:chararray, department:chararray, marks:int);

graded = FOREACH students GENERATE
roll_no, name, department, marks, Grade(marks) AS grade;

DUMP graded;
