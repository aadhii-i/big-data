REGISTER 'grade.py' USING jython AS py;

students = LOAD 'students.csv' USING PigStorage(',')
AS (roll_no:int, name:chararray, department:chararray, marks:int);

graded = FOREACH students GENERATE
roll_no, name, department, marks, py.grade(marks) AS grade;

DUMP graded;
