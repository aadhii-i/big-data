REGISTER 'student-udfs.jar';
DEFINE IsTopper com.college.IsTopper();

students = LOAD 'students.csv' USING PigStorage(',')
AS (roll_no:int, name:chararray, department:chararray, marks:int);

toppers = FILTER students BY IsTopper(marks);

DUMP toppers;
