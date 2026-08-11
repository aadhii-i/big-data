campus = LOAD '/user/aadhi/pig-q2/input/campus.txt'
USING PigStorage('\n')
AS (line:chararray);

words = FOREACH campus
GENERATE FLATTEN(TOKENIZE(line)) AS word;

DUMP words;
