campus = LOAD '/user/aadhi/pig-q2/input/campus.txt'
USING PigStorage('\n')
AS (line:chararray);

words = FOREACH campus
GENERATE FLATTEN(TOKENIZE(line)) AS word;

clean_words = FOREACH words
GENERATE LOWER(REPLACE(word, '[^a-zA-Z0-9]', '')) AS word;

grouped_words = GROUP clean_words BY word;

word_count = FOREACH grouped_words
GENERATE group AS word, COUNT(clean_words) AS frequency;

DUMP word_count;
