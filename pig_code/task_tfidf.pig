REGISTER /usr/local/pig/lib/piggybank.jar;
A = LOAD 'test.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE') AS (Score:int, Id:int, OwnerDisplayName,OwnerUserId:int£¬ Title, Body, ViewCount:int);


A1 = FOREACH A GENERATE OwnerUserId, Body;
C1 = FOREACH A1 GENERATE OwnerUserId AS userid, FLATTEN(TOKENIZE(Body)) AS word;
C = FOREACH C1 GENERATE word AS word, userid AS userid;



B1 = FOREACH C GENERATE userid;
B2 = DISTINCT B1;
B3 = GROUP B2 BY $0;
B4 = FOREACH B3 GENERATE COUNT(B2);
B5 = GROUP B4 ALL;
B6 = FOREACH B5 GENERATE SUM(B4.$0) AS userNums;




D = GROUP C BY (userid, word);

D1 = FOREACH D GENERATE FLATTEN(group) AS (userid, word), COUNT(D.$0) AS wordtimes_peruser;

D2 = GROUP D1 BY userid;

D3 = FOREACH D2 GENERATE SUM(D1.wordtimes_peruser) AS userword_totalnum,  FLATTEN (D1.(word, wordtimes_peruser)) AS (word, wordtimes_peruser), group AS userid;

D4 = FOREACH D3 GENERATE word AS word, userid AS userid, (wordtimes_peruser/userword_totalnum) AS tf;

D5 = GROUP D4 BY word;

D6 = FOREACH D5 GENERATE FLATTEN (D4) AS (word, userid, tf), count(D4.word) AS wordin_usernum;

D7 = FOREACH D6 {idf = LOG((float)B6.userNo/(float)wordin_usernum); tfidf = (float)tf*idf; GENERATE word, userid, tfidf;}; 


E = GROUP D7 BY userid;
E1 = FOREACH E {top = ORDER D7 BY D7.tfidf; top_ten = LIMIT top 10; GENERATE userid, top_ten;};





DUMP E1;
STORE E1 into '/user/root/out/task~tfidf';

