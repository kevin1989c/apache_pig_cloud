REGISTER /usr/local/pig/lib/piggybank.jar;
A = LOAD 'newdata.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE') AS (Score:int, Id:int, OwnerDisplayName,OwnerUserId:int£¬ Title, Body, ViewCount:int);
B = FOREACH A GENERATE Score, Id, OwnerUserId, OwnerDisplayName, Title;
C = LOAD 'newdata1.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE') AS (Score:int, Id:int, OwnerDisplayName,OwnerUserId:int£¬ Title, Body, ViewCount:int);
D = FOREACH C GENERATE Score, Id, OwnerUserId, OwnerDisplayName, Title;
E = LOAD 'newdata2.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE') AS (Score:int, Id:int, OwnerDisplayName,OwnerUserId:int£¬ Title, Body, ViewCount:int);
F = FOREACH E GENERATE Score, Id,OwnerUserId, OwnerDisplayName, Title;
G = union D, F, B;


BI = GROUP G BY (OwnerUserId,OwnerDisplayName);
P = FOREACH BI GENERATE SUM(G.Score), group; 

L = ORDER P BY $0 desc;
I = LIMIT L 10;
DUMP I;
STORE I into '/user/root/out/task2';

