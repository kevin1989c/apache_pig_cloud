REGISTER /usr/local/pig/lib/piggybank.jar;
A = LOAD 'newdata.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE') AS (Score:int, Id:int, OwnerDisplayName,OwnerUserId:int£¬ Title, Body, ViewCount:int
);
B = FOREACH A GENERATE OwnerUserId, Body;
C = LOAD 'newdata1.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE') AS (Score:int, Id:int, OwnerDisplayName,OwnerUserId:int£¬ Title, Body, ViewCount:int
);
D = FOREACH C GENERATE OwnerUserId, Body;
E = LOAD 'newdata2.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE') AS Score:int, Id:int, OwnerDisplayName,OwnerUserId:int£¬ Title, Body, ViewCount:int
);
F = FOREACH E GENERATE OwnerUserId, Body;
G = union D, F, B;

GI = FILTER G BY Body MATCHES '.*java.*';


BI = GROUP GI BY OwnerUserId;
AI = GROUP BI BY $0;

LI = FOREACH AI GENERATE COUNT (BI);
NI = GROUP LI ALL;
I = FOREACH NI GENERATE SUM(LI.$0); 




DUMP I;
STORE I into '/user/root/out/task3';

