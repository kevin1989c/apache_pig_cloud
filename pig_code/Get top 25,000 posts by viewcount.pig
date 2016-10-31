REGISTER /usr/local/pig/lib/piggybank.jar;
A = LOAD 'data.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE') AS (Id, PostTypeId, AcceptedAnswerId, ParentId, CreationDate, DeletionDate, Score:int, ViewCount, Body, OwnerUserId, OwnerDisplayName, LastEditorUserId, LastEditorDisplayName, LastEditDate, LastActivityDate, Title, Tags, AnswerCount, CommentCount, FavoriteCount, ClosedDate, CommunityOwnedDate);
B = FOREACH A GENERATE Score, Id, OwnerDisplayName,OwnerUserId£¬ Title, Body, ViewCount;
C = LOAD 'data1.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE') AS (Id, PostTypeId, AcceptedAnswerId, ParentId, CreationDate, DeletionDate, Score:int, ViewCount, Body, OwnerUserId, OwnerDisplayName, LastEditorUserId, LastEditorDisplayName, LastEditDate, LastActivityDate, Title, Tags, AnswerCount, CommentCount, FavoriteCount, ClosedDate, CommunityOwnedDate);
D = FOREACH A GENERATE Score, Id, OwnerDisplayName,OwnerUserId£¬ Title, Body, ViewCount;
E = LOAD 'data2.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE') AS (Id, PostTypeId, AcceptedAnswerId, ParentId, CreationDate, DeletionDate, Score:int, ViewCount, Body, OwnerUserId, OwnerDisplayName, LastEditorUserId, LastEditorDisplayName, LastEditDate, LastActivityDate, Title, Tags, AnswerCount, CommentCount, FavoriteCount, ClosedDate, CommunityOwnedDate);
F = FOREACH A GENERATE Score, Id, OwnerDisplayName,OwnerUserId£¬ Title, Body, ViewCount;
G = union D, F, B;

H = ORDER G BY ViewCount desc;
I = LIMIT H 250000;
DUMP I;
STORE I into '/user/root/out/task0';

