{\rtf1\ansi\ansicpg1252\cocoartf2709
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fnil\fcharset0 Menlo-Regular;}
{\colortbl;\red255\green255\blue255;\red39\green78\blue204;\red255\green255\blue255;\red44\green55\blue61;
\red0\green0\blue0;\red42\green55\blue62;\red21\green129\blue62;\red238\green57\blue24;\red107\green0\blue1;
}
{\*\expandedcolortbl;;\cssrgb\c20000\c40392\c83922;\cssrgb\c100000\c100000\c100000;\cssrgb\c22745\c27843\c30588;
\cssrgb\c0\c0\c0;\cssrgb\c21569\c27843\c30980;\cssrgb\c5098\c56471\c30980;\cssrgb\c95686\c31765\c11765;\cssrgb\c50196\c0\c0;
}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\fs24 \cf2 \cb3 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 WITH\cf4 \strokec4  \strokec5 fakenewswebsite\strokec4  \cf2 \strokec2 AS\cf4 \strokec4  \cf6 \strokec6 (\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 SELECT\cf4 \strokec4  \cf6 \strokec6 *\cf4 \strokec4 ,\cb1 \
\cf2 \cb3 \strokec2 IF\cf6 \strokec6 (\cf2 \strokec2 STRPOS\cf6 \strokec6 (\cf4 \strokec5 string_field_0\strokec4 , \cf7 \strokec7 '.'\cf6 \strokec6 )\cf4 \strokec4  \cf6 \strokec6 >\cf4 \strokec4  \cf8 \strokec8 0\cf4 \strokec4 ,\cb1 \
\cf2 \cb3 \strokec2 SUBSTR\cf6 \strokec6 (\cf4 \strokec5 string_field_0\strokec4 , \cf8 \strokec8 1\cf4 \strokec4 , \cf2 \strokec2 STRPOS\cf6 \strokec6 (\cf4 \strokec5 string_field_0\strokec4 , \cf7 \strokec7 '.'\cf6 \strokec6 )\cf4 \strokec4  \cf6 \strokec6 -\cf4 \strokec4  \cf8 \strokec8 1\cf6 \strokec6 )\cf4 \strokec4 ,\cb1 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec5 string_field_0\cf6 \strokec6 )\cf4 \strokec4  \cf2 \strokec2 AS\cf4 \strokec4  \strokec5 domain_left\cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 FROM\cf4 \strokec4  \cf7 \strokec7 `prd-msa-dataflow.ky_test.fakenewswebsite_clean`\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf6 \cb3 \strokec6 )\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 ,\cb1 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec5 nonfake\strokec4  \cf2 \strokec2 AS\cf4 \strokec4  \cf6 \strokec6 (\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 SELECT\cf4 \strokec4  \cf6 \strokec6 *\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 FROM\cf4 \strokec4  \cf7 \strokec7 `prd-msa-dataflow.ky_test.nonfakenewswebsite`\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf6 \cb3 \strokec6 )\cf4 \strokec4 ,\cb1 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec5 nonfake1\strokec4  \cf2 \strokec2 AS\cf4 \strokec4  \cf6 \strokec6 (\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 SELECT\cf4 \strokec4  \cf6 \strokec6 *\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 FROM\cf4 \strokec4  \cf7 \strokec7 `prd-msa-dataflow.ky_test.mbfconlinenewsmedia`\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf6 \cb3 \strokec6 )\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 ,\cb1 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec5 gal\strokec4  \cf2 \strokec2 AS\cf4 \strokec4  \cf6 \strokec6 (\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 SELECT\cf4 \strokec4  \cf6 \strokec6 *\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 FROM\cf4 \strokec4  \cf7 \strokec7 `gdelt-bq.gdeltv2.gal`\cf4 \strokec4  \strokec5 gal\cb1 \strokec4 \
\cf2 \cb3 \strokec2 WHERE\cf4 \strokec4  \cf2 \strokec2 TIMESTAMP_TRUNC\cf6 \strokec6 (\cf2 \strokec2 date\cf4 \strokec4 , \strokec5 DAY\cf6 \strokec6 )\cf4 \strokec4  \cf6 \strokec6 >=\cf4 \strokec4  \cf2 \strokec2 TIMESTAMP\cf6 \strokec6 (\cf7 \strokec7 "2020-01-01"\cf6 \strokec6 )\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 AND\cf4 \strokec4  \cf2 \strokec2 TIMESTAMP_TRUNC\cf6 \strokec6 (\cf2 \strokec2 date\cf4 \strokec4 , \strokec5 DAY\cf6 \strokec6 )\cf4 \strokec4  \cf6 \strokec6 <\cf4 \strokec4  \cf2 \strokec2 TIMESTAMP\cf6 \strokec6 (\cf7 \strokec7 "2023-01-01"\cf6 \strokec6 )\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 AND\cf4 \strokec4  \cf9 \strokec9 lang\cf4 \strokec4  = \cf7 \strokec7 'en'\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 AND\cf4 \strokec4  \cf2 \strokec2 length\cf6 \strokec6 (\cf4 \strokec5 gal\strokec4 .\cf2 \strokec2 desc\cf6 \strokec6 )\cf4 \strokec4  \cf6 \strokec6 >\cf4 \strokec4  \cf8 \strokec8 0\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf6 \cb3 \strokec6 )\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 ,\cb1 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec5 gal_final\strokec4  \cf2 \strokec2 AS\cf4 \strokec4  \cf6 \strokec6 (\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf6 \cb3 \strokec6 (\cf2 \strokec2 SELECT\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec5 gal\strokec4 .\cf2 \strokec2 date\cf4 \strokec4 ,\cb1 \
\cb3 \strokec5 gal\strokec4 .\strokec5 url\strokec4 ,\cb1 \
\cb3 \strokec5 gal\strokec4 .\strokec5 domain\strokec4 ,\cb1 \
\cb3 \strokec5 gal\strokec4 .\strokec5 title\strokec4 ,\cb1 \
\cb3 \strokec5 gal\strokec4 .\cf2 \strokec2 desc\cf4 \strokec4 ,\cb1 \
\cb3 \strokec5 non\strokec4 .\strokec5 string_field_0\strokec4 ,\cb1 \
\pard\pardeftab720\partightenfactor0
\cf7 \cb3 \strokec7 "non-fake"\cf4 \strokec4  \cf2 \strokec2 AS\cf4 \strokec4  \strokec5 category\cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 FROM\cf4 \strokec4  \strokec5 gal\cb1 \strokec4 \
\cf2 \cb3 \strokec2 INNER\cf4 \strokec4  \cf2 \strokec2 JOIN\cf4 \strokec4  \strokec5 nonfake1\strokec4  \strokec5 non\strokec4  \cf2 \strokec2 ON\cf4 \strokec4  \cf2 \strokec2 lower\cf6 \strokec6 (\cf4 \strokec5 non\strokec4 .\strokec5 string_field_0\cf6 \strokec6 )\cf4 \strokec4  = \cf2 \strokec2 lower\cf6 \strokec6 (\cf4 \strokec5 gal\strokec4 .\strokec5 domain\cf6 \strokec6 ))\cf4 \cb1 \strokec4 \
\
\
\cf2 \cb3 \strokec2 UNION\cf4 \strokec4  \cf2 \strokec2 ALL\cf4 \cb1 \strokec4 \
\
\
\pard\pardeftab720\partightenfactor0
\cf6 \cb3 \strokec6 (\cf2 \strokec2 SELECT\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec5 gal\strokec4 .\cf2 \strokec2 date\cf4 \strokec4 ,\cb1 \
\cb3 \strokec5 gal\strokec4 .\strokec5 url\strokec4 ,\cb1 \
\cb3 \strokec5 gal\strokec4 .\strokec5 domain\strokec4 ,\cb1 \
\cb3 \strokec5 gal\strokec4 .\strokec5 title\strokec4 ,\cb1 \
\cb3 \strokec5 gal\strokec4 .\cf2 \strokec2 desc\cf4 \strokec4 ,\cb1 \
\cb3 \strokec5 f\strokec4 .\strokec5 string_field_0\strokec4 ,\cb1 \
\pard\pardeftab720\partightenfactor0
\cf7 \cb3 \strokec7 "fake"\cf4 \strokec4  \cf2 \strokec2 AS\cf4 \strokec4  \strokec5 category\cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 FROM\cf4 \strokec4  \strokec5 gal\cb1 \strokec4 \
\cf2 \cb3 \strokec2 INNER\cf4 \strokec4  \cf2 \strokec2 JOIN\cf4 \strokec4  \strokec5 fakenewswebsite\strokec4  \strokec5 f\strokec4  \cf2 \strokec2 ON\cf4 \strokec4  \cf2 \strokec2 lower\cf6 \strokec6 (\cf4 \strokec5 gal\strokec4 .\strokec5 domain\cf6 \strokec6 )\cf4 \strokec4  = \cf2 \strokec2 lower\cf6 \strokec6 (\cf4 \strokec5 f\strokec4 .\strokec5 string_field_0\cf6 \strokec6 )\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 WHERE\cf4 \strokec4  \cf6 \strokec6 (\cf4 \strokec5 f\strokec4 .\cf9 \strokec9 string_field_1\cf4 \strokec4  = \cf7 \strokec7 'fake'\cf6 \strokec6 ))\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf6 \cb3 \strokec6 )\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 ,\cb1 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec5 gkgandgal\strokec4  \cf2 \strokec2 AS\cf4 \strokec4  \cf6 \strokec6 (\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 SELECT\cf4 \strokec4  \cf6 \strokec6 *\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 FROM\cf4 \strokec4  \strokec5 gal_final\cb1 \strokec4 \
\cf2 \cb3 \strokec2 INNER\cf4 \strokec4  \cf2 \strokec2 JOIN\cf4 \strokec4  \cf7 \strokec7 `prd-msa-dataflow.ky_test.16themegkg`\cf4 \strokec4  \strokec5 gkg\cb1 \strokec4 \
\cf2 \cb3 \strokec2 ON\cf4 \strokec4  \strokec5 gal_final\strokec4 .\cf9 \strokec9 url\cf4 \strokec4  = \strokec5 gkg\strokec4 .\strokec5 DocumentIdentifier\cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf6 \cb3 \strokec6 )\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 SELECT\cf4 \strokec4  \cf6 \strokec6 *\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 FROM\cf4 \strokec4  \strokec5 gkgandgal\cb1 \strokec4 \
}