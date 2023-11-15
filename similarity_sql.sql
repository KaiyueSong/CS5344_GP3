{\rtf1\ansi\ansicpg1252\cocoartf2709
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fnil\fcharset0 Menlo-Regular;}
{\colortbl;\red255\green255\blue255;\red39\green78\blue204;\red255\green255\blue255;\red44\green55\blue61;
\red0\green0\blue0;\red42\green55\blue62;\red21\green129\blue62;\red107\green0\blue1;\red238\green57\blue24;
\red204\green0\blue78;}
{\*\expandedcolortbl;;\cssrgb\c20000\c40392\c83922;\cssrgb\c100000\c100000\c100000;\cssrgb\c22745\c27843\c30588;
\cssrgb\c0\c0\c0;\cssrgb\c21569\c27843\c30980;\cssrgb\c5098\c56471\c30980;\cssrgb\c50196\c0\c0;\cssrgb\c95686\c31765\c11765;
\cssrgb\c84706\c10588\c37647;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\fs24 \cf2 \cb3 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 with\cf4 \strokec4  \strokec5 non_fake\strokec4  \cf2 \strokec2 AS\cf4 \strokec4  \cb1 \
\pard\pardeftab720\partightenfactor0
\cf6 \cb3 \strokec6 (\cf2 \strokec2 SELECT\cf4 \strokec4  \cf6 \strokec6 *\cf4 \strokec4  \cf2 \strokec2 FROM\cf4 \strokec4  \cf7 \strokec7 `prd-msa-dataflow.ky_test.galandgkg`\cf4 \strokec4  \cb1 \
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 WHERE\cf4 \strokec4  \cb1 \
\pard\pardeftab720\partightenfactor0
\cf6 \cb3 \strokec6 (\cf2 \strokec2 NOT\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3     \cf6 \strokec6 (\cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 taxes\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf4 \cb1 \strokec4 \
\cb3    \cf6 \strokec6 +\cf4 \strokec4  \cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 unemployment\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf4 \cb1 \strokec4 \
\cb3    \cf6 \strokec6 +\cf4 \strokec4  \cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 economy\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf4 \cb1 \strokec4 \
\cb3    \cf6 \strokec6 +\cf4 \strokec4  \cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 international_relations\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf4 \cb1 \strokec4 \
\cb3    \cf6 \strokec6 +\cf4 \strokec4  \cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 border_issues\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf4 \cb1 \strokec4 \
\cb3    \cf6 \strokec6 +\cf4 \strokec4  \cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 health_care\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf4 \cb1 \strokec4 \
\cb3    \cf6 \strokec6 +\cf4 \strokec4  \cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 public_order\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf4 \cb1 \strokec4 \
\cb3    \cf6 \strokec6 +\cf4 \strokec4  \cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 civil_liberties\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf4 \cb1 \strokec4 \
\cb3    \cf6 \strokec6 +\cf4 \strokec4  \cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 environment\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf4 \cb1 \strokec4 \
\cb3    \cf6 \strokec6 +\cf4 \strokec4  \cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 education\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf4 \cb1 \strokec4 \
\cb3    \cf6 \strokec6 +\cf4 \strokec4  \cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 domestic_politics\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf4 \cb1 \strokec4 \
\cb3    \cf6 \strokec6 +\cf4 \strokec4  \cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 poverty\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf4 \cb1 \strokec4 \
\cb3    \cf6 \strokec6 +\cf4 \strokec4  \cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 disaster\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf4 \cb1 \strokec4 \
\cb3    \cf6 \strokec6 +\cf4 \strokec4  \cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 religion\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf4 \cb1 \strokec4 \
\cb3    \cf6 \strokec6 +\cf4 \strokec4  \cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 infrastructure\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf4 \cb1 \strokec4 \
\cb3    \cf6 \strokec6 +\cf4 \strokec4  \cf2 \strokec2 CASE\cf4 \strokec4  \cf2 \strokec2 WHEN\cf4 \strokec4  \cf8 \strokec8 media_internet\cf4 \strokec4  = \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 THEN\cf4 \strokec4  \cf9 \strokec9 1\cf4 \strokec4  \cf2 \strokec2 ELSE\cf4 \strokec4  \cf9 \strokec9 0\cf4 \strokec4  \cf2 \strokec2 END\cf6 \strokec6 )\cf4 \strokec4  = \cf9 \strokec9 1\cf6 \strokec6 )\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 AND\cf4 \strokec4  \cf8 \strokec8 category\cf4 \strokec4  = \cf7 \strokec7 'non-fake'\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 AND\cf4 \strokec4  \cf2 \strokec2 date\cf4 \strokec4  \cf6 \strokec6 <\cf4 \strokec4  \cf7 \strokec7 '2023-01-01'\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 AND\cf4 \strokec4  \cf2 \strokec2 date\cf4 \strokec4  \cf6 \strokec6 >=\cf4 \strokec4  \cf7 \strokec7 '2021-01-01'\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf6 \cb3 \strokec6 )\cf4 \strokec4 ,\cb1 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec5 fake\strokec4  \cf2 \strokec2 AS\cf4 \strokec4  \cf6 \strokec6 (\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3   \cf2 \strokec2 SELECT\cf4 \strokec4  \cf6 \strokec6 *\cf4 \cb1 \strokec4 \
\cb3   \cf2 \strokec2 FROM\cf4 \strokec4  \cf7 \strokec7 `prd-msa-dataflow.ky_test.galandgkg`\cf4 \strokec4  \cb1 \
\cb3   \cf2 \strokec2 WHERE\cf4 \strokec4  \cf8 \strokec8 category\cf4 \strokec4  = \cf7 \strokec7 'fake'\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf6 \cb3 \strokec6 )\cf4 \strokec4 ,\cb1 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec5 both\strokec4  \cf2 \strokec2 AS\cf4 \strokec4  \cf6 \strokec6 (\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3   \cf2 \strokec2 SELECT\cf4 \strokec4  \cf6 \strokec6 *\cf4 \strokec4  \cf2 \strokec2 FROM\cf4 \strokec4  \strokec5 non_fake\cb1 \strokec4 \
\cb3   \cf2 \strokec2 UNION\cf4 \strokec4  \cf2 \strokec2 ALL\cf4 \cb1 \strokec4 \
\cb3   \cf2 \strokec2 SELECT\cf4 \strokec4  \cf6 \strokec6 *\cf4 \strokec4  \cf2 \strokec2 FROM\cf4 \strokec4  \strokec5 fake\cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf6 \cb3 \strokec6 )\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 ,\cb1 \
\pard\pardeftab720\partightenfactor0
\cf4 \cb3 \strokec5 final\strokec4  \cf2 \strokec2 AS\cf4 \strokec4  \cf6 \strokec6 (\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 SELECT\cf4 \strokec4  \cf2 \strokec2 date\cf4 \strokec4 , \strokec5 title\strokec4 , \cf7 \strokec7 `desc`\cf4 \strokec4 , \strokec5 category\strokec4 , \strokec5 DocumentIdentifier\strokec4 , \cf2 \strokec2 length\cf6 \strokec6 (\cf7 \strokec7 `desc`\cf6 \strokec6 )\cf4 \strokec4  \cf2 \strokec2 AS\cf4 \strokec4  \strokec5 desc_len\strokec4 , \cf2 \strokec2 length\cf6 \strokec6 (\cf4 \strokec5 title\cf6 \strokec6 )\cf4 \strokec4  \cf2 \strokec2 AS\cf4 \strokec4  \strokec5 title_len\strokec4  \cf2 \strokec2 FROM\cf4 \strokec4  \strokec5 both\cb1 \strokec4 \
\cf2 \cb3 \strokec2 WHERE\cf4 \strokec4  \strokec5 DocumentIdentifier\strokec4  \cf2 \strokec2 NOT\cf4 \strokec4  \cf2 \strokec2 LIKE\cf4 \strokec4  \cf7 \strokec7 '%/'\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 ORDER\cf4 \strokec4  \cf2 \strokec2 BY\cf4 \strokec4  \cf2 \strokec2 date\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf6 \cb3 \strokec6 )\cf4 \cb1 \strokec4 \
\
\pard\pardeftab720\partightenfactor0
\cf10 \cb3 \strokec10 --SELECT title, `desc` ,desc_len, title_len\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 SELECT\cf4 \strokec4  \cf2 \strokec2 date\cf4 \strokec4 , \strokec5 title\strokec4 , \cf7 \strokec7 `desc`\cf4 \strokec4 , \strokec5 category\strokec4 , \strokec5 DocumentIdentifier\cb1 \strokec4 \
\cf2 \cb3 \strokec2 FROM\cf4 \strokec4  \strokec5 final\cb1 \strokec4 \
\cf2 \cb3 \strokec2 WHERE\cf4 \strokec4  \cb1 \
\pard\pardeftab720\partightenfactor0
\cf6 \cb3 \strokec6 (\cf4 \strokec5 desc_len\strokec4  \cf6 \strokec6 <=\cf4 \strokec4  \cf9 \strokec9 500\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf2 \cb3 \strokec2 AND\cf4 \strokec4  \strokec5 desc_len\strokec4  \cf6 \strokec6 >\cf4 \strokec4  \cf9 \strokec9 50\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 AND\cf4 \strokec4  \strokec5 title_len\cf6 \strokec6 >\cf4 \strokec4  \cf9 \strokec9 10\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 AND\cf4 \strokec4  \cf8 \strokec8 category\cf4 \strokec4  = \cf7 \strokec7 'non-fake'\cf6 \strokec6 )\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 OR\cf4 \strokec4  \cf8 \strokec8 category\cf4 \strokec4  = \cf7 \strokec7 'fake'\cf4 \cb1 \strokec4 \
\cf2 \cb3 \strokec2 ORDER\cf4 \strokec4  \cf2 \strokec2 BY\cf4 \strokec4  \strokec5 desc_len\strokec4  \cf2 \strokec2 DESC\cf4 \cb1 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf10 \cb3 \strokec10 --ORDER BY date\cf4 \cb1 \strokec4 \
}