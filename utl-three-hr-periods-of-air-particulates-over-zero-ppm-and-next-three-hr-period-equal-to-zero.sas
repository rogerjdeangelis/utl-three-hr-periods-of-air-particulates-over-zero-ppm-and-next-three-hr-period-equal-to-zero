%let pgm=utl-three-hr-periods-of-air-particulates-over-zero-ppm-and-next-three-hr-period-equal-to-zero;

Identify three hr periods of air particulates over zero ppm and next three hr period equal to zero

  Solutions
      1 sas datastep
      2 r sql
        I don't think i would use sql for this however it does demonstate some advanced features
          a tmp table inside the sqldf queru
          b lag function in sqldf
      3 python sql

github
https://tinyurl.com/yvxhc26k
https://github.com/rogerjdeangelis/utl-three-hr-periods-of-air-particulates-over-zero-ppm-and-next-three-hr-period-equal-to-zero

SOAPBOX ON

As a side note there are ways to iterate observations with sqldf queries either
with sql arrays or looping over sql queries, however a non-sql language is probably more appropriate.
There is very little basic programming that you cannot do with SQL that youcan do with base r or python.

Pyhton and R have very useful packages and exotic datatypes and data structures for niche problems?
The enhanced R and python syntax associated with packages and applied to base sas problems is overwhelming
and impossible to commit to memory.

The exception is processing like sas procs: graph, stat and access where extended syntax is necessary.

SOAPBOX OFF

Stackoverflow (variation of this problem-you can use a logival (X>900) and (x<900)
https://tinyurl.com/53eu3amm
https://stackoverflow.com/questions/78946490/how-to-pull-first-and-last-date-from-a-dataframe-with-conditions-and-breaks


Related repos
https://github.com/rogerjdeangelis/utl-pivot-long-pivot-wide-transpose-partitioning-sql-arrays-wps-r-python
https://github.com/rogerjdeangelis/utl-pivot-transpose-by-id-using-wps-r-python-sql-using-partitioning
https://github.com/rogerjdeangelis/utl-transposing-rows-to-columns-using-proc-sql-partitioning
https://github.com/rogerjdeangelis/utl-transpose-pivot-wide-using-sql-partitioning-in-wps-r-python

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

/**************************************************************************************************************************/
/*                    |                                                       |                                           */
/*                    |                                                       |                                           */
/*       INPUT        |                 PROCESS                               |                  OUTPUT                   */
/*                    | SAS                                                   |                                           */
/*                    | ---                                                   |                                           */
/* OBS  VAL           | data want;                                            | WORK.WANT total obs=4                     */
/*                    |   retain flip 0;                                      |                                           */
/*  1    1            |                                                       | FLIP HRS    VAL1    VAL2    VAL3   TOTHRS */
/*  2    1            |   merge sd1.have(rename=val=val1)                     |                                           */
/*  3    0            |         sd1.have(rename=val=val2 firstobs=2)          |   1   12      1       1       1      3    */
/*  4    1            |         sd1.have(rename=val=val3 firstobs=3) end=dne; |   0   16      0       0       0      0    */
/*  5    0            |                                                       |   1   20      1       1       1      3    */
/*  6    0            |   tot= sum(of val1-val3) ;                            |   0   34      0       0       0      0    */
/*  7    1            |   if tot = 3 or tot=0;                                |                                           */
/*  8    0            |                                                       |                                           */
/*  9    0            |   if (tot=3 and flip=0)  then do;flip=1; output;end;  |                                           */
/* 10    1            |   if (tot=0 and flip=1)  then do;flip=0; output;end;  |                                           */
/* 11    1            |                                                       |                                           */
/* 12    1   3 hr =1  | run;quit;                                             |                                           */
/* 13    1            |                                                       |                                           */
/* 14    0            |---------------------------------------------------------------------------------------------------*/
/* 15    0            |                                                       |                                           */
/* 16    0            | R & PYTHON (same code   tmp table & lag               |                                           */
/* 17    0   3 hr =0  | ======================                                | SAS                                       */
/* 18    1            |                                                       |                                           */
/* 19    1            | with                                                  | ROWNAMES    VALUE    GRP                  */
/* 20    1   3 hr =1  |  tmp as (                                             |                                           */
/* 21    0            | select                                                |     1         12      3                   */
/* 22    1            |   value                                               |     2         16      0                   */
/* 23    1            |  ,grp                                                 |     3         20      3                   */
/* 24    0            |  ,lag(grp,1) over (order by value) as prev_grp_1      |     4         34      0                   */
/* 25    1            |  ,lag(value,1) over (order by value) as prev_value_1  |                                           */
/* 26    0            | from (                                                |                                           */
/* 27    0            |    select                                             |                                           */
/* 28    1            |      r.hrs as value                                   |                                           */
/* 29    1            |     ,(l.val+c.val+r.val) as grp                       |                                           */
/* 30    0            |    from                                               |                                           */
/* 31    1            |      have as l, have as c, have as r                  |                                           */
/* 32    0            |    where                                              |                                           */
/* 33    0            |           c.hrs = l.hrs+1                             |                                           */
/* 34    0  3 hr =0   |      and  r.hrs = l.hrs+2                             |                                           */
/*                    |      and (l.val+c.val+r.val) in (3,0)                 |                                           */
/*                    |    order                                              |                                           */
/*                    |      by l.hrs )                                       |                                           */
/*                    |    )                                                  |                                           */
/*                    |    select                                             |                                           */
/*                    |        value                                          |                                           */
/*                    |       ,grp                                            |                                           */
/*                    |    from                                               |                                           */
/*                    |       tmp                                             |                                           */
/*                    |    where                                              |                                           */
/*                    |          (grp <> prev_grp_1)=1                        |                                           */
/*                    |       or (grp <> prev_grp_1) is null                  |                                           */
/*                    |    order by value                                     |                                           */                                                               |                                           */
/*                    |                                                       |                                           */
/**************************************************************************************************************************/

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/
option s validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
do hrs=1 to 34;
  if uniform(4376)<.5 then val=1;
  else val=0;
  output;
end;
run;quit;

 /**************************************************************************************************************************/
 /*                                                                                                                        */
 /* Up to 40 obs from last table SD1.HAVE                                                                                  */
 /*                                                                                                                        */
 /* Obs    HRS    VAL   Obs    HRS    VA                                                                                   */
 /*                                                                                                                        */
 /*   1      1     1     18     18     1                                                                                   */
 /*   2      2     1     19     19     1                                                                                   */
 /*   3      3     0     20     20     1                                                                                   */
 /*   4      4     1     21     21     0                                                                                   */
 /*   5      5     0     22     22     1                                                                                   */
 /*   6      6     0     23     23     1                                                                                   */
 /*   7      7     1     24     24     0                                                                                   */
 /*   8      8     0     25     25     1                                                                                   */
 /*   9      9     0     26     26     0                                                                                   */
 /*  10     10     1     27     27     0                                                                                   */
 /*  11     11     1     28     28     1                                                                                   */
 /*  12     12     1     29     29     1                                                                                   */
 /*  13     13     1     30     30     0                                                                                   */
 /*  14     14     0     31     31     1                                                                                   */
 /*  15     15     0     32     32     0                                                                                   */
 /*  16     16     0     33     33     0                                                                                   */
 /*  17     17     0     34     34     0                                                                                   */
 /*                                                                                                                        */
 /**************************************************************************************************************************/

/*                       _       _            _
/ |  ___  __ _ ___    __| | __ _| |_ __ _ ___| |_ ___ _ __
| | / __|/ _` / __|  / _` |/ _` | __/ _` / __| __/ _ \ `_ \
| | \__ \ (_| \__ \ | (_| | (_| | || (_| \__ \ ||  __/ |_) |
|_| |___/\__,_|___/  \__,_|\__,_|\__\__,_|___/\__\___| .__/
                                                     |_|
*/
data want;
  retain flip 0;

  merge sd1.have(rename=val=val1)
        sd1.have(rename=val=val2 firstobs=2)
        sd1.have(rename=val=val3 firstobs=3) end=dne;

  totHrs= sum(of val1-val3) ;
  if totHrs = 3 or tothrs=0;

  if (tothrs=3 and flip=0)  then do;flip=1; output;end;
  if (tothrs=0 and flip=1)  then do;flip=0; output;end;

  keep flip hrs tothrs;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* WANT total obs=4                                                                                                       */
/*                                                                                                                        */
/* Obs    FLIP    HRS    TOTHRS                                                                                           */
/*                                                                                                                        */
/*  1       1      12       3                                                                                             */
/*  2       0      16       0                                                                                             */
/*  3       1      20       3                                                                                             */
/*  4       0      34       0                                                                                             */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                     _
|___ \   _ __   ___  __ _| |
  __) | | `__| / __|/ _` | |
 / __/  | |    \__ \ (_| | |
|_____| |_|    |___/\__, |_|
                       |_|
*/

%utl_rbeginx;
parmcards4;
library(haven)
library(sqldf)
source("c:/oto/fn_tosas9x.R")
have<-read_sas("d:/sd1/have.sas7bdat")
print(have)
result <- sqldf("
 with
  tmp as (
  select
    value
   ,grp
   ,lag(grp,1) over (order by value) as prev_grp_1
   ,lag(value,1) over (order by value) as prev_value_1
  from (
     select
       r.hrs as value
      ,(l.val+c.val+r.val) as grp
     from
       have as l, have as c, have as r
     where
            c.hrs = l.hrs+1
       and  r.hrs = l.hrs+2
       and (l.val+c.val+r.val) in (3,0)
     order
       by l.hrs )
     )
     select
         value
        ,grp
     from
        tmp
     where
           (grp <> prev_grp_1)=1
        or (grp <> prev_grp_1) is null
     order by value
")
result
fn_tosas9x(
      inp    = result
     ,outlib ="d:/sd1/"
     ,outdsn ="rwant"
     );
;;;;
%utl_rendx;

proc print data=sd1.rwant;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* R                                                                                                                      */
/* =                                                                                                                      */
/*  > result                                                                                                              */
/*    value grp                                                                                                           */
/*  1    12   3                                                                                                           */
/*  2    16   0                                                                                                           */
/*  3    20   3                                                                                                           */
/*  4    34   0                                                                                                           */
/*                                                                                                                        */
/* SAS                                                                                                                    */
/* ===                                                                                                                    */
/*  ROWNAMES    VALUE    GRP                                                                                              */
/*                                                                                                                        */
/*      1         12      3                                                                                               */
/*      2         16      0                                                                                               */
/*      3         20      3                                                                                               */
/*      4         34      0                                                                                               */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____               _   _                             _
|___ /   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
  |_ \  | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
 ___) | | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
|____/  | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
        |_|    |___/                                |_|
*/

%utl_pybeginx;
parmcards4;
import pyperclip
import os
from os import path
import sys
import subprocess
import time
import pandas as pd
import pyreadstat as ps
import numpy as np
import pandas as pd
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
have, meta = ps.read_sas7bdat("d:/sd1/have.sas7bdat")
exec(open('c:/temp/fn_tosas9.py').read())
print(have);
want = pdsql("""with tmp as (
  select
    value
   ,grp
   ,lag(grp,1) over (order by value) as prev_grp_1
   ,lag(value,1) over (order by value) as prev_value_1
  from (
     select
       r.hrs as value
      ,(l.val+c.val+r.val) as grp
     from
       have as l, have as c, have as r
     where
            c.hrs = l.hrs+1
       and  r.hrs = l.hrs+2
       and (l.val+c.val+r.val) in (3,0)
     order
       by l.hrs )
     )
     select
         value
        ,grp
     from
        tmp
     where
           (grp <> prev_grp_1)=1
        or (grp <> prev_grp_1) is null
     order by value
""")
print(want)
fn_tosas9(
   want
   ,dfstr="want"
   ,timeest=3
   )
;;;;
%utl_pyendx;

libname tmp "c:/temp";
proc print data=tmp.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  Python                                                                                                                */
/*                                                                                                                        */
/*     value  grp                                                                                                         */
/*  0   12.0  3.0                                                                                                         */
/*  1   16.0  0.0                                                                                                         */
/*  2   20.0  3.0                                                                                                         */
/*  3   34.0  0.0                                                                                                         */
/*                                                                                                                        */
/*  SAS                                                                                                                   */
/*                                                                                                                        */
/*     value  grp                                                                                                         */
/*  0   12.0  3.0                                                                                                         */
/*  1   16.0  0.0                                                                                                         */
/*  2   20.0  3.0                                                                                                         */
/*  3   34.0  0.0                                                                                                         */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/

