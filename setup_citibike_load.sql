/*******************************************************************************
    1. 各種指定 / データベースの作成 / テーブルの作成
*******************************************************************************/

-- コマンドで操作する場合は以下を順に実行 
-- Tripデータを入れるDB-スキーマを用意

use role sysadmin;

/* 1-1. CITIBIKEデータベースを作成 */
create or replace database citibike; -- CitiBikeデータベースを作成

/* 1-2. 利用データベース, スキーマを指定*/
use database citibike;
use schema public; -- CitiBikeデータベースの下に,publicスキーマを作成

/* 1-3. 利用ウェアハウスを指定 */
use warehouse compute_wh;

/* 1-4. Tripsテーブルを作成 */
create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);



/*******************************************************************************
    2. データ読み込み
*******************************************************************************/

/* 2-1. 外部ステージ作成 */
create or replace stage citibike_trips
    url = 's3://snowflake-workshop-lab/japan/citibike-trips/';

--> 外部ステージにあるファイルの一覧を確認
list @citibike_trips;
 -- 年毎に複数ファイルに分割されて格納されている
 -- AWS S3上に置かれているデータは 377個のファイル(gz圧縮後 1.9GB), 6,150万行

 
/* 2-2. File Format作成 
 Snowflakeのテーブルに綺麗にロードするため */ 
create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';

    
/* 2-3. 外部ステージからデータロード */
copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;

--> Trips テーブルの内容確認 
select * from trips limit 10;


/*******************************************************************************
    11. Snowflake 環境のリセット
*******************************************************************************/

-- Accountadmin を使用して、今回作成した全てのオブジェクトを削除

-- use role accountadmin;

-- drop database if exists citibike;
-- ALTER ACCOUNT SET USE_CACHED_RESULT = false;
-- ALTER ACCOUNT SET USE_CACHED_RESULT = true;
