# -----------------------------------------------
# 必要なライブラリのインポート
# -----------------------------------------------
import streamlit as st                           # Webアプリを作るためのライブラリ
from snowflake.snowpark.context import get_active_session  # Snowflakeとの接続情報を取得するため
import snowflake.snowpark.functions as F         # Snowparkの関数を使うため (フィルタや集計など)

# -----------------------------------------------
# アプリのタイトルと説明文を表示
# -----------------------------------------------
st.title("CitiBike Trips Analyze App")   # アプリのタイトルを表示
st.write(
    """
    これはサンプルのアプリです  
    Snowflakeに保存されているCitiBikeデータを検索・表示できます。
    """
)

# -----------------------------------------------
# Snowflakeとの接続を確立
# -----------------------------------------------
session = get_active_session()   # 現在のSnowflakeセッションを取得

# -----------------------------------------------
# CSVダウンロード用の関数を定義
# -----------------------------------------------
def convert_df(df):
    """
    DataFrameをCSV形式に変換してダウンロード可能な状態にする関数
    """
    return df.to_csv(index=False).encode('utf-8')

# -----------------------------------------------
# TRIPSテーブルを取得（Snowflake上のデータ）
# -----------------------------------------------
df = session.table("CITIBIKE.PUBLIC.TRIPS")

# -----------------------------------------------
# サイドバー（検索条件を入力するエリア）
# -----------------------------------------------
with st.sidebar:
    st.subheader("検索条件")

    # -------------------------------------------
    # 性別の選択（0=不明, 1=男性, 2=女性）
    # DISTINCTで重複を取り除き、選択肢を自動取得
    # -------------------------------------------
    gender_option = st.selectbox(
        "性別", 
        df.select(F.col("GENDER")).distinct()
    )

    # -------------------------------------------
    # ユーザタイプの選択
    # 複数選択可能（例: Subscriber, Customer）
    # -------------------------------------------
    user_type_option = st.pills(
        "ユーザタイプ", 
        df.select(F.col("USERTYPE")).distinct(), 
        selection_mode="multi"
    )

    # -------------------------------------------
    # 日付範囲の選択
    # STARTTIMEカラムの最小値・最大値を取得してスライダーで指定
    # -------------------------------------------
    stats = df.agg(
        F.min("STARTTIME").alias("min_date"),
        F.max("STARTTIME").alias("max_date")
    ).collect()[0]  # → collect() でPythonに持ってくる

    start_date, end_date = st.slider(
        "利用開始日時",
        min_value=stats["MIN_DATE"],   # データの最小日付
        max_value=stats["MAX_DATE"],   # データの最大日付
        value=(stats["MIN_DATE"], stats["MAX_DATE"]),  # デフォルトは全期間
        format="YYYY-MM-DD"
    )

# -----------------------------------------------
# タブを作成（画面を切り替えられるようにする）
# -----------------------------------------------
tab1, tab2, tab3 = st.tabs(["表", "B", "C"])

# -----------------------------------------------
# Tab1: データの表示
# -----------------------------------------------
with tab1:
    # サイドバーで選んだ条件でフィルタリング
    df = df.filter(F.col("GENDER") == gender_option)             # 性別
    df = df.filter(F.col("USERTYPE").isin(user_type_option))     # ユーザタイプ
    df = df.filter(F.col("STARTTIME").between(start_date, end_date))  # 日付範囲

    # データの一部を表示（最初の100行）
    st.dataframe(df.limit(100))

    # 該当件数を表示
    st.write("該当件数:", df.count())

    # -------------------------------------------
    # データのダウンロード機能
    # Snowpark DataFrame → Pandas DataFrame → CSV形式
    # -------------------------------------------
    csv = convert_df(df.limit(100).to_pandas())   # 表示している100件をCSV化
    st.download_button(
       "結果をCSVでダウンロード",
       csv,
       "citi_trips.csv",
       "text/csv",
       key='download-csv'
    )

# -----------------------------------------------
# Tab2: 空のコンテンツ（今後拡張用）
# -----------------------------------------------
with tab2:
    st.write("")

# -----------------------------------------------
# Tab3: 空のコンテンツ（今後拡張用）
# -----------------------------------------------
with tab3:
    st.write("")
