# ===============================================================
# CitiBike Trips Analyze App
# Snowflake上のCitiBikeデータを検索・分析できるStreamlitアプリ
# ===============================================================

# -----------------------------------------------
# 必要なライブラリのインポート
# -----------------------------------------------
import streamlit as st                           # Webアプリを作るためのライブラリ
from snowflake.snowpark.context import get_active_session  # Snowflakeとの接続情報を取得
import snowflake.snowpark.functions as F         # Snowparkの関数を使う (フィルタや集計など)

# -----------------------------------------------
# アプリのタイトルと説明文を表示
# -----------------------------------------------
st.title("CitiBike Trips Analyze App")   # アプリのタイトル
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
# TRIPSテーブルを取得（Snowflake上のデータ）
# -----------------------------------------------
df = session.table("CITIBIKE.PUBLIC.TRIPS")   # SnowflakeテーブルをSnowpark DataFrameとして読み込む

# -----------------------------------------------
# CSVダウンロード用の関数
# -----------------------------------------------
def convert_df(df):
    """
    DataFrameをCSV形式に変換してダウンロード可能な状態にする関数
    """
    return df.to_csv(index=False).encode('utf-8')

# -----------------------------------------------
# サイドバー（検索条件の入力欄）
# -----------------------------------------------
with st.sidebar:
    st.subheader("検索条件")

    # 性別の選択（0=不明, 1=男性, 2=女性）
    # DISTINCTで重複を除き、存在する性別のみ選択肢に表示
    gender_option = st.selectbox(
        "性別", 
        df.select(F.col("GENDER")).distinct()
    )

    # ユーザタイプの選択（例: Subscriber, Customer）
    # 複数選択できるようにしている
    user_type_option = st.pills(
        "ユーザタイプ", 
        df.select(F.col("USERTYPE")).distinct(), 
        selection_mode="multi"
    )

    # 日付範囲の選択
    # STARTTIME列の最小値・最大値を取得して、スライダーに反映
    stats = df.agg(
        F.min("STARTTIME").alias("min_date"),
        F.max("STARTTIME").alias("max_date")
    ).collect()[0]  # → collect()でPythonオブジェクトに変換

    start_date, end_date = st.slider(
        "利用開始日時",
        min_value=stats["MIN_DATE"],   # データの最小日付
        max_value=stats["MAX_DATE"],   # データの最大日付
        value=(stats["MIN_DATE"], stats["MAX_DATE"]),  # デフォルトは全期間
        format="YYYY-MM-DD"
    )

# -----------------------------------------------
# タブを作成（画面を切り替える）
# -----------------------------------------------
tab1, tab2, tab3 = st.tabs(["表", "グラフ", "C"])

# -----------------------------------------------
# Tab1: データを表形式で表示
# -----------------------------------------------
with tab1:
    # フィルタ条件を適用
    df = df.filter(F.col("GENDER") == gender_option)             # 性別フィルタ
    df = df.filter(F.col("USERTYPE").isin(user_type_option))     # ユーザタイプフィルタ
    df = df.filter(F.col("STARTTIME").between(start_date, end_date))  # 日付範囲フィルタ

    # データの一部を表示（最初の100行）
    st.dataframe(df.limit(100))

    # 該当件数を表示
    st.write("該当件数:", df.count())

    # CSVでダウンロードできるようにする
    # → Snowpark DataFrameをPandasに変換 → CSV化
    csv = convert_df(df.limit(100).to_pandas())
    st.download_button(
       "結果をCSVでダウンロード",
       csv,
       "citi_trips.csv",
       "text/csv",
       key='download-csv'
    )

# -----------------------------------------------
# Tab2: グラフ表示（利用回数を月ごとに集計）
# -----------------------------------------------
with tab2:
    # 年月カラムを作成し、利用回数を集計
    df_monthly = (
        df.with_column("YEAR_MONTH", F.to_char(F.col("STARTTIME"), "YYYY-MM"))
          .group_by("YEAR_MONTH")
          .count()
          .sort("YEAR_MONTH")
    )

    # 月ごとの利用件数を棒グラフに表示
    st.bar_chart(df_monthly, x='YEAR_MONTH', y='COUNT')

# -----------------------------------------------
# Tab3: 今後の拡張用（今は空）
# -----------------------------------------------
with tab3:
    st.write("")
