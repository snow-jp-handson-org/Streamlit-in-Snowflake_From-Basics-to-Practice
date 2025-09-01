# ===============================================================
# CitiBike Trips Analyze App
# Snowflake上のCitiBikeデータを検索・分析できるStreamlitアプリ
# ===============================================================

# -----------------------------------------------
# 必要なライブラリのインポート
# -----------------------------------------------
import streamlit as st                           # Webアプリを作るためのライブラリ
from snowflake.snowpark.context import get_active_session  # Snowflake接続情報の取得
import snowflake.snowpark.functions as F         # Snowpark関数（集計・変換・フィルタなど）
import plotly.graph_objects as go               # Plotlyでグラフ作成
from plotly.subplots import make_subplots       # 複数グラフを並べるサブプロット

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
# CSVダウンロード用の関数
# -----------------------------------------------
def convert_df(df):
    """
    DataFrameをCSV形式に変換してダウンロード可能な状態にする関数
    """
    return df.to_csv(index=False).encode('utf-8')

# -----------------------------------------------
# TRIPSテーブルを取得（Snowflake上のデータ）
# -----------------------------------------------
df = session.table("CITIBIKE.PUBLIC.TRIPS")   # Snowpark DataFrameとして読み込む

# -----------------------------------------------
# サイドバー（検索条件の入力）
# -----------------------------------------------
with st.sidebar:
    st.subheader("検索条件")

    # 性別の選択（複数選択可能: 0=不明, 1=男性, 2=女性）
    # DISTINCTで重複を除き、実際に存在する値だけを選択肢に表示
    gender_option = st.multiselect(
        "性別", 
        df.select(F.col("GENDER")).distinct()
    )

    # ユーザタイプの選択（複数選択可能）
    user_type_option = st.pills(
        "ユーザタイプ", 
        df.select(F.col("USERTYPE")).distinct(), 
        selection_mode="multi"
    )

    # 日付範囲の選択
    # STARTTIME列の最小値・最大値を取得してスライダーに反映
    stats = df.agg(
        F.min("STARTTIME").alias("min_date"),
        F.max("STARTTIME").alias("max_date")
    ).collect()[0]  # collect()でPythonオブジェクトとして取得

    start_date, end_date = st.slider(
        "利用開始日時",
        min_value=stats["MIN_DATE"],   # データの最小日付
        max_value=stats["MAX_DATE"],   # データの最大日付
        value=(stats["MIN_DATE"], stats["MAX_DATE"]),  # デフォルトは全期間
        format="YYYY-MM-DD"
    )

# -----------------------------------------------
# タブを作成（画面を切り替え）
# -----------------------------------------------
tab1, tab2, tab3 = st.tabs(["表", "グラフ", "C"])

# -----------------------------------------------
# Tab1: データを表形式で表示
# -----------------------------------------------
with tab1:
    # サイドバーで選んだ条件でフィルタリング
    df = df.filter(F.col("GENDER").isin(gender_option))              # 性別
    df = df.filter(F.col("USERTYPE").isin(user_type_option))         # ユーザタイプ
    df = df.filter(F.col("STARTTIME").between(start_date, end_date)) # 日付範囲

    # データの一部を表示（最初の100行）
    st.dataframe(df.limit(100))

    # 該当件数を表示
    st.write("該当件数:", df.count())

    # データのCSVダウンロード
    csv = convert_df(df.limit(100).to_pandas())  # Snowpark→Pandas→CSV
    st.download_button(
       "結果をCSVでダウンロード",
       csv,
       "citi_trips.csv",
       "text/csv",
       key='download-csv'
    )

# -----------------------------------------------
# Tab2: グラフ表示
# -----------------------------------------------
with tab2:
    # 年月カラムを作成し、USERTYPEごとに月別件数を集計
    df_monthly = (
        df.with_column("YEAR_MONTH", F.to_char(F.col("STARTTIME"), "YYYY-MM"))
          .group_by("YEAR_MONTH", "USERTYPE")
          .count()
          .sort("YEAR_MONTH")
    )

    # Streamlitの棒グラフ（積み上げ棒グラフ）で表示
    st.bar_chart(df_monthly, x='YEAR_MONTH', y='COUNT', color="USERTYPE", stack=True)

    # -------------------------------------------
    # GENDERごとのUSERTYPE割合を円グラフで可視化
    # -------------------------------------------
    gender_dict = {0: "Unknown", 1: "Male", 2: "Female"}

    # GENDERとUSERTYPEの組み合わせで件数を集計
    df_usertype = df.group_by(F.col("GENDER"), F.col("USERTYPE")).count()

    # 集計結果をPython側に取り込み
    rows = df_usertype.collect()

    # GENDERごとにラベルと値をまとめる辞書
    gender_data = {g: {"labels": [], "values": []} for g in gender_dict.keys()}
    for row in rows:
        gender_code = row["GENDER"]
        usertype = row["USERTYPE"]
        cnt = row["COUNT"]
        gender_data[gender_code]["labels"].append(usertype)
        gender_data[gender_code]["values"].append(cnt)

    # サブプロットで複数円グラフを作成（1行に並べる）
    fig = make_subplots(
        rows=1, cols=len(gender_dict),
        specs=[[{"type": "domain"} for _ in gender_dict]]  # domain = 円グラフ
    )

    # GENDERごとに円グラフを追加
    for i, (gender_code, gender_label) in enumerate(gender_dict.items(), start=1):
        fig.add_trace(
            go.Pie(
                labels=gender_data[gender_code]["labels"],   # 円グラフのラベル
                values=gender_data[gender_code]["values"],   # 件数
                name=gender_label,                           # 中心に表示するラベル
                hole=0.4,                                    # ドーナツ型の穴
                hoverinfo="label+percent+name",              # ホバー情報
                textinfo="label+percent",                    # スライス内に表示
                textposition="inside",
                insidetextorientation="radial"
            ),
            1, i
        )

    # レイアウト調整（タイトル・凡例）
    fig.update_layout(
        title_text="性別ごとのユーザ種別割合",
        legend=dict(
            orientation="h",   # 横並び
            yanchor="bottom",
            y=-0.2,            # グラフ下
            xanchor="center",
            x=0.5
        )
    )

    # 各ドーナツの中心にGENDER名を表示
    for trace in fig.data:
        if trace.type == "pie":
            dom = trace.domain  # 描画領域の座標
            x_center = (dom["x"][0] + dom["x"][1]) / 2
            y_center = (dom["y"][0] + dom["y"][1]) / 2
            fig.add_annotation(
                x=x_center, y=y_center,
                xref="paper", yref="paper",
                text=trace.name,   # 中心に表示するラベル
                showarrow=False,
                font=dict(size=16),
                xanchor="center", yanchor="middle",
                align="center"
            )

    # Streamlitに円グラフを表示
    st.plotly_chart(fig, use_container_width=True)

# -----------------------------------------------
# Tab3: 空のコンテンツ（今後拡張用）
# -----------------------------------------------
with tab3:
    st.write("")
