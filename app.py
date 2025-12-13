import streamlit as st
import pandas as pd
import numpy as np
import io
import requests
import json  # JSONを扱うためにimport
from datetime import datetime, date, timedelta
import pytz
import plotly.graph_objects as go
import plotly.express as px
import time
from bs4 import BeautifulSoup


# ページ設定
st.set_page_config(
    page_title="SHOWROOM ライバーKPI分析ツール",
    page_icon="📊",
    layout="wide"
)

# タイトル
st.markdown(
    "<h1 style='font-size:28px; text-align:center; color:#1f2937;'>SHOWROOM ライバーKPI分析ツール</h1>",
    unsafe_allow_html=True
)

# 説明文
st.markdown(
    "<p style='font-size:16px; text-align:center; color:#4b5563;'>"
#    "分析方法を指定して、配信のパフォーマンスを分析します。"
    ""
    "</p>",
    unsafe_allow_html=True
)

st.markdown("---")


# --- 関数定義 ---
#@st.cache_data(ttl=60) # キャッシュ保持を60秒に変更
def fetch_event_data():
    """イベントデータをCSVから読み込み、キャッシュする"""
    try:
        #event_url = "https://mksoul-pro.com/showroom/file/sr-event-entry.csv"
        event_url = "https://mksoul-pro.com/showroom/file/event_database.csv"
        event_df = pd.read_csv(event_url, dtype={'アカウントID': str})
        event_df['開始日時'] = pd.to_datetime(event_df['開始日時'], errors='coerce')
        event_df['終了日時'] = pd.to_datetime(event_df['終了日時'], errors='coerce')
        event_df_filtered = event_df[(event_df['紐付け'] == '○') & event_df['開始日時'].notna() & event_df['終了日時'].notna()].copy()
        event_df_filtered = event_df_filtered.sort_values(by='開始日時', ascending=True)
        return event_df_filtered
    except Exception as e:
        st.warning(f"イベント情報の取得に失敗しました: {e}")
        return pd.DataFrame()

# ★ 新しく追加した認証チェック関数
@st.cache_data(ttl=3600)
def check_authentication(account_id_to_check):
    """アカウントIDが認証されているかチェックする"""
    if account_id_to_check == "mksp":
        return True
    
    ROOM_LIST_URL = "https://mksoul-pro.com/showroom/file/room_list.csv"
    try:
        # ヘッダーなしでD列(インデックス3)のみを文字列として読み込む
        df = pd.read_csv(ROOM_LIST_URL, header=None, usecols=[3], dtype={3: str}, encoding='utf-8-sig')
        # 欠損値を除外してリスト化
        authenticated_ids = df[3].dropna().tolist()
        return account_id_to_check in authenticated_ids
    except Exception as e:
        st.warning(f"認証情報の取得に失敗しました。: {e}")
        return False # 認証に失敗した場合は処理を続行させない

# ★ 新しい関数: ルーム名をAPIから取得
#@st.cache_data(ttl=3600)
def fetch_room_name(room_id):
    """SHOWROOM APIから最新のルーム名を取得する"""
    if not room_id:
        return "ルーム名不明"
    
    url = f"https://www.showroom-live.com/api/room/profile?room_id={room_id}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()  # HTTPエラーをチェック
        data = response.json()
        return data.get("room_name", "ルーム名不明")
    except requests.exceptions.RequestException as e:
        st.error(f"⚠️ ルーム名の取得に失敗しました: {e}")
        return "ルーム名不明"
    except json.JSONDecodeError:
        st.error("⚠️ ルーム名の取得APIから無効な応答が返されました。")
        return "ルーム名不明"
    except Exception as e:
        st.error(f"⚠️ ルーム名取得中に予期せぬエラーが発生しました: {e}")
        return "ルーム名不明"

def clear_analysis_results():
    """分析結果の表示状態をリセットするコールバック関数"""
    if 'run_analysis' in st.session_state:
        st.session_state.run_analysis = False

# --- UI入力セクション ---
# ⑤ アカウントIDをパスワード形式で入力
account_id = st.text_input(
    "アカウントID（全体平均等は所定のIDを入力）:",
    "",
    type="password",
    key="account_id_input",  # 新しくkeyを追加
    on_change=clear_analysis_results # on_changeイベントを追加
)

# ① 分析方法の選択時に分析結果をクリア
analysis_type = st.radio(
    "分析方法を選択:",
    ('期間で指定', 'イベントで指定'),
    horizontal=True,
    key='analysis_type_selector',
    on_change=clear_analysis_results
)

# 日本時間（JST）を明示的に指定
JST = pytz.timezone('Asia/Tokyo')
today = datetime.now(JST).date()

# UI要素の状態を保持する変数を初期化
selected_date_range_val = None
selected_event_val = None

# 条件に応じた入力ウィジェットの表示
if analysis_type == '期間で指定':
    default_end_date = today - timedelta(days=1)
    default_start_date = default_end_date - timedelta(days=30)
    selected_date_range_val = st.date_input(
        "分析期間:",
        (default_start_date, default_end_date),
        min_value=date(2023, 9, 1), 
        max_value=today,
        on_change=clear_analysis_results
    )
else:  # 'イベントで指定'
    if account_id:
        # ★★★ ここで認証チェックを先に行う ★★★
        if not check_authentication(account_id):
            st.error(f"指定されたアカウントID（{account_id}）は認証されていません。")
        else:
            # 認証成功時のみイベント取得処理を実行
            event_df = fetch_event_data()
            if not event_df.empty:
                #user_events = event_df[event_df['アカウントID'] == account_id].sort_values('開始日時', ascending=False)
                user_events = event_df[(event_df['アカウントID'] == account_id) & (event_df['開始日時'] >= '2023-09-01')].sort_values('開始日時', ascending=False)
                if not user_events.empty:
                    event_names = user_events['イベント名'].unique().tolist()
                    if event_names:
                        # イベント変更時に分析結果をクリアするコールバックを追加
                        selected_event_val = st.selectbox(
                            "分析するイベントを選択:", 
                            options=event_names,
                            on_change=clear_analysis_results
                        )
                        
                        event_details_to_link = user_events[user_events['イベント名'] == selected_event_val]
                        if not event_details_to_link.empty:
                            start_time = event_details_to_link.iloc[0]['開始日時']
                            end_time = event_details_to_link.iloc[0]['終了日時']
                            
                            # 💡 【ここを置き換え】順位・ポイント・レベル表示部
                            if pd.notna(start_time) and pd.notna(end_time):
                                start_time_str = start_time.strftime('%Y/%m/%d %H:%M')
                                end_time_str = end_time.strftime('%Y/%m/%d %H:%M')
                                st.markdown(f"**イベント期間：{start_time_str} - {end_time_str}**", unsafe_allow_html=True)

                            # ==============================================
                            # ✅ 新ロジック: 終了日が未来ならAPIで取得、それ以外はCSV
                            # ==============================================
                            use_api = False
                            try:
                                JST = pytz.timezone("Asia/Tokyo")
                                now_jst = datetime.now(JST)
                                event_end_jst = end_time if end_time.tzinfo else JST.localize(end_time)
                                if event_end_jst > now_jst:
                                    use_api = True
                            except Exception as e:
                                st.warning(f"⚠️ イベント終了日時の判定に失敗しました ({e})")
                                use_api = False

                            event_rank = event_point = event_level = "N/A"

                            if use_api:
                                try:
                                    #st.caption("※開催中イベントのため、最新順位をAPIから取得しています。")
                                    api_url_base = "https://www.showroom-live.com/api/event/room_list"
                                    all_rooms = []
                                    # 🔁 ページを全取得（最大50ページ程度まで安全上限）
                                    for page in range(1, 60):
                                        api_url = f"{api_url_base}?event_id={event_details_to_link.iloc[0]['event_id']}&p={page}"
                                        resp = requests.get(api_url, timeout=5)
                                        if resp.status_code != 200:
                                            break
                                        data = resp.json()
                                        rooms = data.get("list") or data.get("room_list") or []
                                        if not rooms:
                                            break
                                        all_rooms.extend(rooms)
                                        if len(rooms) < 30:
                                            break

                                    # 🎯 該当room_idを抽出（アカウントID一致からCSVでroom_idを取得）
                                    target_room_id = str(event_details_to_link.iloc[0]["ルームID"]) if "ルームID" in event_details_to_link.columns else None
                                    matched = next((r for r in all_rooms if str(r.get("room_id")) == str(target_room_id)), None)

                                    if matched:
                                        event_rank = matched.get("rank", "-")
                                        event_point = matched.get("point", 0)
                                        # event_entry内のquest_levelを安全に取得
                                        ev = matched.get("event_entry") or {}
                                        event_level = ev.get("quest_level", "")
                                    else:
                                        st.warning("⚠️ 対象ルームがAPI結果に見つかりません。CSVデータを使用します。")
                                        use_api = False

                                except Exception as e:
                                    st.warning(f"⚠️ API取得失敗のため、CSVデータを使用します。詳細: {e}")
                                    use_api = False

                            # 🟡 API未使用または失敗時はCSVの値を利用
                            if not use_api:
                                event_rank = event_details_to_link.iloc[0].get("順位", "N/A")
                                event_point = event_details_to_link.iloc[0].get("ポイント", "N/A")
                                event_level = event_details_to_link.iloc[0].get("レベル", "N/A")

                            # ✅ ポイントのカンマ区切り処理
                            try:
                                event_point_display = f"{int(event_point):,}"
                            except Exception:
                                event_point_display = str(event_point)

                            # ✅ 表示部分（既存デザイン踏襲）
                            st.markdown(f"**順位：{event_rank} / ポイント：{event_point_display} / レベル：{event_level}**", unsafe_allow_html=True)
                            # ==============================================


                            # 以前の修正: イベントURLへのリンクを追加
                            if 'URL' in event_details_to_link.columns:
                                event_url = event_details_to_link.iloc[0]['URL']
                            else:
                                event_url = None
                            
                            if pd.notna(event_url) and event_url != '':
                                st.markdown(f"**▶ [イベントページへ移動する]({event_url})**", unsafe_allow_html=True)
                    
                    else:
                        st.info("このアカウントIDに紐づくイベントはありません。")
                else:
                    st.info("このアカウントIDに紐づくイベントデータが見つかりませんでした。")
            else:
                st.warning("イベントデータを取得できませんでした。")
    else:
        st.info("先にアカウントIDを入力してください。")

    # ★ 修正箇所: 注意書きをif elseブロックの外に移動
    st.caption("※分析したい参加イベントが紐づいていない（見つからない）場合は運営にご照会ください。")


# ボタンの前に余白を追加
st.markdown("<div style='margin-top:20px;'></div>", unsafe_allow_html=True)


# データの読み込みと前処理関数
# @st.cache_data(ttl=3600) # データのキャッシュを1時間保持
def load_and_preprocess_data(account_id, start_date, end_date):
    if not account_id:
        st.error("アカウントIDを入力してください。")
        return None, None, None, None

    if start_date > end_date:
        st.error("開始日は終了日より前の日付を選択してください。")
        return None, None, None, None

    loop_start_date = start_date.date() if isinstance(start_date, (datetime, pd.Timestamp)) else start_date
    loop_end_date = end_date.date() if isinstance(end_date, (datetime, pd.Timestamp)) else end_date

    all_dfs = []
    
    # 読み込み対象の月をリストアップ
    target_months = []
    current_date_loop = loop_start_date
    while current_date_loop <= loop_end_date:
        target_months.append(current_date_loop)
        if current_date_loop.month == 12:
            current_date_loop = date(current_date_loop.year + 1, 1, 1)
        else:
            current_date_loop = date(current_date_loop.year, current_date_loop.month + 1, 1)
    
    total_months = len(target_months)

    # プログレスバーを一本で実装
    progress_bar = st.progress(0)
    progress_text = st.empty()
    
    # 2回目の読み込み
    is_mksp = account_id == "mksp"
    mksp_df_temp = pd.DataFrame()
    df_temp = pd.DataFrame()
    room_id_temp = None

    total_steps = 2 * total_months if not is_mksp else total_months

    # 1回目：全体データ(mksp)の読み込み
    for i, current_date in enumerate(target_months):
        year = current_date.year
        month = current_date.month
        progress = (i + 1) / total_steps
        progress_bar.progress(progress)
        progress_text.text(f"📊 全体データ ({year}年{month}月) を取得中... ({i+1}/{total_months})")
        
        url = f"https://mksoul-pro.com/showroom/csv/{year:04d}-{month:02d}_all_all.csv"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            csv_data = io.StringIO(response.content.decode('utf-8-sig'))
            df = pd.read_csv(csv_data, on_bad_lines='skip')
            df.columns = df.columns.str.strip().str.replace('"', '')
            all_dfs.append(df)
        
        except requests.exceptions.RequestException as e:
            if e.response and e.response.status_code == 404:
                # st.warning(f"⚠️ {year}年{month}月のデータが見つかりませんでした。スキップします。")
                pass
            else:
                st.error(f"❌ データの取得中に予期せぬエラーが発生しました: {e}")
                progress_bar.empty()
                progress_text.empty()
                return None, None
        except Exception as e:
            st.error(f"❌ CSVファイルの処理中に予期せぬエラーが発生しました。詳細: {e}")
            progress_bar.empty()
            progress_text.empty()
            return None, None
            
    if not all_dfs:
        st.error(f"選択された期間のデータが一つも見つかりませんでした。")
        progress_bar.empty()
        progress_text.empty()
        return None, None, None, None

    combined_df = pd.concat(all_dfs, ignore_index=True)
    if "配信日時" not in combined_df.columns:
        raise KeyError("CSVファイルに '配信日時' 列が見つかりませんでした。")
    combined_df["配信日時"] = pd.to_datetime(combined_df["配信日時"])

    mksp_df_temp = combined_df.copy()

    # 2回目：個別アカウントIDの読み込み（mkspではない場合のみ）
    if not is_mksp:
        individual_dfs = []
        for i, current_date in enumerate(target_months):
            year = current_date.year
            month = current_date.month
            progress = (total_months + i + 1) / total_steps
            progress_bar.progress(progress)
            progress_text.text(f"👤 個人データ ({year}年{month}月) を取得中... ({i+1}/{total_months})")
            
            url = f"https://mksoul-pro.com/showroom/csv/{year:04d}-{month:02d}_all_all.csv"
            
            try:
                response = requests.get(url)
                response.raise_for_status()
                csv_data = io.StringIO(response.content.decode('utf-8-sig'))
                df = pd.read_csv(csv_data, on_bad_lines='skip')
                df.columns = df.columns.str.strip().str.replace('"', '')
                individual_dfs.append(df)
            
            except requests.exceptions.RequestException as e:
                if e.response and e.response.status_code == 404:
                    # st.warning(f"⚠️ {year}年{month}月のデータが見つかりませんでした。スキップします。")
                    pass
                else:
                    st.error(f"❌ データの取得中に予期せぬエラーが発生しました: {e}")
                    progress_bar.empty()
                    progress_text.empty()
                    return None, None, None, None
            except Exception as e:
                st.error(f"❌ CSVファイルの処理中に予期せぬエラーが発生しました。詳細: {e}")
                progress_bar.empty()
                progress_text.empty()
                return None, None, None, None

        if not individual_dfs:
            st.warning(f"指定されたアカウントID（{account_id}）のデータが選択された期間に見つかりませんでした。")
            progress_bar.empty()
            progress_text.empty()
            return None, None, None, None
            
        individual_combined_df = pd.concat(individual_dfs, ignore_index=True)
        if "配信日時" not in individual_combined_df.columns:
            raise KeyError("CSVファイルに '配信日時' 列が見つかりませんでした。")
        individual_combined_df["配信日時"] = pd.to_datetime(individual_combined_df["配信日時"])

        filtered_by_account_df = individual_combined_df[individual_combined_df["アカウントID"] == account_id].copy()

        if filtered_by_account_df.empty:
            st.warning(f"指定されたアカウントID（{account_id}）の配信データが選択された期間に見つかりませんでした。")
            progress_bar.empty()
            progress_text.empty()
            return None, None, None, None
        
        if isinstance(start_date, (datetime, pd.Timestamp)):
            filtered_df = filtered_by_account_df[
                (filtered_by_account_df["配信日時"] >= start_date) & 
                (filtered_by_account_df["配信日時"] <= end_date)
            ].copy()
        else:
            filtered_df = filtered_by_account_df[
                (filtered_by_account_df["配信日時"].dt.date >= start_date) & 
                (filtered_by_account_df["配信日時"].dt.date <= end_date)
            ].copy()
        
        df_temp = filtered_df.copy()
        if "ルームID" in df_temp.columns and not df_temp.empty:
            room_id_temp = df_temp["ルームID"].iloc[0]
            # ルーム名を取得して一時変数に格納
            room_name_temp = fetch_room_name(room_id_temp)
        else:
            room_name_temp = "ルーム名不明"


    # mkspの場合は、mksp_df_tempをそのままdf_tempとして扱う
    else:
        filtered_by_account_df = combined_df.copy()
        if isinstance(start_date, (datetime, pd.Timestamp)):
            filtered_df = filtered_by_account_df[
                (filtered_by_account_df["配信日時"] >= start_date) & 
                (filtered_by_account_df["配信日時"] <= end_date)
            ].copy()
        else:
            filtered_df = filtered_by_account_df[
                (filtered_by_account_df["配信日時"].dt.date >= start_date) & 
                (filtered_by_account_df["配信日時"].dt.date <= end_date)
            ].copy()
        
        df_temp = filtered_df.copy()
        room_id_temp = None
        room_name_temp = "ルーム名不明" # mkspの場合はルーム名を取得しない

    # 数値型に変換する共通処理
    def convert_to_numeric(df):
        if df is None or df.empty:
            return df
        numeric_cols = [
            "合計視聴数", "視聴会員数", "フォロワー数", "獲得支援point", "コメント数",
            "ギフト数", "期限あり/期限なしSG総額", "コメント人数", "初コメント人数",
            "ギフト人数", "初ギフト人数", "フォロワー増減数", "初ルーム来訪者数", "配信時間(分)", "短時間滞在者数",
            "期限あり/期限なしSGのギフティング数", "期限あり/期限なしSGのギフティング人数"
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", "").replace("-", "0"),
                    errors='coerce'
                ).fillna(0)
        return df

    mksp_df_temp = convert_to_numeric(mksp_df_temp)
    df_temp = convert_to_numeric(df_temp)

    # 最終的なプログレスバーとテキストを非表示にする
    progress_bar.empty()
    progress_text.empty()
    
    return mksp_df_temp, df_temp, room_id_temp, room_name_temp

def categorize_time_of_day_with_range(hour):
    if 3 <= hour < 6: return "早朝 (3-6時)"
    elif 6 <= hour < 9: return "朝 (6-9時)"
    elif 9 <= hour < 12: return "午前 (9-12時)"
    elif 12 <= hour < 14: return "昼 (12-14時)"
    elif 14 <= hour < 15: return "昼跨ぎ (14-15時)"
    elif 15 <= hour < 18: return "午後 (15-18時)"
    elif 18 <= hour < 21: return "夜前半 (18-21時)"
    elif 21 <= hour < 22: return "夜ピーク (21-22時)"
    elif 22 <= hour < 24: return "夜後半 (22-24時)"
    else: return "深夜 (0-3時)"

def merge_event_data(df_to_merge, event_df):
    """配信データにイベント名をマージする"""
    if event_df.empty:
        df_to_merge['イベント名'] = ""
        return df_to_merge

    def find_event_name(row):
        account_id = str(row['アカウントID'])
        stream_time = row['配信日時']
        
        matching_events = event_df[
            (event_df['アカウントID'] == account_id) &
            (event_df['開始日時'] <= stream_time) &
            (event_df['終了日時'] >= stream_time)
        ]
        
        if not matching_events.empty:
            return matching_events.iloc[0]['イベント名']
        return ""

    df_to_merge['イベント名'] = df_to_merge.apply(find_event_name, axis=1)
    return df_to_merge


# --- メインロジック ---
if st.button("分析を実行"):
    # 1. アカウントIDの入力チェック
    if not account_id:
        st.error("アカウントIDを入力してください。")
        st.session_state.run_analysis = False
    # 2. 認証チェック
    elif not check_authentication(account_id):
        st.error(f"指定されたアカウントID（{account_id}）は認証されていません。")
        st.session_state.run_analysis = False
    # 3. 全てのチェックをパスした場合、分析処理を開始
    else:
        final_start_date, final_end_date = None, None

        if st.session_state.analysis_type_selector == '期間で指定':
            if selected_date_range_val and len(selected_date_range_val) == 2:
                final_start_date, final_end_date = selected_date_range_val
            else:
                st.error("有効な期間が選択されていません。")
        
        else:  # 'イベントで指定'
            if not selected_event_val:
                st.error("分析対象のイベントが選択されていません。")
            else:
                event_df = fetch_event_data()
                event_details = event_df[
                    (event_df['アカウントID'] == account_id) & 
                    (event_df['イベント名'] == selected_event_val)
                ]
                if not event_details.empty:
                    final_start_date = event_details.iloc[0]['開始日時']
                    final_end_date = event_details.iloc[0]['終了日時']
                else:
                    st.error("選択されたイベントの詳細が見つかりませんでした。")

        if final_start_date and final_end_date:
            st.session_state.run_analysis = True
            st.session_state.start_date = final_start_date
            st.session_state.end_date = final_end_date
            st.session_state.account_id = account_id
        else:
            st.session_state.run_analysis = False


if 'run_analysis' not in st.session_state:
    st.session_state.run_analysis = False

if st.session_state.get('run_analysis', False):
    start_date = st.session_state.start_date
    end_date = st.session_state.end_date
    account_id = st.session_state.account_id # 保存したaccount_idを使用

    mksp_df, df, room_id, room_name = load_and_preprocess_data(account_id, start_date, end_date)
    
    if df is None or df.empty:
        st.error("指定された期間・アカウントIDの配信データが見つかりませんでした。アカウントIDや期間を再度ご確認ください。")
        st.session_state.run_analysis = False # 分析を中断する
    else:
        st.success("データの読み込みが完了しました！")
        
        # --- ここから：イベント指定時は分析用データ自体を選択イベントの配信のみで上書きする ---
        # event_df_master を取得
        event_df_master = fetch_event_data()

        # ① 「時間帯」列を先に作っておく（列順に影響を出さないように）
        if '時間帯' not in df.columns:
            df['時間帯'] = df['配信日時'].dt.hour.apply(categorize_time_of_day_with_range)

        # ② 既存のマージ関数でイベント名を付与（元コードの merge_event_data を使用）
        df = merge_event_data(df, event_df_master)

        # ③ イベントで指定モードの場合は、選択イベントの**実際参加期間**の配信のみ抽出する
        #    （選択イベント名が available か確認してから絞り込む）
        if st.session_state.get('analysis_type_selector') == 'イベントで指定':
            # selectboxで使っている変数が selected_event_val ならそれを優先、なければセッションを参照
            selected_ev = selected_event_val if 'selected_event_val' in locals() and selected_event_val else st.session_state.get('selected_event_val', None)

            if selected_ev:
                # 選択イベントの期間を event_db から取得（アカウントIDで絞る）
                ev_details = event_df_master[
                    (event_df_master['アカウントID'] == account_id) &
                    (event_df_master['イベント名'] == selected_ev)
                ]
                if not ev_details.empty:
                    ev_start = ev_details.iloc[0]['開始日時']
                    ev_end = ev_details.iloc[0]['終了日時']

                    # **両方の条件**で絞る：① イベント名が選択イベント、かつ ② 配信日時がそのイベント期間内
                    df = df[
                        (df['イベント名'] == selected_ev) &
                        (df['配信日時'] >= ev_start) &
                        (df['配信日時'] <= ev_end)
                    ].copy()
                else:
                    # イベント詳細が見つからなければ、念のためイベント名でのみフィルタ（堅牢化）
                    df = df[df['イベント名'] == selected_ev].copy()
        # --- ここまで ---
        
        
        if mksp_df is not None and not mksp_df.empty:
            numeric_cols_to_check = [
                '合計視聴数', '初ルーム来訪者数', 'コメント人数', '初コメント人数', 'ギフト人数',
                '初ギフト人数', '視聴会員数', '短時間滞在者数', 'ギフト数',
                '期限あり/期限なしSGのギフティング数', '期限あり/期限なしSGのギフティング人数'
            ]
            for col in numeric_cols_to_check:
                if col in mksp_df.columns:
                    mksp_df[col] = pd.to_numeric(mksp_df[col], errors='coerce').fillna(0)

            filtered_df_visit = mksp_df[mksp_df['合計視聴数'] > 0].copy()
            st.session_state.mk_avg_rate_visit = (filtered_df_visit['初ルーム来訪者数'] / filtered_df_visit['合計視聴数']).mean() * 100 if not filtered_df_visit.empty else 0
            st.session_state.mk_median_rate_visit = (filtered_df_visit['初ルーム来訪者数'] / filtered_df_visit['合計視聴数']).median() * 100 if not filtered_df_visit.empty else 0

            filtered_df_comment = mksp_df[mksp_df['コメント人数'] > 0].copy()
            st.session_state.mk_avg_rate_comment = (filtered_df_comment['初コメント人数'] / filtered_df_comment['コメント人数']).mean() * 100 if not filtered_df_comment.empty else 0
            st.session_state.mk_median_rate_comment = (filtered_df_comment['初コメント人数'] / filtered_df_comment['コメント人数']).median() * 100 if not filtered_df_comment.empty else 0

            filtered_df_gift = mksp_df[mksp_df['ギフト人数'] > 0].copy()
            st.session_state.mk_avg_rate_gift = (filtered_df_gift['初ギフト人数'] / filtered_df_gift['ギフト人数']).mean() * 100 if not filtered_df_gift.empty else 0
            st.session_state.mk_median_rate_gift = (filtered_df_gift['初ギフト人数'] / filtered_df_gift['ギフト人数']).median() * 100 if not filtered_df_gift.empty else 0

            filtered_df_short_stay = mksp_df[mksp_df['視聴会員数'] > 0].copy()
            st.session_state.mk_avg_rate_short_stay = (filtered_df_short_stay['短時間滞在者数'] / filtered_df_short_stay['視聴会員数']).mean() * 100 if not filtered_df_short_stay.empty else 0
            st.session_state.mk_median_rate_short_stay = (filtered_df_short_stay['短時間滞在者数'] / filtered_df_short_stay['視聴会員数']).median() * 100 if not filtered_df_short_stay.empty else 0

            filtered_df_sg_gift = mksp_df[mksp_df['ギフト数'] > 0].copy()
            st.session_state.mk_avg_rate_sg_gift = (filtered_df_sg_gift['期限あり/期限なしSGのギフティング数'] / filtered_df_sg_gift['ギフト数']).mean() * 100 if not filtered_df_sg_gift.empty else 0
            st.session_state.mk_median_rate_sg_gift = (filtered_df_sg_gift['期限あり/期限なしSGのギフティング数'] / filtered_df_sg_gift['ギフト数']).median() * 100 if not filtered_df_sg_gift.empty else 0

            filtered_df_sg_person = mksp_df[mksp_df['ギフト人数'] > 0].copy()
            st.session_state.mk_avg_rate_sg_person = (filtered_df_sg_person['期限あり/期限なしSGのギフティング人数'] / filtered_df_sg_person['ギフト人数']).mean() * 100 if not filtered_df_sg_person.empty else 0
            st.session_state.mk_median_rate_sg_person = (filtered_df_sg_person['期限あり/期限なしSGのギフティング人数'] / filtered_df_sg_person['ギフト人数']).median() * 100 if not filtered_df_sg_person.empty else 0

        if account_id == "mksp":
            st.subheader("💡 全ライバーの集計データ")
            st.info("このビューでは、個人関連データは表示されません。")
            
            total_support_points = int(df["獲得支援point"].sum())
            total_viewers = int(df["合計視聴数"].sum())
            total_comments = int(df["コメント数"].sum())
            
            st.markdown(f"**合計獲得支援ポイント:** {total_support_points:,} pt")
            st.markdown(f"**合計視聴数:** {total_viewers:,} 人")
            st.markdown(f"**合計コメント数:** {total_comments:,} 件")

            st.subheader("📊 時間帯別パフォーマンス分析 (平均値)")
            st.info("※ このグラフは、各時間帯に配信した際の各KPIの**平均値**を示しています。棒上の数字は、その時間帯の配信件数です。")
            
            df['時間帯'] = df['配信日時'].dt.hour.apply(categorize_time_of_day_with_range)
            
            time_of_day_kpis_mean = df.groupby('時間帯').agg({
                '獲得支援point': 'mean',
                '合計視聴数': 'mean',
                'コメント数': 'mean'
            }).reset_index()

            time_of_day_order = ["深夜 (0-3時)", "早朝 (3-6時)", "朝 (6-9時)", "午前 (9-12時)", "昼 (12-14時)", "昼跨ぎ (14-15時)", "午後 (15-18時)", "夜前半 (18-21時)", "夜ピーク (21-22時)", "夜後半 (22-24時)"]
            time_of_day_kpis_mean['時間帯'] = pd.Categorical(time_of_day_kpis_mean['時間帯'], categories=time_of_day_order, ordered=True)
            time_of_day_kpis_mean = time_of_day_kpis_mean.sort_values('時間帯')
            
            time_of_day_counts = df['時間帯'].value_counts().reindex(time_of_day_order, fill_value=0)

            col1, col2, col3 = st.columns(3)

            with col1:
                fig1 = go.Figure(go.Bar(x=time_of_day_kpis_mean['時間帯'], y=time_of_day_kpis_mean['獲得支援point'], text=time_of_day_counts.loc[time_of_day_kpis_mean['時間帯']], textposition='auto', marker_color='#1f77b4', name='獲得支援point'))
                fig1.update_layout(title_text="獲得支援point", title_font_size=16, yaxis=dict(title="獲得支援point", title_font_size=14), font=dict(size=12), height=400, margin=dict(t=50, b=0, l=40, r=40))
                st.plotly_chart(fig1, use_container_width=True)
            with col2:
                fig2 = go.Figure(go.Bar(x=time_of_day_kpis_mean['時間帯'], y=time_of_day_kpis_mean['合計視聴数'], text=time_of_day_counts.loc[time_of_day_kpis_mean['時間帯']], textposition='auto', marker_color='#ff7f0e', name='合計視聴数'))
                fig2.update_layout(title_text="合計視聴数", title_font_size=16, yaxis=dict(title="合計視聴数", title_font_size=14), font=dict(size=12), height=400, margin=dict(t=50, b=0, l=40, r=40))
                st.plotly_chart(fig2, use_container_width=True)
            with col3:
                fig3 = go.Figure(go.Bar(x=time_of_day_kpis_mean['時間帯'], y=time_of_day_kpis_mean['コメント数'], text=time_of_day_counts.loc[time_of_day_kpis_mean['時間帯']], textposition='auto', marker_color='#2ca02c', name='コメント数'))
                fig3.update_layout(title_text="コメント数", title_font_size=16, yaxis=dict(title="コメント数", title_font_size=14), font=dict(size=12), height=400, margin=dict(t=50, b=0, l=40, r=40))
                st.plotly_chart(fig3, use_container_width=True)

            st.subheader("📊 時間帯別パフォーマンス分析 (中央値)")
            st.info("※ このグラフは、各時間帯に配信した際の各KPIの**中央値**を示しています。突出した値の影響を受けにくく、一般的な傾向を把握するのに役立ちます。棒上の数字は、その時間帯の配信件数です。")
            
            time_of_day_kpis_median = df.groupby('時間帯').agg({'獲得支援point': 'median', '合計視聴数': 'median', 'コメント数': 'median'}).reset_index()
            time_of_day_kpis_median['時間帯'] = pd.Categorical(time_of_day_kpis_median['時間帯'], categories=time_of_day_order, ordered=True)
            time_of_day_kpis_median = time_of_day_kpis_median.sort_values('時間帯')
            
            col4, col5, col6 = st.columns(3)
            
            with col4:
                fig4 = go.Figure(go.Bar(x=time_of_day_kpis_median['時間帯'], y=time_of_day_kpis_median['獲得支援point'], text=time_of_day_counts.loc[time_of_day_kpis_median['時間帯']], textposition='auto', marker_color='#1f77b4', name='獲得支援point'))
                fig4.update_layout(title_text="獲得支援point (中央値)", title_font_size=16, yaxis=dict(title="獲得支援point", title_font_size=14), font=dict(size=12), height=400, margin=dict(t=50, b=0, l=40, r=40))
                st.plotly_chart(fig4, use_container_width=True)
            with col5:
                fig5 = go.Figure(go.Bar(x=time_of_day_kpis_median['時間帯'], y=time_of_day_kpis_median['合計視聴数'], text=time_of_day_counts.loc[time_of_day_kpis_median['時間帯']], textposition='auto', marker_color='#ff7f0e', name='合計視聴数'))
                fig5.update_layout(title_text="合計視聴数 (中央値)", title_font_size=16, yaxis=dict(title="合計視聴数", title_font_size=14), font=dict(size=12), height=400, margin=dict(t=50, b=0, l=40, r=40))
                st.plotly_chart(fig5, use_container_width=True)
            with col6:
                fig6 = go.Figure(go.Bar(x=time_of_day_kpis_median['時間帯'], y=time_of_day_kpis_median['コメント数'], text=time_of_day_counts.loc[time_of_day_kpis_median['時間帯']], textposition='auto', marker_color='#2ca02c', name='コメント数'))
                fig6.update_layout(title_text="コメント数 (中央値)", title_font_size=16, yaxis=dict(title="コメント数", title_font_size=14), font=dict(size=12), height=400, margin=dict(t=50, b=0, l=40, r=40))
                st.plotly_chart(fig6, use_container_width=True)
            
        else: # 個別アカウントIDの場合
            st.subheader("📈 主要KPIの推移")
            df_sorted_asc = df.sort_values(by="配信日時", ascending=True).copy()
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df_sorted_asc["配信日時"], y=df_sorted_asc["獲得支援point"], name="獲得支援point", mode='lines+markers', marker=dict(symbol='circle')))
            fig.add_trace(go.Scatter(x=df_sorted_asc["配信日時"], y=df_sorted_asc["配信時間(分)"], name="配信時間(分)", mode='lines+markers', yaxis="y2", marker=dict(symbol='square')))
            fig.add_trace(go.Scatter(x=df_sorted_asc["配信日時"], y=df_sorted_asc["合計視聴数"], name="合計視聴数", mode='lines+markers', yaxis="y2", marker=dict(symbol='star')))
            fig.update_layout(title="KPIの推移（配信時間別）", xaxis=dict(title="配信日時"), yaxis=dict(title="獲得支援point", side="left", showgrid=False), yaxis2=dict(title="配信時間・視聴数", overlaying="y", side="right"), legend=dict(x=0, y=1.1, orientation="h"), hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)
            
            st.subheader("📊 時間帯別パフォーマンス分析 (平均値)")
            st.info("※ このグラフは、各時間帯に配信した際の各KPIの**平均値**を示しています。棒上の数字は、その時間帯の配信件数です。")
            df['時間帯'] = df['配信日時'].dt.hour.apply(categorize_time_of_day_with_range)
            time_of_day_kpis_mean = df.groupby('時間帯').agg({'獲得支援point': 'mean', '合計視聴数': 'mean', 'コメント数': 'mean'}).reset_index()
            # 修正: '朝 (6-9時)' を含む完全なリストに統一
            time_of_day_order = ["深夜 (0-3時)", "早朝 (3-6時)", "朝 (6-9時)", "午前 (9-12時)", "昼 (12-14時)", "昼跨ぎ (14-15時)", "午後 (15-18時)", "夜前半 (18-21時)", "夜ピーク (21-22時)", "夜後半 (22-24時)"]
            time_of_day_kpis_mean['時間帯'] = pd.Categorical(time_of_day_kpis_mean['時間帯'], categories=time_of_day_order, ordered=True)
            time_of_day_kpis_mean = time_of_day_kpis_mean.sort_values('時間帯')
            time_of_day_counts = df['時間帯'].value_counts().reindex(time_of_day_order, fill_value=0)
            col1, col2, col3 = st.columns(3)
            with col1:
                fig1 = go.Figure(go.Bar(x=time_of_day_kpis_mean['時間帯'], y=time_of_day_kpis_mean['獲得支援point'], text=time_of_day_counts.loc[time_of_day_kpis_mean['時間帯']], textposition='auto', marker_color='#1f77b4', name='獲得支援point'))
                fig1.update_layout(title_text="獲得支援point", title_font_size=16, yaxis=dict(title="獲得支援point", title_font_size=14), font=dict(size=12), height=400, margin=dict(t=50, b=0, l=40, r=40))
                st.plotly_chart(fig1, use_container_width=True)
            with col2:
                fig2 = go.Figure(go.Bar(x=time_of_day_kpis_mean['時間帯'], y=time_of_day_kpis_mean['合計視聴数'], text=time_of_day_counts.loc[time_of_day_kpis_mean['時間帯']], textposition='auto', marker_color='#ff7f0e', name='合計視聴数'))
                fig2.update_layout(title_text="合計視聴数", title_font_size=16, yaxis=dict(title="合計視聴数", title_font_size=14), font=dict(size=12), height=400, margin=dict(t=50, b=0, l=40, r=40))
                st.plotly_chart(fig2, use_container_width=True)
            with col3:
                fig3 = go.Figure(go.Bar(x=time_of_day_kpis_mean['時間帯'], y=time_of_day_kpis_mean['コメント数'], text=time_of_day_counts.loc[time_of_day_kpis_mean['時間帯']], textposition='auto', marker_color='#2ca02c', name='コメント数'))
                fig3.update_layout(title_text="コメント数", title_font_size=16, yaxis=dict(title="コメント数", title_font_size=14), font=dict(size=12), height=400, margin=dict(t=50, b=0, l=40, r=40))
                st.plotly_chart(fig3, use_container_width=True)

            st.subheader("📊 時間帯別パフォーマンス分析 (中央値)")
            st.info("※ このグラフは、各時間帯に配信した際の各KPIの**中央値**を示しています。突出した値の影響を受けにくく、一般的な傾向を把握するのに役立ちます。棒上の数字は、その時間帯の配信件数です。")
            time_of_day_kpis_median = df.groupby('時間帯').agg({'獲得支援point': 'median', '合計視聴数': 'median', 'コメント数': 'median'}).reset_index()
            time_of_day_kpis_median['時間帯'] = pd.Categorical(time_of_day_kpis_median['時間帯'], categories=time_of_day_order, ordered=True)
            time_of_day_kpis_median = time_of_day_kpis_median.sort_values('時間帯')
            col4, col5, col6 = st.columns(3)
            with col4:
                fig4 = go.Figure(go.Bar(x=time_of_day_kpis_median['時間帯'], y=time_of_day_kpis_median['獲得支援point'], text=time_of_day_counts.loc[time_of_day_kpis_median['時間帯']], textposition='auto', marker_color='#1f77b4', name='獲得支援point'))
                fig4.update_layout(title_text="獲得支援point (中央値)", title_font_size=16, yaxis=dict(title="獲得支援point", title_font_size=14), font=dict(size=12), height=400, margin=dict(t=50, b=0, l=40, r=40))
                st.plotly_chart(fig4, use_container_width=True)
            with col5:
                fig5 = go.Figure(go.Bar(x=time_of_day_kpis_median['時間帯'], y=time_of_day_kpis_median['合計視聴数'], text=time_of_day_counts.loc[time_of_day_kpis_median['時間帯']], textposition='auto', marker_color='#ff7f0e', name='合計視聴数'))
                fig5.update_layout(title_text="合計視聴数 (中央値)", title_font_size=16, yaxis=dict(title="合計視聴数", title_font_size=14), font=dict(size=12), height=400, margin=dict(t=50, b=0, l=40, r=40))
                st.plotly_chart(fig5, use_container_width=True)
            with col6:
                fig6 = go.Figure(go.Bar(x=time_of_day_kpis_median['時間帯'], y=time_of_day_kpis_median['コメント数'], text=time_of_day_counts.loc[time_of_day_kpis_median['時間帯']], textposition='auto', marker_color='#2ca02c', name='コメント数'))
                fig6.update_layout(title_text="コメント数 (中央値)", title_font_size=16, yaxis=dict(title="コメント数", title_font_size=14), font=dict(size=12), height=400, margin=dict(t=50, b=0, l=40, r=40))
                st.plotly_chart(fig6, use_container_width=True)
            
            st.subheader("📝 配信ごとの詳細データ")
            st.markdown(f"**▶ [各項目について](https://mksoul-pro.com/showroom/repo_koumoku)**", unsafe_allow_html=True)
            #st.link_button(
            #    label="▶ 各項目について",  # ボタンに表示されるテキスト
            #    url="https://mksoul-pro.com/showroom/repo_koumoku" # 遷移先のURL
            #)
            df_display = df.sort_values(by="配信日時", ascending=False).copy()
            # --- ここから修正（最小変更） ---
            event_df_master = fetch_event_data()

            # ① 列順を壊さないために「時間帯」列を先に作成しておく（存在しなければ追加）
            if '時間帯' not in df_display.columns:
                df_display['時間帯'] = df_display['配信日時'].dt.hour.apply(categorize_time_of_day_with_range)

            # ② イベント名マージ（既存の関数を利用）
            df_display = merge_event_data(df_display, event_df_master)

            # ③ 「イベントで指定」モードかつ選択イベントがあれば、**選択イベント名だけを抽出**
            #    （これが今回の要求：選択イベント以外は表示しない）
            if st.session_state.get('analysis_type_selector') == 'イベントで指定':
                # 選択イベントが selectbox で入っている変数名が selected_event_val の場合（UI側で同名を使っている想定）
                selected_ev = selected_event_val if 'selected_event_val' in locals() else None
                # もしセッションに保持されているならそれを優先
                if not selected_ev:
                    selected_ev = st.session_state.get('selected_event_val', None)
                if selected_ev:
                    df_display = df_display[df_display['イベント名'] == selected_ev].copy()
            # --- ここまで修正 ---
            
            # 修正: ルーム名列を追加
            if 'ルーム名' not in df_display.columns:
                 df_display['ルーム名'] = ''
            df_display['ルーム名'] = room_name

            # ③ 時刻のフォーマットを変更
            df_display_formatted = df_display.copy()
            df_display_formatted['配信日時'] = df_display_formatted['配信日時'].dt.strftime('%Y-%m-%d %H:%M')
            st.dataframe(df_display_formatted, hide_index=True)
            
            st.subheader("📝 全体サマリー")
            total_support_points = int(df_display["獲得支援point"].sum())
            if "フォロワー数" in df_display.columns and not df_display.empty:
                df_sorted_by_date = df_display.sort_values(by="配信日時")
                if not df_sorted_by_date.empty:
                    final_followers = int(df_sorted_by_date["フォロワー数"].iloc[-1])
                    initial_followers = int(df_sorted_by_date["フォロワー数"].iloc[0])
                    total_follower_increase = final_followers - initial_followers
                    st.markdown(f"**フォロワー純増数:** {total_follower_increase:,} 人")
                    st.markdown(f"**最終フォロワー数:** {final_followers:,} 人")
            st.markdown(f"**合計獲得支援ポイント:** {total_support_points:,} pt")

            st.subheader("📊 その他数値分析")
            row1_col1, row1_col2, row1_col3 = st.columns(3)
            row2_col1, row2_col2, row2_col3 = st.columns(3)
            metric_html_style = """<style>.stMetric-container{background-color:transparent;border:none;padding-bottom:20px;}.metric-label{font-size:16px;font-weight:600;color:#000;margin-bottom:-5px;}.metric-value{font-size:32px;font-weight:700;color:#1f77b4;}.metric-caption{font-size:12px;color:#a0a0a0;margin-top:-5px;}.metric-help{font-size:12px;color:#808080;margin-top:10px;line-height:1.5}</style>"""
            st.markdown(metric_html_style, unsafe_allow_html=True)
            with row1_col1:
                first_time_df = df_display.dropna(subset=['初ルーム来訪者数', '合計視聴数'])
                total_members_for_first_time = first_time_df["合計視聴数"].sum()
                first_time_visitors = first_time_df["初ルーム来訪者数"].sum()
                first_time_rate = f"{(first_time_visitors / total_members_for_first_time * 100):.1f}%" if total_members_for_first_time > 0 else "0%"
                metric_html = f"""<div class="stMetric-container"><div class="metric-label">初見訪問者率</div><div class="metric-value">{first_time_rate}</div><div class="metric-caption">（MK平均値：{st.session_state.get('mk_avg_rate_visit', 0):.1f}% / MK中央値：{st.session_state.get('mk_median_rate_visit', 0):.1f}%）</div><div class="metric-help">合計視聴数に対する初ルーム来訪者数の割合です。</div></div>"""
                st.markdown(metric_html, unsafe_allow_html=True)
            with row1_col2:
                comment_df = df_display.dropna(subset=['初コメント人数', 'コメント人数'])
                total_commenters = comment_df["コメント人数"].sum()
                first_time_commenters = comment_df["初コメント人数"].sum()
                first_comment_rate = f"{(first_time_commenters / total_commenters * 100):.1f}%" if total_commenters > 0 else "0%"
                metric_html = f"""<div class="stMetric-container"><div class="metric-label">初コメント率</div><div class="metric-value">{first_comment_rate}</div><div class="metric-caption">（MK平均値：{st.session_state.get('mk_avg_rate_comment', 0):.1f}% / MK中央値：{st.session_state.get('mk_median_rate_comment', 0):.1f}%）</div><div class="metric-help">合計コメント人数に対する初コメント会員数の割合です。</div></div>"""
                st.markdown(metric_html, unsafe_allow_html=True)
            with row1_col3:
                gift_df = df_display.dropna(subset=['初ギフト人数', 'ギフト人数'])
                total_gifters = gift_df["ギフト人数"].sum()
                first_time_gifters = gift_df["初ギフト人数"].sum()
                first_gift_rate = f"{(first_time_gifters / total_gifters * 100):.1f}%" if total_gifters > 0 else "0%"
                metric_html = f"""<div class="stMetric-container"><div class="metric-label">初ギフト率</div><div class="metric-value">{first_gift_rate}</div><div class="metric-caption">（MK平均値：{st.session_state.get('mk_avg_rate_gift', 0):.1f}% / MK中央値：{st.session_state.get('mk_median_rate_gift', 0):.1f}%）</div><div class="metric-help">合計ギフト会員数に対する初ギフト会員数の割合です。</div></div>"""
                st.markdown(metric_html, unsafe_allow_html=True)
            with row2_col1:
                short_stay_df = df_display.dropna(subset=['短時間滞在者数', '視聴会員数'])
                total_viewers_for_short_stay = short_stay_df["視聴会員数"].sum()
                short_stay_visitors = short_stay_df["短時間滞在者数"].sum()
                short_stay_rate = f"{(short_stay_visitors / total_viewers_for_short_stay * 100):.1f}%" if total_viewers_for_short_stay > 0 else "0%"
                metric_html = f"""<div class="stMetric-container"><div class="metric-label">短時間滞在者率</div><div class="metric-value">{short_stay_rate}</div><div class="metric-caption">（MK平均値：{st.session_state.get('mk_avg_rate_short_stay', 0):.1f}% / MK中央値：{st.session_state.get('mk_median_rate_short_stay', 0):.1f}%）</div><div class="metric-help">視聴会員数に対する滞在時間が1分未満の会員数の割合です。</div></div>"""
                st.markdown(metric_html, unsafe_allow_html=True)
            with row2_col2:
                sg_gift_df = df_display.dropna(subset=['期限あり/期限なしSGのギフティング数', 'ギフト数'])
                total_gifts = sg_gift_df["ギフト数"].sum()
                total_sg_gifts = sg_gift_df["期限あり/期限なしSGのギフティング数"].sum()
                sg_gift_rate = f"{(total_sg_gifts / total_gifts * 100):.1f}%" if total_gifts > 0 else "0%"
                metric_html = f"""<div class="stMetric-container"><div class="metric-label">SGギフト数率</div><div class="metric-value">{sg_gift_rate}</div><div class="metric-caption">（MK平均値：{st.session_state.get('mk_avg_rate_sg_gift', 0):.1f}% / MK中央値：{st.session_state.get('mk_median_rate_sg_gift', 0):.1f}%）</div><div class="metric-help">ギフト総数に対するSGギフト数の割合です。</div></div>"""
                st.markdown(metric_html, unsafe_allow_html=True)
            with row2_col3:
                sg_person_df = df_display.dropna(subset=['期限あり/期限なしSGのギフティング人数', 'ギフト人数'])
                total_gifters = sg_person_df["ギフト人数"].sum()
                total_sg_gifters = sg_person_df["期限あり/期限なしSGのギフティング人数"].sum()
                sg_person_rate = f"{(total_sg_gifters / total_gifters * 100):.1f}%" if total_gifters > 0 else "0%"
                metric_html = f"""<div class="stMetric-container"><div class="metric-label">SGギフト人数率</div><div class="metric-value">{sg_person_rate}</div><div class="metric-caption">（MK平均値：{st.session_state.get('mk_avg_rate_sg_person', 0):.1f}% / MK中央値：{st.session_state.get('mk_median_rate_sg_person', 0):.1f}%）</div><div class="metric-help">ギフト人数総数に対するSGギフト人数の割合です。</div></div>"""
                st.markdown(metric_html, unsafe_allow_html=True)

            st.markdown("<hr>", unsafe_allow_html=True)

            st.subheader("🎯 ヒット配信")
            st.info("特定の条件を満たしたパフォーマンスの高い配信をピックアップしています。")

            avg_support_points = df_display["獲得支援point"].mean()
            avg_sg_total = df_display["期限あり/期限なしSG総額"].mean()
            avg_sg_gifters = df_display["期限あり/期限なしSGのギフティング人数"].mean()
            avg_gifters = df_display["ギフト人数"].mean()
            avg_commenters = df_display["コメント人数"].mean()

            hit_broadcasts = []
            for index, row in df_display.iterrows():
                hit_items = []
                # ① 初見訪問者率
                if pd.notna(row['初ルーム来訪者数']) and row['合計視聴数'] > 0:
                    rate = (row['初ルーム来訪者数'] / row['合計視聴数']) * 100
                    mk_avg_rate_visit = st.session_state.get('mk_avg_rate_visit', 0)
                    if rate >= mk_avg_rate_visit * 2.2:
                        hit_items.append('初見訪問者率')
                # ② 初コメント率
                if pd.notna(row['初コメント人数']) and row['コメント人数'] > 0:
                    rate = (row['初コメント人数'] / row['コメント人数']) * 100
                    mk_avg_rate_comment = st.session_state.get('mk_avg_rate_comment', 0)
                    if rate >= mk_avg_rate_comment * 2.2:
                        hit_items.append('初コメント率')
                # ③ 初ギフト率
                if pd.notna(row['初ギフト人数']) and row['ギフト人数'] > 0:
                    rate = (row['初ギフト人数'] / row['ギフト人数']) * 100
                    mk_avg_rate_gift = st.session_state.get('mk_avg_rate_gift', 0)
                    if rate >= mk_avg_rate_gift * 2.2:
                        hit_items.append('初ギフト率')
                # ④ 短時間滞在者率
                if pd.notna(row['短時間滞在者数']) and row['視聴会員数'] > 0:
                    rate = (row['短時間滞在者数'] / row['視聴会員数']) * 100
                    mk_avg_rate_short_stay = st.session_state.get('mk_avg_rate_short_stay', 0)
                    if rate <= mk_avg_rate_short_stay * 0.4:
                        hit_items.append('短時間滞在者率')
                # ⑤ 獲得支援point
                if pd.notna(row['獲得支援point']) and row['獲得支援point'] >= avg_support_points * 2.7: hit_items.append('獲得支援point')
                # ⑥ SG総額
                if pd.notna(row['期限あり/期限なしSG総額']) and row['期限あり/期限なしSG総額'] >= avg_sg_total * 2.7: hit_items.append('SG総額')
                # ⑦ SGギフト人数
                if pd.notna(row['期限あり/期限なしSGのギフティング人数']) and row['期限あり/期限なしSGのギフティング人数'] >= avg_sg_gifters * 2.2: hit_items.append('SGギフト人数')
                # ⑧ ギフト人数
                if pd.notna(row['ギフト人数']) and row['ギフト人数'] >= avg_gifters * 2.2: hit_items.append('ギフト人数')
                # ⑨ コメント人数
                if pd.notna(row['コメント人数']) and row['コメント人数'] >= avg_commenters * 2.2: hit_items.append('コメント人数')
                if hit_items:
                    hit_broadcasts.append({'配信日時': row['配信日時'], 'ヒット項目': ', '.join(hit_items), 'イベント名': row['イベント名']})
            if hit_broadcasts:
                hit_df = pd.DataFrame(hit_broadcasts)
                hit_df['配信日時'] = pd.to_datetime(hit_df['配信日時']).dt.strftime('%Y-%m-%d %H:%M')
                st.dataframe(hit_df, hide_index=True)
            else:
                st.info("条件を満たす「ヒット配信」は見つかりませんでした。")