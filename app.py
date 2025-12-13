import streamlit as st
import requests
import pandas as pd
import io
import datetime
from dateutil import parser
import numpy as np
import re
import json # JSON出力をきれいにするためにインポート

# Streamlit の初期設定
st.set_page_config(
    page_title="SHOWROOM ルームステータス可視化ツール",
    layout="wide"
)

# --- 定数設定 ---
ROOM_LIST_URL = "https://mksoul-pro.com/showroom/file/room_list.csv"
ROOM_PROFILE_API = "https://www.showroom-live.com/api/room/profile?room_id={room_id}"
API_EVENT_ROOM_LIST_URL = "https://www.showroom-live.com/api/event/room_list" 
HEADERS = {} 

GENRE_MAP = {
    112: "ミュージック", 102: "アイドル", 103: "タレント", 104: "声優",
    105: "芸人", 107: "バーチャル", 108: "モデル", 109: "俳優",
    110: "アナウンサー", 113: "クリエイター", 200: "ライバー",
}

# --- ユーティリティ関数 ---

def _safe_get(data, keys, default_value=None):
    """ネストされた辞書から安全に値を取得するヘルパー関数"""
    temp = data
    for key in keys:
        if isinstance(temp, dict) and key in temp:
            temp = temp.get(key)
        else:
            return default_value
    # 欠損値判定を厳密化
    if temp is None or (isinstance(temp, (str, float)) and pd.isna(temp)):
        return default_value
    return temp

# 🚨 この関数はルームIDの数値範囲による簡易判定（今回の「公/フ」表示では使用しない）
def get_official_mark(room_id):
    """簡易的な公/フ判定"""
    try:
        room_id = int(room_id)
        if room_id < 100000:
             return "公"
        elif room_id >= 100000:
             return "フ"
        else:
            return "不明"
    except (TypeError, ValueError):
        return "不明"


def get_room_profile(room_id):
    """ライバー（ルーム）プロフィール情報APIからデータを取得する"""
    url = ROOM_PROFILE_API.format(room_id=room_id)
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException:
        return None

# --- イベント情報取得関数群 ---

def get_total_entries(event_id):
    """
    指定されたイベントの総参加ルーム数を取得します。
    """
    params = {"event_id": event_id}
    try:
        response = requests.get(API_EVENT_ROOM_LIST_URL, headers=HEADERS, params=params, timeout=10)
        if response.status_code == 404:
            return 0
        response.raise_for_status()
        data = response.json()
        return data.get('total_entries', 0)
    except requests.exceptions.RequestException:
        return "N/A"
    except ValueError:
        return "N/A"


def get_event_room_list_data(event_id):
    """ /api/event/room_list?event_id= を叩いて参加ルーム一覧（主に上位30）を取得する """
    params = {"event_id": event_id}
    try:
        resp = requests.get(API_EVENT_ROOM_LIST_URL, headers=HEADERS, params=params, timeout=10)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        data = resp.json()
        
        if isinstance(data, dict):
            # APIレスポンスの構造に対応するため、複数のキーをチェック
            for k in ('list', 'room_list', 'event_entry_list', 'entries', 'data', 'event_list'):
                if k in data and isinstance(data[k], list):
                    return data[k]
        if isinstance(data, list):
            return data
            
    except Exception:
        return []
        
    return []

def get_event_participants_info(event_id, target_room_id, limit=10):
    """
    イベント参加ルーム情報・状況APIから必要な情報を抽出する。
    上位10ルームについては、個別のプロフィールAPIを叩いて詳細情報を統合する。
    """
    if not event_id:
        # 🚨 生APIデータデバッグのためにキーを追加
        return {"total_entries": "-", "rank": "-", "point": "-", "level": "-", "top_participants": [], "raw_api_data_sample": "N/A"}

    total_entries = get_total_entries(event_id)
    room_list_data = get_event_room_list_data(event_id)
    current_room_data = None
    
    # --- 🔥 デバッグ情報として生APIデータのサンプルを抽出 (先頭3件) ---
    raw_api_data_sample = room_list_data[:3]
    # -----------------------------------------------------------------
    
    # ターゲットルームの情報をリストから探す
    for room in room_list_data:
        if str(room.get("room_id")) == str(target_room_id):
            current_room_data = room
            break

    rank = _safe_get(current_room_data, ["rank"], "-")
    point = _safe_get(current_room_data, ["point"], "-")
    
    # 🚨 修正: `quest_level` が `None` の問題に対応するため、複数の候補キーからレベル情報を取得する
    level = None
    level_keys = ["quest_level", "level", "event_level", "event_rank_info"] # 候補キー
    for key in level_keys:
        level = _safe_get(current_room_data, [key], None)
        if level is not None and level != "-":
            break
    
    if level is None:
        level = "-"
    
    # ... (以下のロジックは変更なし) ...

    top_participants = room_list_data
    # 要件: ポイントの高い順にソート
    if top_participants:
        # ポイントでソート (pointがない/None/無効な値の場合は0として扱う)
        top_participants.sort(key=lambda x: int(str(x.get('point', 0) or 0)), reverse=True)
    
    top_participants = top_participants[:limit] # 上位10件に制限


    # ✅ 上位10ルームのプロフィール情報を取得し、データをエンリッチ（統合）
    enriched_participants = []
    for participant in top_participants:
        room_id = participant.get('room_id')
        
        # 取得必須のキーを初期化（Noneで初期化）
        for key in ['room_level', 'show_rank_subdivided', 'follower_num', 'live_continuous_days', 'is_official_api']: 
            participant[key] = None
            
        if room_id:
            # 個別のプロフィールAPIを叩く
            profile = get_room_profile(room_id)
            if profile:
                # プロフィールデータをマージ
                participant['room_level'] = _safe_get(profile, ["room_level"], None)
                participant['show_rank_subdivided'] = _safe_get(profile, ["show_rank_subdivided"], None)
                participant['follower_num'] = _safe_get(profile, ["follower_num"], None)
                participant['live_continuous_days'] = _safe_get(profile, ["live_continuous_days"], None)
                
                # ✅ is_officialを追加で取得
                participant['is_official_api'] = _safe_get(profile, ["is_official"], None)
                
                # ルーム名が空の場合に備えて補完
                if not participant.get('room_name'):
                     participant['room_name'] = _safe_get(profile, ["room_name"], f"Room {room_id}")
        
        
        # 🔥 エンリッチ後、`quest_level` がNoneの場合、他の候補キーをチェック
        if _safe_get(participant, ["quest_level"], None) is None:
             # `get_event_participants_info` の冒頭でチェックしたキーを、ここでも適用
             for key in level_keys:
                 if key in participant and _safe_get(participant, [key], None) is not None:
                     participant['quest_level'] = _safe_get(participant, [key], None)
                     break
        
        # 最終的に`quest_level`がセットされていない場合、ここでキーを追加（DataFrame化でエラーが出ないように）
        if 'quest_level' not in participant:
             participant['quest_level'] = None


        enriched_participants.append(participant)

    # 応答に必要な情報を返す
    return {
        "total_entries": total_entries if isinstance(total_entries, int) and total_entries > 0 else "-",
        "rank": rank,
        "point": point,
        "level": level,
        "top_participants": enriched_participants, # エンリッチされたリストを返す
        "raw_api_data_sample": raw_api_data_sample # 🚨 生データデバッグ情報
    }
# --- イベント情報取得関数群ここまで ---


def display_room_status(profile_data, input_room_id):
    """取得したルームプロフィールデータとイベントデータを表示する"""
    
    # データを安全に取得
    room_name = _safe_get(profile_data, ["room_name"], "取得失敗")
    # ... (他の基本情報取得は省略) ...
    room_level = _safe_get(profile_data, ["room_level"], "-")
    show_rank = _safe_get(profile_data, ["show_rank_subdivided"], "-")
    next_score = _safe_get(profile_data, ["next_score"], "-")
    prev_score = _safe_get(profile_data, ["prev_score"], "-")
    follower_num = _safe_get(profile_data, ["follower_num"], "-")
    live_continuous_days = _safe_get(profile_data, ["live_continuous_days"], "-")
    is_official = _safe_get(profile_data, ["is_official"], None)
    genre_id = _safe_get(profile_data, ["genre_id"], None)
    event = _safe_get(profile_data, ["event"], {})

    # 加工・整形
    official_status = "公式" if is_official is True else "フリー" if is_official is False else "-"
    genre_name = GENRE_MAP.get(genre_id, f"その他 ({genre_id})" if genre_id else "-")
    
    room_url = f"https://www.showroom-live.com/room/profile?room_id={input_room_id}"
    
    
    # --- 💡 カスタムCSSの定義（既存のまま維持） ---
    custom_styles = """
    <style>
    /* 全体のフォント統一と余白調整 */
    h3 { 
        margin-top: 20px; 
        padding-top: 10px; 
        /* セクション見出しのボーダーを削除 */
        border-bottom: none; 
    }

    /* タイトル領域のスタイル */
    .room-title-container {
        padding: 15px 20px;
        margin-bottom: 20px;
        border-radius: 8px;
        background-color: #f0f2f6; 
        border: 1px solid #e6e6e6;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
        display: flex;
        align-items: center;
    }
    .room-title-container h1 {
        margin: 0;
        padding: 0;
        line-height: 1.2;
        font-size: 28px; 
    }
    .room-title-container .title-icon {
        font-size: 30px; 
        margin-right: 15px;
        color: #ff4b4b; 
    }
    .room-title-container a {
        text-decoration: none; 
        color: #1c1c1c; 
    }
    
    /* 🚀 ルーム基本情報のカスタムメトリック用スタイル */
    .custom-metric-container {
        margin-bottom: 15px; 
        padding: 5px 0;
    }
    .metric-label {
        font-size: 14px; 
        color: #666; 
        font-weight: 600;
        margin-bottom: 5px;
        display: block; 
    }
    .metric-value {
        font-size: 24px !important; 
        font-weight: bold;
        line-height: 1.1;
        color: #1c1c1c;
    }
    
    /* st.metric の値を強制的に揃える (イベント情報セクション用) */
    .stMetric label {
        font-size: 14px; 
    }
    .stMetric > div > div:nth-child(2) > div {
        font-size: 24px !important; 
        font-weight: bold;
    }
    
    /* HTMLテーブルのスタイル */
    .stHtml .dataframe {
        width: 100%; 
        /* min-width固定値なし */
        border-collapse: collapse;
        margin-top: 10px; 
    }
    .stHtml .dataframe th {
        background-color: #e8eaf6; 
        color: #1a237e; 
        font-weight: bold;
        padding: 8px 10px; 
        font-size: 14px;
        text-align: left;
        border-bottom: 2px solid #c5cae9; 
        white-space: nowrap;
    }
    .stHtml .dataframe td {
        padding: 6px 10px; 
        font-size: 13px; 
        line-height: 1.4;
        border-bottom: 1px solid #f0f0f0;
        white-space: nowrap; 
    }
    .stHtml .dataframe tbody tr:hover {
        background-color: #f7f9fd; 
    }

    /* 列ごとの配置調整 (レベルが末尾に来るように調整) */
    /* 数値系の列をすべて右寄せに統一 */
    .stHtml .dataframe th:nth-child(2), .stHtml .dataframe td:nth-child(2), /* ルームレベル */
    .stHtml .dataframe th:nth-child(4), .stHtml .dataframe td:nth-child(4), /* フォロワー数 */
    .stHtml .dataframe th:nth-child(5), .stHtml .dataframe td:nth-child(5), /* まいにち配信 */
    .stHtml .dataframe th:nth-child(7), .stHtml .dataframe td:nth-child(7), /* 順位 */
    .stHtml .dataframe th:nth-child(8), .stHtml .dataframe td:nth-child(8), /* ポイント */
    .stHtml .dataframe th:nth-child(9), .stHtml .dataframe td:nth-child(9) { /* レベル (最終列) */
        text-align: right !important; 
        width: 10%; 
    }
    /* ランクと公/フを中央寄せ */
    .stHtml .dataframe th:nth-child(3), .stHtml .dataframe td:nth-child(3) { /* ランク */
        text-align: center !important; 
        width: 8%;
    }
    .stHtml .dataframe th:nth-child(6), .stHtml .dataframe td:nth-child(6) { /* 公/フ */
        text-align: center !important; 
        font-weight: bold;
        color: inherit; 
        width: 5%;
    }
    /* ルーム名のセル幅を柔軟に */
    .stHtml .dataframe th:nth-child(1), .stHtml .dataframe td:nth-child(1) {
        min-width: 280px; 
        white-space: normal !important; 
    }
    </style>
    """
    st.markdown(custom_styles, unsafe_allow_html=True)

    # ヘルパー関数: カスタムスタイルを適用したメトリックを表示
    def custom_metric(label, value):
        st.markdown(
            f'<div class="custom-metric-container">'
            f'<span class="metric-label">{label}</span>'
            f'<div class="metric-value">{value}</div>'
            f'</div>',
            unsafe_allow_html=True
        )


    # --- 1. 🎤 ルーム名/ID (タイトル領域) ---
    st.markdown(
        f'<div class="room-title-container">'
        f'<span class="title-icon">🎤</span>'
        f'<h1><a href="{room_url}" target="_blank">{room_name} ({input_room_id})</a> のルームステータス</h1>'
        f'</div>', 
        unsafe_allow_html=True
    )
    
    # ... (ルーム基本情報表示は省略) ...
    st.markdown("### 📊 ルーム基本情報")
    col1, col2, col3, col4 = st.columns([1.5, 1.5, 1.5, 1.5]) 

    with col1:
        custom_metric("ルームレベル", f'{room_level:,}' if isinstance(room_level, int) else str(room_level))
        custom_metric("フォロワー数", f'{follower_num:,}' if isinstance(follower_num, int) else str(follower_num))
        
    with col2:
        custom_metric("まいにち配信（日数）", live_continuous_days)
        custom_metric("公式 or フリー", official_status)

    with col3:
        custom_metric("現在のSHOWランク", show_rank)
        custom_metric("ジャンル", genre_name)

    with col4:
        custom_metric("上位ランクまでのスコア", f'{next_score:,}' if isinstance(next_score, int) else str(next_score))
        custom_metric("下位ランクまでのスコア", f'{prev_score:,}' if isinstance(prev_score, int) else str(prev_score))


    st.divider()

    # --- 3. 🏆 現在の参加イベント情報（第二カテゴリー） ---
    st.markdown("### 🏆 現在の参加イベント情報")

    event_id = event.get("event_id")
    event_name = event.get("name", "現在イベントに参加していません")
    event_url = event.get("url", "#")
    started_at_ts = event.get("started_at")
    ended_at_ts = event.get("ended_at")

    if event_id and event_name:
        
        # タイムスタンプを日本時間（JST）の文字列に変換
        def _ts_to_jst_str(ts):
            if ts:
                dt_utc = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
                dt_jst = dt_utc.astimezone(datetime.timezone(datetime.timedelta(hours=9)))
                return dt_jst.strftime('%Y/%m/%d %H:%M')
            return "-"

        started_at_str = _ts_to_jst_str(started_at_ts)
        ended_at_str = _ts_to_jst_str(ended_at_ts)

        # イベント名とリンク
        st.markdown(f"##### 🔗 **<a href='{event_url}' target='_blank'>{event_name}</a>**", unsafe_allow_html=True)
        
        # イベント期間の表示 (2カラム)
        st.markdown("#### イベント期間")
        event_col_time1, event_col_time2 = st.columns(2)
        with event_col_time1:
            st.info(f"📅 開始: **{started_at_str}**")
        with event_col_time2:
            st.info(f"🔚 終了: **{ended_at_str}**")

        # イベント参加情報（API取得）
        with st.spinner("イベント参加情報を取得中..."):
            # 🚨 get_event_participants_info の戻り値に raw_api_data_sample が追加されている
            event_info = get_event_participants_info(event_id, input_room_id, limit=10)
            
            total_entries = event_info["total_entries"]
            rank = event_info["rank"]
            point = event_info["point"]
            level = event_info["level"]
            raw_api_data_sample = event_info["raw_api_data_sample"] # 🚨 新しいデバッグ情報
            
            # イベント参加情報表示 (4カラムで横並び) - st.metric を使用
            st.markdown("#### 参加状況（自己ルーム）")
            event_col_data1, event_col_data2, event_col_data3, event_col_data4 = st.columns([1, 1, 1, 1])
            with event_col_data1:
                st.metric(label="参加ルーム数", value=f"{total_entries:,}" if isinstance(total_entries, int) else str(total_entries), delta_color="off")
            with event_col_data2:
                st.metric(label="現在の順位", value=str(rank), delta_color="off")
            with event_col_data3:
                st.metric(label="獲得ポイント", value=f"{point:,}" if isinstance(point, int) else str(point), delta_color="off")
            with event_col_data4:
                st.metric(label="レベル", value=str(level), delta_color="off")

            top_participants = event_info["top_participants"]


        st.divider()

        # --- 🚨 新しいデバッグ情報表示セクション ---
        st.markdown("### 🔴 API生データサンプル（イベント参加情報）")
        if raw_api_data_sample and raw_api_data_sample != "N/A":
            st.warning("この情報には、レベル情報の正しいキー名が含まれている可能性があります。")
            json_str = json.dumps(raw_api_data_sample, indent=2, ensure_ascii=False)
            st.code(json_str, language='json')
        else:
            st.info("API生データサンプルは取得できませんでした。")
        st.markdown("---")
        # ----------------------------------------
        
        # --- 4. 🔝 参加イベント上位10ルーム（HTMLテーブル） ---
        st.markdown("### 🔝 参加イベント上位10ルーム")
        
        if top_participants:
            
            dfp = pd.DataFrame(top_participants)

            # 必要なカラムが全て存在することを確認
            cols = [
                'room_name', 'room_level', 'show_rank_subdivided', 'follower_num',
                'live_continuous_days', 'room_id', 'rank', 'point',
                'is_official_api', 'quest_level' # quest_levelを含む
            ]
            
            # DataFrameに欠損しているカラムをNoneで埋める（APIエラー時などに備えて）
            for c in cols:
                if c not in dfp.columns:
                    dfp[c] = None
                    
            dfp_display = dfp[cols].copy()

            # ▼ rename（ユーザー様の仕様通り）
            dfp_display.rename(columns={
                'room_name': 'ルーム名', 
                'room_level': 'ルームレベル', 
                'show_rank_subdivided': 'ランク',
                'follower_num': 'フォロワー数', 
                'live_continuous_days': 'まいにち配信', 
                'rank': '順位', 
                'point': 'ポイント',
                'is_official_api': 'is_official_api',
                'quest_level': 'レベル' 
            }, inplace=True)

            # ▼ 公/フ 判定関数（API情報使用）
            def get_official_mark_from_api(is_official_value):
                """APIのis_official値に基づいて公/フを判定する (True=公, False=フ)"""
                if is_official_value is True:
                    return "公"
                elif is_official_value is False:
                    return "フ"
                else:
                    return "不明"
            
            # ▼ 公/フ を追加
            dfp_display["公/フ"] = dfp_display['is_official_api'].apply(get_official_mark_from_api)
            
            # 不要になった is_official_api 列を削除 (room_idはリンク生成のために残す)
            dfp_display.drop(columns=['is_official_api'], inplace=True, errors='ignore')


            # --- ▼ 数値フォーマット関数（カンマ区切りを切替可能） ▼ ---
            def _fmt_int_for_display(v, use_comma=True):
                """
                数値を整形する。None, NaN, 空文字列の場合はハイフンを返す。
                """
                try:
                    # None, NaN, 空文字列の場合にハイフン "-" を返す
                    if v is None or (isinstance(v, (str, float)) and (str(v).strip() == "" or pd.isna(v))):
                        return "-"
                    
                    num = float(v)
                    
                    if use_comma:
                        return f"{int(num):,}"
                    else:
                        # カンマなしの場合、整数を文字列で返す (例: 2 -> "2")
                        return f"{int(num)}"
                        
                except Exception:
                    # 数値変換できなかった場合は文字列として返す
                    return str(v)

            # --- ▼ 列ごとにフォーマット適用 ▼ ---
            format_cols_no_comma = ['ルームレベル', 'フォロワー数', 'まいにち配信', '順位'] 
            format_cols_comma = ['ポイント']

            for col in format_cols_comma:
                if col in dfp_display.columns:
                    dfp_display[col] = dfp_display[col].apply(lambda x: _fmt_int_for_display(x, use_comma=True))
            
            for col in format_cols_no_comma:
                if col in dfp_display.columns:
                    dfp_display[col] = dfp_display[col].apply(lambda x: _fmt_int_for_display(x, use_comma=False))
            

            # 🔥 デバッグロジック追加と「レベル」列のフォーマット処理を分離

            # デバッグ情報を格納するリスト
            debug_data = [] 

            def format_level_safely_FINAL_WITH_DEBUG(val, room_id):
                """APIの値(val)を安全にレベル表示用文字列に変換する (デバッグ情報付き)"""
                raw_type = type(val).__name__
                
                # None, NaN, 空文字列、空リスト/タプル、False の場合にハイフン "-" を返す
                if val is None or pd.isna(val) or str(val).strip() == "" or val is False or (isinstance(val, (list, tuple)) and not val):
                    final_value = "-"
                else:
                    try:
                        # 有効な数値（0や2など）は必ず整数文字列に変換して返す
                        final_value = str(int(val))
                    except (ValueError, TypeError):
                        # 数値変換できなかった場合はハイフンを返す
                        final_value = "-"
                
                # デバッグ情報を記録
                debug_data.append({
                    'Room ID': room_id,
                    'API Raw Value (quest_level)': str(val),
                    'API Raw Type': raw_type,
                    'Conversion Result': final_value
                })
                
                return final_value

            if 'レベル' in dfp_display.columns:
                # 処理に使うroom_id列（一時的に残っている）を取得
                room_ids = dfp_display['room_id'] 
                
                # apply関数は一つの引数しか取れないため、zipを使ってループ処理
                dfp_display['レベル'] = [
                    format_level_safely_FINAL_WITH_DEBUG(level_val, room_id)
                    for level_val, room_id in zip(dfp_display['レベル'], room_ids)
                ]
            
            
            # 最終的な欠損値/空文字列のハイフン化（主にランクなど数値フォーマットを通らない文字列列用）
            for col in ['ランク']: # レベルは既に専用処理で整形済み
                if col in dfp_display.columns:
                    dfp_display[col] = dfp_display[col].apply(lambda x: '-' if x == '' or pd.isna(x) else x)


            # --- ルーム名をリンクに置き換える ---
            def _make_link_final(row):
                rid = row['room_id']
                name = row['ルーム名']
                if not name:
                    name = f"room_{rid}"
                # target="_blank"で別窓リンク
                return f'<a href="https://www.showroom-live.com/room/profile?room_id={rid}" target="_blank">{name}</a>'

            # リンクを生成し、dfp_displayの'ルーム名'列を上書き
            dfp_display['ルーム名'] = dfp_display.apply(_make_link_final, axis=1)
            
            # 不要になった room_id 列を削除
            dfp_display.drop(columns=['room_id'], inplace=True, errors='ignore')

            # ▼ 列順をここで整える（レベルは末尾）
            dfp_display = dfp_display[
                ['ルーム名', 'ルームレベル', 'ランク', 'フォロワー数',
                 'まいにち配信', '公/フ', '順位', 'ポイント', 'レベル'] 
            ]
            
            # --- 💡 前回のデバッグ情報表示セクション ---
            if debug_data:
                st.markdown("---")
                st.markdown("### 🐞 デバッグ情報（レベル列の変換結果）")
                
                df_debug = pd.DataFrame(debug_data)
                
                # Room IDをリンクに変換
                def make_room_id_link(room_id):
                    return f'<a href="https://www.showroom-live.com/room/profile?room_id={room_id}" target="_blank">{room_id}</a>'
                
                df_debug['Room ID'] = df_debug['Room ID'].apply(make_room_id_link)
                
                # デバッグテーブルを表示
                html_debug_table = df_debug.to_html(
                    escape=False, 
                    index=False, 
                    justify='left', 
                    classes='dataframe data-table' 
                )
                st.markdown(f'<div style="overflow-x: auto; padding-bottom: 10px;">{html_debug_table}</div>', unsafe_allow_html=True)
            # -----------------------------------

            # コンパクトに expander 内で表示
            with st.expander("参加ルーム一覧（ポイント順上位10ルーム）", expanded=True):
                
                # to_htmlでHTMLタグが混入したルーム名列を正しくエスケープせずに表示させる
                html_table = dfp_display.to_html(
                    escape=False, 
                    index=False, 
                    justify='left', 
                    classes='dataframe data-table data-table-full-width' 
                )
                
                # 不要な改行を削除し、HTML出力を安定化させる
                html_table = html_table.replace('\n', '')
                html_table = re.sub(r'>\s+<', '><', html_table)

                # HTMLテーブル全体を div でラップし、インラインで横スクロールを強制適用
                html_container = f'<div style="overflow-x: auto; padding-bottom: 10px;">{html_table}</div>'

                # カスタムCSSとHTMLテーブルを一緒に表示
                st.markdown(html_container, unsafe_allow_html=True)
        else:
            st.info("参加ルーム情報が取得できませんでした（ランキングイベントではない、またはデータがまだありません）。")

    else:
        st.info("現在、このルームはイベントに参加していません。")

# --- メインロジック ---
# ... (認証ロジック、メインUIのロジックは省略。変更なし) ...

# セッションステートの初期化
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'show_status' not in st.session_state:
    st.session_state.show_status = False
if 'input_room_id' not in st.session_state:
    st.session_state.input_room_id = ""

# ▼▼ 認証ステップ ▼▼
if not st.session_state.authenticated:
    st.title("💖 SHOWROOM ルームステータス可視化ツール")
    st.markdown("##### 🔑 認証コードを入力してください")
    
    input_auth_code = st.text_input(
        "認証コードを入力してください:",
        placeholder="認証コード",
        type="password",
        key="room_id_input_auth"
    )

    # 認証ボタン
    if st.button("認証する"):
        if input_auth_code:
            with st.spinner("認証中..."):
                try:
                    response = requests.get(ROOM_LIST_URL, timeout=5)
                    response.raise_for_status()
                    room_df = pd.read_csv(io.StringIO(response.text), header=None, dtype=str)

                    valid_codes = set(str(x).strip() for x in room_df.iloc[:, 0].dropna())

                    if input_auth_code.strip() in valid_codes:
                        st.session_state.authenticated = True
                        st.success("✅ 認証に成功しました。ツールを利用できます。")
                        st.rerun()
                    else:
                        st.error("❌ 認証コードが無効です。正しい認証コードを入力してください。")
                except Exception as e:
                    st.error(f"認証リストを取得できませんでした: {e}")
        else:
            st.warning("認証コードを入力してください。")

    st.stop()
# ▲▲ 認証ステップここまで ▲▲


# --- 認証後メインUI ---

if st.session_state.authenticated:
    st.title("💖 SHOWROOM ルームステータス可視化ツール")
    
    st.markdown("### 🔎 ルームIDの入力")
    
    input_room_id_current = st.text_input(
        "表示したいルームIDを入力してください:",
        placeholder="例: 496122",
        key="room_id_input_main",
        value=st.session_state.input_room_id
    ).strip()

    if input_room_id_current != st.session_state.input_room_id:
        st.session_state.input_room_id = input_room_id_current
        st.session_state.show_status = False

    # 「ルームステータスを表示」ボタン
    if st.button("ルームステータスを表示"):
        if st.session_state.input_room_id and st.session_state.input_room_id.isdigit():
            st.session_state.show_status = True
        elif st.session_state.input_room_id:
            st.error("ルームIDは数字で入力してください。")
        else:
            st.warning("ルームIDを入力してください。")

    st.divider()
    
    # 情報表示エリア
    if st.session_state.show_status and st.session_state.input_room_id:
        
        # APIからの情報取得
        with st.spinner(f"ルームID {st.session_state.input_room_id} の情報を取得中..."):
            room_profile = get_room_profile(st.session_state.input_room_id)
        
        if room_profile:
            # 情報の表示
            display_room_status(room_profile, st.session_state.input_room_id)
        else:
            st.error(f"ルームID {st.session_state.input_room_id} の情報を取得できませんでした。IDを確認してください。")

    # 認証解除ボタン
    st.markdown("---")
    if st.button("認証を解除する", help="認証状態をリセットし、認証コード入力画面に戻ります"):
        st.session_state.authenticated = False
        st.session_state.show_status = False
        st.session_state.input_room_id = ""
        st.rerun()