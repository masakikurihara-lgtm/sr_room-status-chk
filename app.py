import streamlit as st
import requests
import pandas as pd
import io
import datetime
from dateutil import parser
import numpy as np

# Streamlit の初期設定
st.set_page_config(
    page_title="SHOWROOM ルームステータス可視化ツール"
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
    if temp is None or (isinstance(temp, (str, float)) and pd.isna(temp)):
        return default_value
    return temp

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

# --- イベント情報取得関数群 (前回のロジック維持) ---

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
    イベント参加ルーム情報・状況APIから必要な情報を抽出する。（この関数で総参加者数とランキングデータを統合）
    """
    if not event_id:
        return {"total_entries": "-", "rank": "-", "point": "-", "level": "-", "top_participants": []}

    total_entries = get_total_entries(event_id)
    room_list_data = get_event_room_list_data(event_id)
    current_room_data = None
    
    for room in room_list_data:
        if str(room.get("room_id")) == str(target_room_id):
            current_room_data = room
            break

    rank = _safe_get(current_room_data, ["rank"], "-")
    point = _safe_get(current_room_data, ["point"], "-")
    level = _safe_get(current_room_data, ["quest_level"], "-")

    top_participants = room_list_data
    if top_participants:
        top_participants.sort(key=lambda x: int(str(x.get('point', 0) or 0)), reverse=True)
    
    top_participants = top_participants[:limit]


    return {
        "total_entries": total_entries if isinstance(total_entries, int) and total_entries > 0 else "-",
        "rank": rank,
        "point": point,
        "level": level,
        "top_participants": top_participants
    }
# --- イベント情報取得関数群ここまで ---


def display_room_status(profile_data, input_room_id):
    """取得したルームプロフィールデータとイベントデータを表示する"""
    
    # データを安全に取得
    room_name = _safe_get(profile_data, ["room_name"], "取得失敗")
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

    # タイトル
    st.markdown(f"## 🎤 <a href='{room_url}' target='_blank'>{room_name} ({input_room_id})</a> のルームステータス", unsafe_allow_html=True)
    
    # --- 📊 ルーム基本情報（第一カテゴリー） ---
    st.markdown("### 📊 ルーム基本情報")
    
    col1, col2 = st.columns(2)

    # Note: 取得データがint型か確認し、カンマ区切りを適用
    with col1:
        st.metric(label="ルームレベル", value=f"{room_level:,}" if isinstance(room_level, int) else str(room_level))
        st.metric(label="現在のSHOWランク", value=show_rank)
        st.metric(label="上位SHOWランクまでのスコア", value=f"{next_score:,}" if isinstance(next_score, int) else str(next_score))
        st.metric(label="下位SHOWランクまでのスコア", value=f"{prev_score:,}" if isinstance(prev_score, int) else str(prev_score))

    with col2:
        st.metric(label="フォロワー数", value=f"{follower_num:,}" if isinstance(follower_num, int) else str(follower_num))
        st.metric(label="まいにち配信（日数）", value=live_continuous_days)
        st.metric(label="公式 or フリー", value=official_status)
        st.metric(label="ジャンル", value=genre_name)

    st.divider()

    # --- 🏆 現在の参加イベント情報（第二カテゴリー） ---
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

        st.markdown(f"##### 🔗 <a href='{event_url}' target='_blank'>{event_name}</a>", unsafe_allow_html=True)

        # イベント期間の表示
        event_col_time1, event_col_time2 = st.columns(2)
        with event_col_time1:
            st.markdown(f"**イベント開始日時**")
            st.write(started_at_str)
        with event_col_time2:
            st.markdown(f"**イベント終了日時**")
            st.write(ended_at_str)

        # イベント参加情報（API取得）
        with st.spinner("イベント参加情報を取得中..."):
            event_info = get_event_participants_info(event_id, input_room_id, limit=10)
            
            total_entries = event_info["total_entries"]
            rank = event_info["rank"]
            point = event_info["point"]
            level = event_info["level"]
            top_participants = event_info["top_participants"]
            
            # イベント参加情報表示 (4カラムで横並び)
            event_col_data1, event_col_data2, event_col_data3, event_col_data4 = st.columns(4)
            with event_col_data1:
                st.metric(label="参加ルーム数", value=f"{total_entries:,}" if isinstance(total_entries, int) else str(total_entries))
            with event_col_data2:
                st.metric(label="順位", value=str(rank))
            with event_col_data3:
                st.metric(label="ポイント", value=f"{point:,}" if isinstance(point, int) else str(point))
            with event_col_data4:
                st.metric(label="レベル", value=str(level))

        st.divider()

        # --- 🔝 参加イベント上位10ルーム（Streamlitネイティブ表示） ---
        st.markdown("### 🔝 参加イベント上位10ルーム")
        
        if top_participants:
            dfp = pd.DataFrame(top_participants)
            
            # 必要なカラムが全て存在するように初期化
            cols = [
                'room_name', 'room_level', 'show_rank_subdivided', 'follower_num',
                'live_continuous_days', 'room_id', 'rank', 'point'
            ]
            for c in cols:
                if c not in dfp.columns:
                    dfp[c] = None

            dfp_display = dfp[cols].copy()

            # ▼ rename
            dfp_display.rename(columns={
                'room_name': 'ルーム名', 'room_level': 'Lv', 'show_rank_subdivided': 'ランク',
                'follower_num': 'Fw数', 'live_continuous_days': 'まいにち', 'room_id': 'ID',
                'rank': '順位', 'point': 'P'
            }, inplace=True)

            # ▼ 公/フ を追加
            dfp_display["公/フ"] = dfp_display["ID"].apply(get_official_mark)

            # ▼ 表示列順を整える (コンパクト表示)
            dfp_display = dfp_display[
                ['順位', 'P', 'ルーム名', 'ID', 'Lv', 'ランク', 'Fw数', 'まいにち', '公/フ']
            ]

            # --- ▼ 数値フォーマット関数 ▼ ---
            def _fmt_int_for_display(v, use_comma=True):
                try:
                    if v is None or (isinstance(v, (str, float)) and (v == "" or pd.isna(v))):
                        return "-"
                    num = int(v)
                    return f"{num:,}" if use_comma else f"{num}"
                except (ValueError, TypeError):
                    return str(v)

            # --- ▼ 列ごとにフォーマット適用 ▼ ---
            for col in ['Lv', 'Fw数', 'まいにち', '順位']:
                dfp_display[col] = dfp_display[col].apply(lambda x: _fmt_int_for_display(x, use_comma=False))
            dfp_display['P'] = dfp_display['P'].apply(lambda x: _fmt_int_for_display(x, use_comma=True))
            
            # ルーム名がNoneやNaNの場合は空文字列に変換
            dfp_display['ルーム名'] = dfp_display['ルーム名'].fillna('')
            dfp_display['ランク'] = dfp_display['ランク'].fillna('-')
            
            # Streamlitのネイティブなデータフレーム表示を使用
            with st.expander("参加ルーム一覧（ポイント順上位10ルーム）", expanded=True):
                # HTMLの崩れを回避するため、st.dataframe()を使用
                st.dataframe(dfp_display, 
                             hide_index=True, 
                             use_container_width=True) # 縦型考慮のため幅いっぱいに使用

        else:
            st.info("参加ルーム情報が取得できませんでした（ランキングイベントではない、またはデータがまだありません）。")

    else:
        st.info("現在、このルームはイベントに参加していません。")

# --- メインロジック ---

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