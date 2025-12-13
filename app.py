import streamlit as st
import requests
import pandas as pd
import io
import datetime
from dateutil import parser
import numpy as np

# Streamlit の初期設定
st.set_page_config(
    page_title="SHOWROOM ルームステータス可視化ツール",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- 定数設定 ---
# 認証用のルームリストURL
ROOM_LIST_URL = "https://mksoul-pro.com/showroom/file/room_list.csv"

# API URL
ROOM_PROFILE_API = "https://www.showroom-live.com/api/room/profile?room_id={room_id}"
EVENT_ROOM_LIST_API = "https://www.showroom-live.com/api/event/room_list?event_id={event_id}&p={page}"

# ジャンルIDと名称の対応
GENRE_MAP = {
    112: "ミュージック",
    102: "アイドル",
    103: "タレント",
    104: "声優",
    105: "芸人",
    107: "バーチャル",
    108: "モデル",
    109: "俳優",
    110: "アナウンサー",
    113: "クリエイター",
    200: "ライバー",
}

# --- ユーティリティ関数 ---

def _safe_get(data, keys, default_value=None):
    """ネストされた辞書から安全に値を取得するヘルパー関数"""
    temp = data
    for key in keys:
        if isinstance(temp, dict) and key in temp:
            temp = temp[key]
        else:
            return default_value
    # 空文字やNaNをデフォルト値に変換
    if temp is None or (isinstance(temp, (str, float)) and pd.isna(temp)):
        return default_value
    return temp

def get_official_mark(room_id):
    """ルームIDに基づいて 公/フ のマークを決定（※本来はAPIから取得すべき情報だが、ここでは暫定的にID範囲で判定）"""
    try:
        room_id = int(room_id)
        # IDが6桁未満のルームは公式の可能性が高い、など、簡易的な判定ロジックをここに置く
        # 正確な判定のためには、APIを叩く必要があります
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
    except requests.exceptions.RequestException as e:
        st.error(f"ルームID {room_id} のプロフィール情報を取得できませんでした: {e}")
        return None

def get_event_participants(event_info, limit=10, sort_by_point=False):
    """イベント参加ルーム情報・状況APIから参加ルームのリストを取得する"""
    event_id = event_info.get("event_id")
    if not event_id:
        return []

    participants = []
    page = 1
    # イベント開始日時が設定されているか確認
    started_at = event_info.get("started_at")
    current_time = datetime.datetime.now(datetime.timezone.utc).timestamp()
    event_is_active = started_at is not None and started_at < current_time

    if not event_is_active:
        return []

    while True:
        url = EVENT_ROOM_LIST_API.format(event_id=event_id, page=page)
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if "room_list" not in data or not data["room_list"]:
                break # room_listが空になったら終了

            participants.extend(data["room_list"])

            if len(participants) >= limit:
                participants = participants[:limit] # 上限まで取得
                break

            if data.get("next_page") is None:
                break # 次のページが無ければ終了

            page += 1

        except requests.exceptions.RequestException as e:
            st.warning(f"イベントID {event_id} の参加ルーム情報取得中にエラーが発生しました（Page {page}）: {e}")
            break

        if page > 10: # 無限ループ防止のためページ数の上限を設定
            break

    # 要件：イベントが開始されている場合は、ポイントの高い順にソートして表示
    if event_is_active and sort_by_point:
        participants.sort(key=lambda x: x.get('point', 0), reverse=True)

    return participants

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
    
    # ルーム情報（第一カテゴリー）
    st.markdown("### 📊 ルーム基本情報")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(label="ルームレベル", value=f"{room_level:,}" if isinstance(room_level, (int, float)) and room_level != "-" else str(room_level))
        st.metric(label="現在のSHOWランク", value=show_rank)
        st.metric(label="上位SHOWランクまでのスコア", value=f"{next_score:,}" if isinstance(next_score, (int, float)) and next_score != "-" else str(next_score))
        st.metric(label="下位SHOWランクまでのスコア", value=f"{prev_score:,}" if isinstance(prev_score, (int, float)) and prev_score != "-" else str(prev_score))

    with col2:
        st.metric(label="フォロワー数", value=f"{follower_num:,}" if isinstance(follower_num, (int, float)) and follower_num != "-" else str(follower_num))
        st.metric(label="まいにち配信（日数）", value=live_continuous_days)
        st.metric(label="公式 or フリー", value=official_status)
        st.metric(label="ジャンル", value=genre_name)

    st.divider()

    # イベント情報（第二カテゴリー）
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
                # タイムスタンプはUTC前提
                dt_utc = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
                # JSTに変換 (UTC+9)
                dt_jst = dt_utc.astimezone(datetime.timezone(datetime.timedelta(hours=9)))
                return dt_jst.strftime('%Y/%m/%d %H:%M')
            return "-"

        started_at_str = _ts_to_jst_str(started_at_ts)
        ended_at_str = _ts_to_jst_str(ended_at_ts)

        st.markdown(f"##### 🔗 <a href='{event_url}' target='_blank'>{event_name}</a>", unsafe_allow_html=True)

        event_col1, event_col2, event_col3, event_col4 = st.columns(4)

        with event_col1:
            st.markdown(f"**イベント開始日時**")
            st.write(started_at_str)
        with event_col2:
            st.markdown(f"**イベント終了日時**")
            st.write(ended_at_str)

        # イベント参加情報（API取得が必要）
        if event_id:
            with st.spinner("イベント参加情報を取得中..."):
                # イベントの全参加ルーム情報を取得
                participants_data = get_event_participants(event, limit=1000000) # まずは全件取得を試みる
                
                total_entries = len(participants_data)
                
                # 自身のルームIDの情報を探す
                current_room_data = next((r for r in participants_data if str(r.get("room_id")) == str(input_room_id)), None)

                rank = _safe_get(current_room_data, ["rank"], "-")
                point = _safe_get(current_room_data, ["point"], "-")
                level = _safe_get(current_room_data, ["quest_level"], "-")
                
                # イベント参加情報表示
                event_col5, event_col6, event_col7, event_col8 = st.columns(4)
                with event_col5:
                    st.metric(label="参加ルーム数", value=f"{total_entries:,}")
                with event_col6:
                    st.metric(label="順位", value=str(rank))
                with event_col7:
                    st.metric(label="ポイント", value=f"{point:,}" if isinstance(point, (int, float)) and point != "-" else str(point))
                with event_col8:
                    st.metric(label="レベル", value=str(level))

            st.divider()

            # プラスアルファ情報：参加イベント上位10ルーム
            st.markdown("### 🔝 参加イベント上位10ルーム")
            
            # イベント開始判定
            current_time_ts = datetime.datetime.now(datetime.timezone.utc).timestamp()
            event_active = started_at_ts and started_at_ts < current_time_ts

            if event_active:
                # イベント開催中の場合、ポイントの高い順にソートした上位10ルームを取得
                top_participants = get_event_participants(event, limit=10, sort_by_point=True)
            else:
                st.info("イベント開始前のため、参加ルーム一覧の表示はできません。")
                top_participants = []


            if top_participants:
                # DataFrame 化して列名を日本語化して表示（ルーム名はリンク付きで表示）
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

                # ▼ まず rename（必ず先！）
                dfp_display.rename(columns={
                    'room_name': 'ルーム名',
                    'room_level': 'ルームレベル',
                    'show_rank_subdivided': 'SHOWランク',
                    'follower_num': 'フォロワー数',
                    'live_continuous_days': 'まいにち配信',
                    'room_id': 'ルームID',
                    'rank': '順位',
                    'point': 'ポイント'
                }, inplace=True)

                # ▼ 次に 公/フ を追加（列名 ルームID が存在する状態で）
                dfp_display["公/フ"] = dfp_display["ルームID"].apply(get_official_mark)

                # ▼ 列順をここで整える（仕様通り）
                dfp_display = dfp_display[
                    ['ルーム名', 'ルームレベル', 'SHOWランク', 'フォロワー数',
                     'まいにち配信', '公/フ', 'ルームID', '順位', 'ポイント']
                ]

                # --- ▼ 数値フォーマット関数（カンマ区切りを切替可能） ▼ ---
                def _fmt_int_for_display(v, use_comma=True):
                    try:
                        if v is None or (isinstance(v, (str, float)) and (v == "" or pd.isna(v))):
                            return ""
                        num = int(v) # 整数に変換
                        # ✅ カンマ区切りあり or なしを切り替え
                        return f"{num:,}" if use_comma else f"{num}"
                    except Exception:
                        return str(v)

                # --- ▼ 列ごとにフォーマット適用（確実に順序反映） ▼ ---
                for col in dfp_display.columns:
                    # ✅ カンマ区切り「あり」列
                    if col == 'ポイント':
                        # apply内で_fmt_int_for_displayを呼び出す
                        dfp_display[col] = dfp_display[col].apply(lambda x: _fmt_int_for_display(x, use_comma=True))
                    # ✅ カンマ区切り「なし」列
                    elif col in ['ルームレベル', 'フォロワー数', 'まいにち配信', '順位']:
                        dfp_display[col] = dfp_display[col].apply(lambda x: _fmt_int_for_display(x, use_comma=False))

                # ルーム名をリンクにしてテーブル表示（HTMLテーブルを利用）
                def _make_link(row):
                    rid = row['ルームID']
                    name = row['ルーム名'] or f"room_{rid}"
                    # ルーム名の長さに応じて省略表示の検討はありますが、今回はシンプルなリンク表示で
                    return f'<a href="https://www.showroom-live.com/room/profile?room_id={rid}" target="_blank">{name}</a>'

                dfp_display['ルーム名'] = dfp_display.apply(_make_link, axis=1)

                # ルームIDカラムは表示上不要なため削除
                dfp_display = dfp_display.drop(columns=['ルームID'])

                # コンパクトに expander 内で表示（領域を占有しない）
                with st.expander("参加ルーム一覧（ポイント順上位10ルーム）", expanded=True):
                    # HTML表示を利用してリンクを有効化
                    st.write(dfp_display.to_html(escape=False, index=False, justify='left'), unsafe_allow_html=True)
            elif event_active:
                st.info("参加ルーム情報が取得できませんでした（イベント側データが空か、データの取得に失敗しました）。")

    else:
        st.info("現在、このルームはイベントに参加していません。")

# --- メインロジック ---

# セッションステートの初期化
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

# ▼▼ 認証ステップ ▼▼
if not st.session_state.authenticated:
    st.markdown("##### 🔑 認証コードを入力してください")
    input_room_id_auth = st.text_input(
        "認証コードを入力してください:",
        placeholder="",
        type="password",
        key="room_id_input_auth" # key名を変更して衝突を避ける
    )

    # 認証ボタン
    if st.button("認証する"):
        if input_room_id_auth:  # 入力が空でない場合のみ
            with st.spinner("認証中..."):
                try:
                    # 認証リストの取得
                    response = requests.get(ROOM_LIST_URL, timeout=5)
                    response.raise_for_status()
                    room_df = pd.read_csv(io.StringIO(response.text), header=None, dtype=str)

                    # 認証リスト（1列目）から有効なコードのセットを作成
                    # strip()で前後の空白を除去、dropna()で欠損値を除去
                    valid_codes = set(str(x).strip() for x in room_df.iloc[:, 0].dropna())

                    if input_room_id_auth.strip() in valid_codes:
                        st.session_state.authenticated = True
                        st.success("✅ 認証に成功しました。ツールを利用できます。")
                        st.rerun()  # 認証成功後に再読み込みしてメインUIへ
                    else:
                        st.error("❌ 認証コードが無効です。正しい認証コードを入力してください。")
                except Exception as e:
                    st.error(f"認証リストを取得できませんでした: {e}")
        else:
            st.warning("認証コードを入力してください。")

    # 認証が終わるまで他のUIを描画しない
    st.stop()
# ▲▲ 認証ステップここまで ▲▲


# --- 認証後メインUI ---

if st.session_state.authenticated:
    st.title("💖 SHOWROOM ルームステータス可視化ツール")
    st.sidebar.markdown("# 🔍 ルーム検索")

    # ルームIDの入力
    input_room_id = st.sidebar.text_input(
        "表示したいルームIDを入力してください:",
        placeholder="例: 496122",
        key="room_id_input_main" # key名を変更
    ).strip()

    if input_room_id:
        if input_room_id.isdigit():
            # APIからの情報取得
            with st.spinner(f"ルームID {input_room_id} の情報を取得中..."):
                room_profile = get_room_profile(input_room_id)
            
            if room_profile:
                # 情報の表示
                display_room_status(room_profile, input_room_id)
            else:
                st.error(f"ルームID {input_room_id} の情報を取得できませんでした。IDを確認してください。")
        else:
            st.warning("ルームIDは数字で入力してください。")

    # 認証解除ボタン（任意）
    if st.sidebar.button("認証を解除する"):
        st.session_state.authenticated = False
        st.rerun()