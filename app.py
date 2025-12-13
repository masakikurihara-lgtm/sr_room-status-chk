import streamlit as st
import requests
import pandas as pd
import io
import datetime
from dateutil import parser
import numpy as np
import re
import json 

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
    # 取得した値がNone、空の文字列、またはNaNの場合もデフォルト値を返す
    if temp is None or (isinstance(temp, str) and temp.strip() == "") or (isinstance(temp, float) and pd.isna(temp)):
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

# --- イベント情報取得関数群 ---

def get_total_entries(event_id):
    """イベント参加者総数を取得する（これはページネーションの必要なし）"""
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
    """
    全参加者リストを取得する。（ページネーション対応を強化）
    """
    all_rooms = []
    page = 1 # ページカウンター ('p' パラメーターの値)
    count = 50 # 1ページあたりの取得件数（標準的な値）
    max_pages = 200 # 無限ループ防止のため最大ページ数を設定（イベント参加者が1万人の場合でもカバー）
    
    while page <= max_pages:
        params = {"event_id": event_id, "p": page, "count": count}
        try:
            # ページごとにAPIをリクエスト
            resp = requests.get(API_EVENT_ROOM_LIST_URL, headers=HEADERS, params=params, timeout=15)
            
            if resp.status_code == 404:
                # 404エラーの場合はイベントIDが存在しないか終了している
                break
            
            resp.raise_for_status()
            data = resp.json()
            
            current_page_rooms = []
            
            # APIレスポンスからリストデータを抽出
            if isinstance(data, dict):
                # 複数のキー名からルームリストを取得するロジックは維持
                for k in ('list', 'room_list', 'event_entry_list', 'entries', 'data', 'event_list'):
                    if k in data and isinstance(data[k], list):
                        current_page_rooms = data[k]
                        break
            elif isinstance(data, list):
                current_page_rooms = data
                
            if not current_page_rooms:
                # リストが空であれば、これ以上データがないと判断してループ終了
                break

            all_rooms.extend(current_page_rooms)
            
            # 取得数がページあたりの件数（count）より少なければ最終ページと判断
            if len(current_page_rooms) < count:
                break
            
            page += 1 # 次のページへ

        except Exception:
            # ネットワークエラーなどで中断
            break
            
    return all_rooms

def get_event_participants_info(event_id, target_room_id, limit=10):
    """
    イベント参加ルーム情報・状況APIから必要な情報を抽出する。
    ターゲットルームの順位、ポイント、レベルを確実に取得する。（検索ロジックを最終強化）
    """
    # ターゲットルームIDを文字列に統一
    target_room_id_str = str(target_room_id).strip()
    
    if not event_id:
        return {"total_entries": "-", "rank": "-", "point": "-", "level": "-", "top_participants": []}

    # 全参加者リストを取得（2ページ目以降も含む）
    room_list_data = get_event_room_list_data(event_id)
    total_entries = get_total_entries(event_id)
    current_room_data = None
    
    # --- 🎯 ターゲットルームの情報をリスト全体から確実に探す (検索ロジック最終強化) ---
    # 【★この部分を強化★】
    # room_list_data の全要素を走査し、room_id が一致するものを探す
    for room in room_list_data:
        # APIのレスポンスには room_id が文字列または数値で入る可能性があるため、
        # str() でキャストして比較する
        room_id_in_list = room.get("room_id")
        
        if room_id_in_list is not None and str(room_id_in_list).strip() == target_room_id_str:
            current_room_data = room
            # 見つけたらすぐにループを抜ける（このデータを使用する）
            break
    # ------------------------------------------------------------------------------------


    # --- 🎯 ターゲットルームの参加状況を確定 ---
    rank = None
    point = None
    level = None
    
    if current_room_data:
        # _safe_get を使用して安全に値を取得
        rank = _safe_get(current_room_data, ["rank"], default_value=None)
        
        point = _safe_get(current_room_data, ["point"], default_value=None)
        if point is None:
             point = _safe_get(current_room_data, ["score"], default_value=None)
        
        level = _safe_get(current_room_data, ["event_entry", "quest_level"], default_value=None)
        if level is None:
             level = _safe_get(current_room_data, ["entry_level"], default_value=None)
        if level is None:
             level = _safe_get(current_room_data, ["event_entry", "level"], default_value=None)
    
    # 取得結果の None を表示用のハイフンに変換 (0や有効な値はそのまま残る)
    # ※この段階ではまだ数値型を維持している可能性がある
    rank = "-" if rank is None else rank
    point = "-" if point is None else point
    level = "-" if level is None else level
    # ------------------------------------------------------------------------------------

    # --- 上位10ルームのリストを作成し、エンリッチメント処理に進む ---
    top_participants = room_list_data
    if top_participants:
        # point/score は文字列またはNoneの可能性があるため、intにキャストしてソート
        # 安全のため、変換できない場合は0として扱う
        def safe_int_for_sort(x):
            try:
                return int(str(x.get('point', x.get('score', 0)) or 0))
            except:
                return 0
            
        top_participants.sort(key=safe_int_for_sort, reverse=True)
    
    # ここでリストを上位10件に制限する
    top_participants = top_participants[:limit]


    # ✅ 上位10ルームのプロフィール情報を取得し、データをエンリッチ（統合）
    enriched_participants = []
    for participant in top_participants:
        room_id = participant.get('room_id')
        
        # 取得必須のキーを初期化（Noneで初期化）
        for key in ['room_level_profile', 'show_rank_subdivided', 'follower_num', 'live_continuous_days', 'is_official_api']: 
            participant[key] = None
            
        if room_id:
            profile = get_room_profile(room_id)
            if profile:
                # プロフィールAPIから取得した「ルームレベル」を 'room_level_profile' として格納
                participant['room_level_profile'] = _safe_get(profile, ["room_level"], None)
                participant['show_rank_subdivided'] = _safe_get(profile, ["show_rank_subdivided"], None)
                participant['follower_num'] = _safe_get(profile, ["follower_num"], None)
                participant['live_continuous_days'] = _safe_get(profile, ["live_continuous_days"], None)
                participant['is_official_api'] = _safe_get(profile, ["is_official"], None)
                
                if not participant.get('room_name'):
                     participant['room_name'] = _safe_get(profile, ["room_name"], f"Room {room_id}")
        
        # イベントの「レベル」を取得 ('event_entry.quest_level' またはその他のキーから)
        participant['quest_level'] = _safe_get(participant, ["event_entry", "quest_level"], None)
        if participant['quest_level'] is None:
             participant['quest_level'] = _safe_get(participant, ["entry_level"], None)
        if participant['quest_level'] is None:
             participant['quest_level'] = _safe_get(participant, ["event_entry", "level"], None)

        # 最終的に quest_level がセットされていない場合、ここでキーを追加（DataFrame化でエラーが出ないように）
        if 'quest_level' not in participant:
             participant['quest_level'] = None

        enriched_participants.append(participant)

    # 応答に必要な情報を返す
    return {
        "total_entries": total_entries if isinstance(total_entries, int) and total_entries > 0 else "-",
        "rank": rank,
        "point": point,
        "level": level, # ターゲットルームのレベル
        "top_participants": enriched_participants, # エンリッチされたリストを返す
    }
# --- イベント情報取得関数群ここまで ---


def display_room_status(profile_data, input_room_id):
    """取得したルームプロフィールデータとイベントデータを表示する"""
    
    # データを安全に取得
    room_name = _safe_get(profile_data, ["room_name"], "取得失敗")
    room_level = _safe_get(profile_data, ["room_level"], "-") # これはプロフィールのルームレベル
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
    
    
    # --- 💡 カスタムCSSの定義 ---
    custom_styles = """
    <style>
    /* 全体のフォント統一と余白調整 */
    h3 { 
        margin-top: 20px; 
        padding-top: 10px; 
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
        border-collapse: collapse;
        margin-top: 10px; 
        width: 100%; /* 親要素の幅を使う */
        max-width: 1000px; /* テーブルの最大幅を制限 (調整可能) */
        min-width: 800px; /* 最小幅を設定 */
    }
    
    /* 中央寄せラッパー (テーブル全体を中央に配置) */
    .center-table-wrapper {
        display: flex;
        justify-content: center; /* 子要素（テーブル）を水平方向の中央に配置 */
        width: 100%;
        overflow-x: auto;
        text-align: center !important;
    }

    .stHtml .dataframe th {
        background-color: #e8eaf6; 
        color: #1a237e; 
        font-weight: bold;
        padding: 8px 10px; 
        font-size: 14px;
        /* ヘッダーのデフォルトは中央寄せを維持 */
        text-align: center !important; 
        white-space: nowrap;
    }
    .stHtml .dataframe td {
        padding: 6px 10px; 
        font-size: 13px; 
        line-height: 1.4;
        border-bottom: 1px solid #f0f0f0;
        /* データのデフォルトは中央寄せを維持 */
        text-align: center !important; 
        white-space: nowrap; 
    }
    .stHtml .dataframe tbody tr:hover {
        background-color: #f7f9fd; 
    }

    /* 列ごとの配置調整 (10列のインデックス調整) */
    
    /* 1. ルーム名: 中央寄せ */
    .stHtml .dataframe th:nth-child(1), .stHtml .dataframe td:nth-child(1) {
        text-align: center !important; /* 強制中央せ */
        min-width: 280px; 
        white-space: normal !important; 
    }
    
    /* 数値系の列を中央寄せに統一 */
    .stHtml .dataframe th:nth-child(2), .stHtml .dataframe td:nth-child(2), /* ルームレベル */
    .stHtml .dataframe th:nth-child(4), .stHtml .dataframe td:nth-child(4), /* フォロワー数 */
    .stHtml .dataframe th:nth-child(5), .stHtml .dataframe td:nth-child(5), /* まいにち配信 */
    .stHtml .dataframe th:nth-child(9), .stHtml .dataframe td:nth-child(9) { /* ポイント */
        text-align: center !important; /* 中央右寄せ */
        width: 10%; 
    }

    /* 中央寄せを維持しつつ幅調整 (ランク、公式 or フリー、ルームID、順位、レベル) */
    .stHtml .dataframe th:nth-child(3), .stHtml .dataframe td:nth-child(3), /* ランク */
    .stHtml .dataframe th:nth-child(6), .stHtml .dataframe td:nth-child(6), /* 公式 or フリー */
    .stHtml .dataframe th:nth-child(7), .stHtml .dataframe td:nth-child(7), /* ルームID */
    .stHtml .dataframe th:nth-child(8), .stHtml .dataframe td:nth-child(8), /* 順位 */
    .stHtml .dataframe th:nth-child(10), .stHtml .dataframe td:nth-child(10) { /* レベル (最終列) */
        text-align: center !important; /* 強制中央寄せ */
        width: 8%;
    }
    
    /* '公式 or フリー' の強調 */
    .stHtml .dataframe th:nth-child(6), .stHtml .dataframe td:nth-child(6) {
        font-weight: bold;
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
    
    # --- 2. 📊 ルーム基本情報（第一カテゴリー） ---
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
            # 修正後の関数を呼び出し
            event_info = get_event_participants_info(event_id, input_room_id, limit=10)
            
            total_entries = event_info["total_entries"]
            rank = event_info["rank"]
            point = event_info["point"]
            level = event_info["level"] # ターゲットルームのレベル
            
            # イベント参加情報表示 (4カラムで横並び) - st.metric を使用
            st.markdown("#### 参加状況（自己ルーム）")
            event_col_data1, event_col_data2, event_col_data3, event_col_data4 = st.columns([1, 1, 1, 1])
            with event_col_data1:
                st.metric(label="参加ルーム数", value=f"{total_entries:,}" if isinstance(total_entries, int) else str(total_entries), delta_color="off")
            with event_col_data2:
                # 順位は確定した値を使用
                rank_display = str(rank)
                # ハイフンでなければ数値としてカンマ区切りに変換
                if rank != '-':
                    try:
                        # 整数に変換できるか試す
                        rank_display = f"{int(rank):,}"
                    except (ValueError, TypeError):
                        # 変換できなければ元の文字列表示
                        pass
                st.metric(label="現在の順位", value=rank_display, delta_color="off")

            with event_col_data3:
                # 獲得ポイントは確定した値を使用
                point_display = str(point)
                # ハイフンでなければ数値としてカンマ区切りに変換
                if point != '-':
                    try:
                        point_display = f"{int(point):,}"
                    except (ValueError, TypeError):
                        pass
                st.metric(label="獲得ポイント", value=point_display, delta_color="off")

            with event_col_data4:
                # レベルは確定した値を使用
                level_display = str(level)
                # ハイフンでなければ数値としてカンマ区切りに変換
                if level != '-':
                    try:
                        level_display = f"{int(level):,}"
                    except (ValueError, TypeError):
                        pass
                st.metric(label="レベル", value=level_display, delta_color="off")
            
            top_participants = event_info["top_participants"]


        st.divider()

        # --- 4. 🔝 参加イベント上位10ルーム（HTMLテーブル） ---
        st.markdown("### 🔝 参加イベント上位10ルーム")
        
        if top_participants:
            
            dfp = pd.DataFrame(top_participants)

            # 必要なカラムが全て存在することを確認
            cols = [
                'room_name', 'room_level_profile', 'show_rank_subdivided', 'follower_num',
                'live_continuous_days', 'room_id', 'rank', 'point',
                'is_official_api', 'quest_level' # quest_levelを含む
            ]
            
            # DataFrameに欠損しているカラムをNoneで埋める
            for c in cols:
                if c not in dfp.columns:
                    dfp[c] = None
                    
            dfp_display = dfp[cols].copy()

            # ▼ rename
            dfp_display.rename(columns={
                'room_name': 'ルーム名', 
                'room_level_profile': 'ルームレベル', 
                'show_rank_subdivided': 'ランク',
                'follower_num': 'フォロワー数', 
                'live_continuous_days': 'まいにち配信', 
                'room_id': 'ルームID', 
                'rank': '順位', 
                'point': 'ポイント',
                'is_official_api': 'is_official_api',
                'quest_level': 'レベル' 
            }, inplace=True)

            # ▼ 公式 or フリー 判定関数（API情報使用）
            def get_official_status_from_api(is_official_value):
                """APIのis_official値に基づいて「公式」または「フリー」を判定する"""
                if is_official_value is True:
                    return "公式"
                elif is_official_value is False:
                    return "フリー"
                else:
                    return "不明"
            
            # ▼ 公式 or フリー を追加
            dfp_display["公式 or フリー"] = dfp_display['is_official_api'].apply(get_official_status_from_api)
            
            dfp_display.drop(columns=['is_official_api'], inplace=True, errors='ignore')


            # --- ▼ 数値フォーマット関数（カンマ区切りを切替可能） ▼ ---
            def _fmt_int_for_display(v, use_comma=True):
                """
                数値を整形する。None, NaN, 空文字列、ハイフン以外の '-' の場合はハイフンを返す。
                """
                try:
                    # None, NaN, 空文字列の場合はハイフンを返す
                    if v is None or (isinstance(v, (str, float)) and (str(v).strip() == "" or pd.isna(v) or str(v).strip() == '-')):
                        return "-"
                    
                    # 数値に変換できるか試す
                    num = float(v)
                    
                    if use_comma:
                        return f"{int(num):,}"
                    else:
                        return f"{int(num)}"
                        
                except Exception:
                    # 変換エラーが発生した場合、元の値を文字列として返す（またはハイフン）
                    return str(v) if str(v).strip() != "" else "-"

            # --- ▼ 列ごとにフォーマット適用 ▼ ---
            # 'ルームレベル'、'フォロワー数'、'まいにち配信'、'順位'、'ルームID' はカンマなし
            format_cols_no_comma = ['ルームレベル', 'フォロワー数', 'まいにち配信', '順位', 'ルームID'] 
            # 'ポイント' はカンマあり
            format_cols_comma = ['ポイント']

            for col in format_cols_comma:
                if col in dfp_display.columns:
                    dfp_display[col] = dfp_display[col].apply(lambda x: _fmt_int_for_display(x, use_comma=True))
            
            for col in format_cols_no_comma:
                if col in dfp_display.columns:
                    dfp_display[col] = dfp_display[col].apply(lambda x: _fmt_int_for_display(x, use_comma=False))
            

            # 🔥 「レベル」列のフォーマット処理 (数値型として取得できなかった場合を考慮)
            def format_level_safely_FINAL(val):
                """APIの値(val)を安全にレベル表示用文字列に変換する"""
                if val is None or pd.isna(val) or str(val).strip() == "" or val is False or (isinstance(val, (list, tuple)) and not val):
                    return "-"
                else:
                    try:
                        # 整数に変換可能であれば整数として表示
                        return str(int(val))
                    except (ValueError, TypeError):
                        # 変換できなければ文字列をそのまま返す（またはハイフン）
                        return str(val) if str(val).strip() != "" else "-"

            if 'レベル' in dfp_display.columns:
                dfp_display['レベル'] = dfp_display['レベル'].apply(format_level_safely_FINAL)
            
            
            # 最終的な欠損値/空文字列のハイフン化（主にランクなど数値フォーマットを通らない文字列列用）
            for col in ['ランク']: 
                if col in dfp_display.columns:
                    # None, NaN, 空文字列、ハイフン以外の '-' を含む場合はハイフンに変換
                    dfp_display[col] = dfp_display[col].apply(lambda x: '-' if x is None or x == '' or pd.isna(x) or str(x).strip() == '-' else x)


            # --- ルーム名をリンクに置き換える ---
            def _make_link_final(row):
                rid = row['ルームID'] 
                name = row['ルーム名']
                if not name:
                    name = f"room_{rid}"
                
                # ルームIDがハイフンでない、つまり有効な値の場合のみリンクを生成
                if rid != '-':
                    return f'<a href="https://www.showroom-live.com/room/profile?room_id={rid}" target="_blank">{name}</a>'
                return name

            # リンクを生成し、dfp_displayの'ルーム名'列を上書き
            dfp_display['ルーム名'] = dfp_display.apply(_make_link_final, axis=1)
            
            # ▼ 列順をここで整える
            dfp_display = dfp_display[
                ['ルーム名', 'ルームレベル', 'ランク', 'フォロワー数',
                 'まいにち配信', '公式 or フリー', 'ルームID', '順位', 'ポイント', 'レベル'] 
            ]
            
            # コンパクトに expander 内で表示
            with st.expander("参加ルーム一覧（ポイント順上位10ルーム）", expanded=True):
                
                html_table = dfp_display.to_html(
                    escape=False, 
                    index=False, 
                    # 外部CSSで制御するため justify は削除 (元のコードの意図を尊重)
                    classes='dataframe data-table data-table-full-width' 
                )
                
                # HTMLを整形（改行や余分な空白を除去し、HTMLのサイズを小さくする）
                html_table = html_table.replace('\n', '')
                html_table = re.sub(r'>\s+<', '><', html_table)
                
                # テーブル全体を 'center-table-wrapper' でラップする
                centered_html = f'<div class="center-table-wrapper">{html_table}</div>'

                # HTMLテーブルを直接 st.markdown で出力
                st.markdown(centered_html, unsafe_allow_html=True)

        else:
            st.info("参加ルーム情報が取得できませんでした（ランキングイベントではない、またはデータがまだありません）。")

    else:
        st.info("現在、このルームはイベントに参加していません。")


# --- メインロジック ---
# st.session_stateの初期化 (認証機能のために必須)
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'show_status' not in st.session_state:
    st.session_state.show_status = False
if 'input_room_id' not in st.session_state:
    st.session_state.input_room_id = ""


if not st.session_state.authenticated:
    st.title("💖 SHOWROOM ルームステータス可視化ツール")
    st.markdown("##### 🔑 認証コードを入力してください")
    input_auth_code = st.text_input(
        "認証コードを入力してください:",
        placeholder="認証コード",
        type="password",
        key="room_id_input_auth"
    )
    if st.button("認証する"):
        if input_auth_code:
            with st.spinner("認証中..."):
                try:
                    response = requests.get(ROOM_LIST_URL, timeout=5)
                    response.raise_for_status()
                    # 認証コードリストの取得と検証ロジックを維持
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
        
    if st.button("ルームステータスを表示"):
        if st.session_state.input_room_id and st.session_state.input_room_id.isdigit():
            st.session_state.show_status = True
        elif st.session_state.input_room_id:
            st.error("ルームIDは数字で入力してください。")
        else:
            st.warning("ルームIDを入力してください。")
            
    st.divider()
    
    if st.session_state.show_status and st.session_state.input_room_id:
        with st.spinner(f"ルームID {st.session_state.input_room_id} の情報を取得中..."):
            room_profile = get_room_profile(st.session_state.input_room_id)
        if room_profile:
            # display_room_status 関数を呼び出し
            display_room_status(room_profile, st.session_state.input_room_id)
        else:
            st.error(f"ルームID {st.session_state.input_room_id} の情報を取得できませんでした。IDを確認してください。")
            
    st.markdown("---")
    
    if st.button("認証を解除する", help="認証状態をリセットし、認証コード入力画面に戻ります"):
        st.session_state.authenticated = False
        st.session_state.show_status = False
        st.session_state.input_room_id = ""
        st.rerun()