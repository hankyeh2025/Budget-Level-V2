"""
Budget Level v2.1 - 心理帳戶管理系統
使用信封袋理財法概念，管理心理帳戶
v2.1: 新增 Wallet_Log, Period, Bank_Account sheets
"""

import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

# =============================================================================
# 常數定義
# =============================================================================

# 台灣時區
TAIWAN_TZ = ZoneInfo("Asia/Taipei")


def get_taiwan_now() -> datetime:
    """取得台灣時間"""
    return datetime.now(TAIWAN_TZ)


def get_taiwan_today() -> date:
    """取得台灣日期"""
    return datetime.now(TAIWAN_TZ).date()


# 四個心理帳戶 (v2.1: Investing 移除)
ACCOUNT_LIVING = "Living"
ACCOUNT_SAVING = "Saving"
ACCOUNT_BACKUP = "Back_Up"
ACCOUNT_FREEFUND = "Free_Fund"

# Wallet Log Types (v2.1 新增)
WALLET_INCOME = "Income"
WALLET_ALLOCATE_OUT = "Allocate_Out"
WALLET_TRANSFER_IN = "Transfer_In"
WALLET_ADJUSTMENT = "Adjustment"

# Transaction Types (v2.1 簡化)
TYPE_EXPENSE = "Expense"
TYPE_SAVING_IN = "Saving_In"
TYPE_SAVING_OUT = "Saving_Out"
TYPE_SETTLEMENT_IN = "Settlement_In"
TYPE_SETTLEMENT_OUT = "Settlement_Out"
TYPE_TRANSFER = "Transfer"

# Payment Methods (v2.1 新增)
PAYMENT_CREDIT = "Credit"
PAYMENT_DIRECT = "Direct"

# Period Status (v2.1 新增)
PERIOD_ACTIVE = "Active"
PERIOD_SETTLED = "Settled"

# Sheet 名稱 (v2.1: 9 sheets)
SHEET_BANK_ACCOUNT = "Bank_Account"
SHEET_WALLET_LOG = "Wallet_Log"
SHEET_PERIOD = "Period"
SHEET_CATEGORY = "Category"
SHEET_SUB_TAG = "Sub_Tag"
SHEET_SAVING_GOAL = "Saving_Goal"
SHEET_TRANSACTION = "Transaction"
SHEET_SETTLEMENT_LOG = "Settlement_Log"
SHEET_CONFIG = "Config"

# Google Sheets API Scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# =============================================================================
# Google Sheets 連線
# =============================================================================

@st.cache_resource
def get_gspread_client():
    """建立 Google Sheets 連線（永久快取）"""
    try:
        credentials = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=SCOPES
        )
        client = gspread.authorize(credentials)
        return client
    except Exception as e:
        st.error(f"無法連線到 Google Sheets: {e}")
        return None


@st.cache_resource
def get_spreadsheet():
    """取得 Spreadsheet 物件"""
    client = get_gspread_client()
    if client is None:
        return None
    try:
        spreadsheet = client.open_by_key(st.secrets["spreadsheet_id"])
        return spreadsheet
    except Exception as e:
        st.error(f"無法開啟試算表: {e}")
        return None


# =============================================================================
# 資料存取層 - 讀取
# =============================================================================

@st.cache_data(ttl=60)
def load_all_data() -> dict:
    """一次載入所有 9 張 sheet 資料（減少 API 呼叫）"""
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return {
            "bank_accounts": pd.DataFrame(),
            "wallet_log": pd.DataFrame(),
            "periods": pd.DataFrame(),
            "categories": pd.DataFrame(),
            "sub_tags": pd.DataFrame(),
            "saving_goals": pd.DataFrame(),
            "transactions": pd.DataFrame(),
            "settlement_log": pd.DataFrame(),
            "config": {}
        }

    try:
        data = {}

        # Bank_Account
        try:
            ws = spreadsheet.worksheet(SHEET_BANK_ACCOUNT)
            data["bank_accounts"] = pd.DataFrame(ws.get_all_records())
        except gspread.exceptions.WorksheetNotFound:
            data["bank_accounts"] = pd.DataFrame()

        # Wallet_Log
        try:
            ws = spreadsheet.worksheet(SHEET_WALLET_LOG)
            df = pd.DataFrame(ws.get_all_records())
            if not df.empty and "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            data["wallet_log"] = df
        except gspread.exceptions.WorksheetNotFound:
            data["wallet_log"] = pd.DataFrame()

        # Period
        try:
            ws = spreadsheet.worksheet(SHEET_PERIOD)
            df = pd.DataFrame(ws.get_all_records())
            if not df.empty:
                if "Start_Date" in df.columns:
                    df["Start_Date"] = pd.to_datetime(df["Start_Date"], errors="coerce")
                if "End_Date" in df.columns:
                    df["End_Date"] = pd.to_datetime(df["End_Date"], errors="coerce")
            data["periods"] = df
        except gspread.exceptions.WorksheetNotFound:
            data["periods"] = pd.DataFrame()

        # Category
        try:
            ws = spreadsheet.worksheet(SHEET_CATEGORY)
            data["categories"] = pd.DataFrame(ws.get_all_records())
        except gspread.exceptions.WorksheetNotFound:
            data["categories"] = pd.DataFrame()

        # Sub_Tag
        try:
            ws = spreadsheet.worksheet(SHEET_SUB_TAG)
            data["sub_tags"] = pd.DataFrame(ws.get_all_records())
        except gspread.exceptions.WorksheetNotFound:
            data["sub_tags"] = pd.DataFrame()

        # Saving_Goal
        try:
            ws = spreadsheet.worksheet(SHEET_SAVING_GOAL)
            data["saving_goals"] = pd.DataFrame(ws.get_all_records())
        except gspread.exceptions.WorksheetNotFound:
            data["saving_goals"] = pd.DataFrame()

        # Transaction
        try:
            ws = spreadsheet.worksheet(SHEET_TRANSACTION)
            df = pd.DataFrame(ws.get_all_records())
            if not df.empty and "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            data["transactions"] = df
        except gspread.exceptions.WorksheetNotFound:
            data["transactions"] = pd.DataFrame()

        # Settlement_Log
        try:
            ws = spreadsheet.worksheet(SHEET_SETTLEMENT_LOG)
            data["settlement_log"] = pd.DataFrame(ws.get_all_records())
        except gspread.exceptions.WorksheetNotFound:
            data["settlement_log"] = pd.DataFrame()

        # Config
        try:
            ws = spreadsheet.worksheet(SHEET_CONFIG)
            config_data = ws.get_all_records()
            data["config"] = {row["Key"]: row["Value"] for row in config_data if "Key" in row}
        except gspread.exceptions.WorksheetNotFound:
            data["config"] = {}

        return data

    except Exception as e:
        st.error(f"載入資料失敗: {e}")
        return {
            "bank_accounts": pd.DataFrame(),
            "wallet_log": pd.DataFrame(),
            "periods": pd.DataFrame(),
            "categories": pd.DataFrame(),
            "sub_tags": pd.DataFrame(),
            "saving_goals": pd.DataFrame(),
            "transactions": pd.DataFrame(),
            "settlement_log": pd.DataFrame(),
            "config": {}
        }


def load_bank_accounts() -> pd.DataFrame:
    """載入銀行帳戶"""
    return load_all_data()["bank_accounts"]


def load_wallet_log() -> pd.DataFrame:
    """載入錢包記錄"""
    return load_all_data()["wallet_log"]


def load_periods() -> pd.DataFrame:
    """載入週期資料"""
    return load_all_data()["periods"]


def load_categories() -> pd.DataFrame:
    """載入 Living 科目"""
    return load_all_data()["categories"]


def load_sub_tags() -> pd.DataFrame:
    """載入科目子類"""
    return load_all_data()["sub_tags"]


def load_saving_goals() -> pd.DataFrame:
    """載入儲蓄目標"""
    return load_all_data()["saving_goals"]


def load_transactions() -> pd.DataFrame:
    """載入所有交易記錄"""
    return load_all_data()["transactions"]


def load_settlement_log() -> pd.DataFrame:
    """載入結算記錄"""
    return load_all_data()["settlement_log"]


def load_config() -> dict:
    """載入系統設定"""
    return load_all_data()["config"]


# =============================================================================
# 資料存取層 - 寫入
# =============================================================================

def add_wallet_log(
    log_type: str,
    amount: float,
    bank_id: str = "",
    note: str = "",
    ref: str = ""
) -> bool:
    """
    新增錢包記錄

    Args:
        log_type: WALLET_INCOME, WALLET_ALLOCATE_OUT, WALLET_TRANSFER_IN, WALLET_ADJUSTMENT
        amount: 金額
        bank_id: 銀行帳戶 ID（選填）
        note: 備註（選填）
        ref: 關聯參考（選填）

    Returns:
        bool: 是否成功
    """
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return False

    try:
        worksheet = spreadsheet.worksheet(SHEET_WALLET_LOG)

        # 產生 Log_ID (WL + timestamp)
        log_id = f"WL{get_taiwan_now().strftime('%Y%m%d%H%M%S')}"

        # 確保 amount 是 Python 原生類型
        amount = float(amount)

        # 欄位順序：Log_ID | Timestamp | Date | Type | Amount | Bank_ID | Note | Ref
        row = [
            log_id,                                          # Log_ID
            get_taiwan_now().strftime("%Y-%m-%d %H:%M:%S"),  # Timestamp
            get_taiwan_now().strftime("%Y-%m-%d"),           # Date
            log_type,                                        # Type
            amount,                                          # Amount
            bank_id,                                         # Bank_ID
            note,                                            # Note
            ref                                              # Ref
        ]

        worksheet.append_row(row, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return True

    except Exception as e:
        st.error(f"新增錢包記錄失敗: {e}")
        return False


def add_period(
    start_date: date,
    end_date: date,
    living_budget: float
) -> str:
    """
    新增預算週期

    Args:
        start_date: 開始日期
        end_date: 結束日期
        living_budget: Living 預算

    Returns:
        str: Period_ID，失敗時回傳空字串
    """
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return ""

    try:
        worksheet = spreadsheet.worksheet(SHEET_PERIOD)

        # 產生 Period_ID (PER + timestamp)
        period_id = f"PER{get_taiwan_now().strftime('%Y%m%d%H%M%S')}"

        # 確保 living_budget 是 Python 原生類型
        living_budget = float(living_budget)

        # 欄位順序：Period_ID | Start_Date | End_Date | Status | Living_Budget | Settled_At
        row = [
            period_id,                              # Period_ID
            start_date.strftime("%Y-%m-%d"),        # Start_Date
            end_date.strftime("%Y-%m-%d"),          # End_Date
            PERIOD_ACTIVE,                          # Status
            living_budget,                          # Living_Budget
            ""                                      # Settled_At (空)
        ]

        worksheet.append_row(row, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return period_id

    except Exception as e:
        st.error(f"新增週期失敗: {e}")
        return ""


def add_bank_account(
    name: str,
    note: str = ""
) -> bool:
    """
    新增銀行帳戶

    Args:
        name: 帳戶名稱
        note: 備註（選填）

    Returns:
        bool: 是否成功
    """
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return False

    try:
        worksheet = spreadsheet.worksheet(SHEET_BANK_ACCOUNT)

        # 產生 Bank_ID (BANK + timestamp)
        bank_id = f"BANK{get_taiwan_now().strftime('%Y%m%d%H%M%S')}"

        # 欄位順序：Bank_ID | Name | Note | Status
        row = [
            bank_id,    # Bank_ID
            name,       # Name
            note,       # Note
            "Active"    # Status
        ]

        worksheet.append_row(row, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return True

    except Exception as e:
        st.error(f"新增銀行帳戶失敗: {e}")
        return False


def add_transaction(
    trans_type: str,
    amount: float,
    account: str,
    category_id: str = "",
    sub_tag_id: str = "",
    item: str = "",
    note: str = "",
    goal_id: str = "",
    target_account: str = "",
    ref: str = "",
    period_id: str = "",
    bank_id: str = "",
    payment_method: str = ""
) -> bool:
    """
    新增交易記錄 (v2.1 新增 Period_ID, Bank_ID, Payment_Method)
    """
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return False

    try:
        worksheet = spreadsheet.worksheet(SHEET_TRANSACTION)

        # 產生交易 ID
        trans_id = f"TXN{get_taiwan_now().strftime('%Y%m%d%H%M%S')}"

        # 確保 amount 是 Python 原生類型
        amount = float(amount)

        # 欄位順序 (v2.1):
        # Txn_ID | Timestamp | Date | Type | Amount | Account | Category_ID | Sub_Tag_ID |
        # Goal_ID | Target_Account | Item | Note | Ref | Period_ID | Bank_ID | Payment_Method
        row = [
            trans_id,                                        # Txn_ID
            get_taiwan_now().strftime("%Y-%m-%d %H:%M:%S"),  # Timestamp
            get_taiwan_now().strftime("%Y-%m-%d"),           # Date
            trans_type,                                      # Type
            amount,                                          # Amount
            account,                                         # Account
            category_id,                                     # Category_ID
            sub_tag_id,                                      # Sub_Tag_ID
            goal_id,                                         # Goal_ID
            target_account,                                  # Target_Account
            item,                                            # Item
            note,                                            # Note
            ref,                                             # Ref
            period_id,                                       # Period_ID (v2.1 新增)
            bank_id,                                         # Bank_ID (v2.1 新增)
            payment_method                                   # Payment_Method (v2.1 新增)
        ]

        worksheet.append_row(row, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return True

    except Exception as e:
        st.error(f"新增交易失敗: {e}")
        return False


# =============================================================================
# 工具函式
# =============================================================================

def get_active_period() -> Optional[pd.Series]:
    """取得當前活躍的 Period"""
    periods = load_periods()
    if periods.empty:
        return None

    active = periods[periods["Status"] == PERIOD_ACTIVE]
    if active.empty:
        return None

    # 取最新的一筆
    return active.iloc[-1]


def get_current_period_dates() -> tuple[Optional[date], Optional[date]]:
    """取得當前週期的起始和結束日期"""
    period = get_active_period()
    if period is None:
        return None, None

    start = period["Start_Date"]
    end = period["End_Date"]

    # 處理 datetime 或 date 類型
    if hasattr(start, 'date'):
        start = start.date()
    if hasattr(end, 'date'):
        end = end.date()

    return start, end


def get_days_left_in_period() -> int:
    """計算本期剩餘天數"""
    _, period_end = get_current_period_dates()
    if period_end is None:
        return 0

    today = get_taiwan_today()
    days_left = (period_end - today).days + 1  # 包含今天
    return max(days_left, 1)


def parse_amount(value: str) -> float:
    """
    解析金額輸入，支援千分位逗號

    Args:
        value: 使用者輸入的金額字串

    Returns:
        float: 解析後的金額，解析失敗回傳 0
    """
    if not value:
        return 0.0
    try:
        # 移除千分位逗號和空白
        cleaned = str(value).replace(",", "").replace(" ", "").strip()
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


# =============================================================================
# UI 元件 - Tab 1: 記帳
# =============================================================================

def tab_expense():
    """Tab 1: 記帳 (Placeholder)"""
    st.header("記帳")

    # 顯示當前週期資訊
    period = get_active_period()
    if period is not None:
        start, end = get_current_period_dates()
        days_left = get_days_left_in_period()

        st.info(f"**本期：** {start} ~ {end} （剩餘 {days_left} 天）")
        st.metric("Living 預算", f"${float(period['Living_Budget']):,.0f}")
    else:
        st.warning("尚未建立預算週期，請到「策略」頁面建立")

    st.divider()

    # Placeholder
    st.markdown("### 快速記帳")
    st.caption("功能建置中...")

    st.divider()

    st.markdown("### 本期消費紀錄")
    transactions = load_transactions()
    if not transactions.empty:
        expenses = transactions[transactions["Type"] == TYPE_EXPENSE]
        if not expenses.empty:
            st.dataframe(expenses.head(10), use_container_width=True)
        else:
            st.info("本期尚無消費紀錄")
    else:
        st.info("尚無交易記錄")


# =============================================================================
# UI 元件 - Tab 2: 目標
# =============================================================================

def tab_goals():
    """Tab 2: 目標 (Placeholder)"""
    st.header("目標")

    # 儲蓄目標
    st.markdown("### 進行中的儲蓄目標")
    goals = load_saving_goals()

    if goals.empty:
        st.info("尚無儲蓄目標")
    else:
        active_goals = goals[goals["Status"] == "Active"]
        if active_goals.empty:
            st.info("目前沒有進行中的目標")
        else:
            for _, goal in active_goals.iterrows():
                with st.container(border=True):
                    st.markdown(f"**{goal['Name']}**")
                    target = float(goal.get("Target_Amount", 0))
                    accumulated = float(goal.get("Accumulated", 0))
                    progress = min(accumulated / target, 1.0) if target > 0 else 0
                    st.progress(progress)
                    st.caption(f"${accumulated:,.0f} / ${target:,.0f}")

    st.divider()
    st.caption("功能建置中...")


# =============================================================================
# UI 元件 - Tab 3: 策略
# =============================================================================

def tab_strategy():
    """Tab 3: 策略 (Placeholder)"""
    st.header("策略")

    # 週期管理
    st.markdown("### 週期管理")
    period = get_active_period()

    if period is not None:
        start, end = get_current_period_dates()
        st.success(f"當前週期：{start} ~ {end}")
        st.metric("Living 預算", f"${float(period['Living_Budget']):,.0f}")
    else:
        st.warning("尚未建立週期")

        # 簡易建立週期表單
        with st.expander("建立新週期"):
            col1, col2 = st.columns(2)
            with col1:
                new_start = st.date_input("開始日期", value=get_taiwan_today())
            with col2:
                new_end = st.date_input("結束日期", value=get_taiwan_today() + timedelta(days=30))

            new_budget = st.number_input("Living 預算", min_value=0, value=30000, step=1000)

            if st.button("建立週期", type="primary"):
                period_id = add_period(new_start, new_end, new_budget)
                if period_id:
                    st.success(f"已建立週期：{period_id}")
                    st.rerun()

    st.divider()

    # 銀行帳戶管理
    st.markdown("### 銀行帳戶")
    bank_accounts = load_bank_accounts()

    if bank_accounts.empty:
        st.info("尚無銀行帳戶")
    else:
        for _, bank in bank_accounts.iterrows():
            st.markdown(f"- **{bank['Name']}** ({bank['Bank_ID']})")

    with st.expander("新增銀行帳戶"):
        bank_name = st.text_input("帳戶名稱")
        bank_note = st.text_input("備註（選填）")

        if st.button("新增帳戶"):
            if bank_name:
                if add_bank_account(bank_name, bank_note):
                    st.success(f"已新增帳戶：{bank_name}")
                    st.rerun()
            else:
                st.error("請輸入帳戶名稱")

    st.divider()

    # 設定總覽
    st.markdown("### 系統設定")
    config = load_config()
    if config:
        for key, value in config.items():
            st.markdown(f"- **{key}**: {value}")
    else:
        st.info("尚無設定資料")


# =============================================================================
# 連線狀態與資料統計
# =============================================================================

def render_connection_status():
    """顯示連線狀態和資料統計"""
    with st.expander("連線狀態與資料統計", expanded=False):
        spreadsheet = get_spreadsheet()

        if spreadsheet is None:
            st.error("未連線")
            return

        st.success(f"已連線：{spreadsheet.title}")

        # 載入所有資料並顯示統計
        data = load_all_data()

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Bank_Account", len(data["bank_accounts"]))
            st.metric("Wallet_Log", len(data["wallet_log"]))
            st.metric("Period", len(data["periods"]))

        with col2:
            st.metric("Category", len(data["categories"]))
            st.metric("Sub_Tag", len(data["sub_tags"]))
            st.metric("Saving_Goal", len(data["saving_goals"]))

        with col3:
            st.metric("Transaction", len(data["transactions"]))
            st.metric("Settlement_Log", len(data["settlement_log"]))
            st.metric("Config", len(data["config"]))


# =============================================================================
# 主程式
# =============================================================================

def main():
    st.set_page_config(
        page_title="Budget Level v2.1",
        page_icon="💰",
        layout="wide"
    )

    st.title("Budget Level v2.1")
    st.caption("心理帳戶管理系統 - v2.1 Rebuild")

    # 顯示 Toast 訊息（從 session_state 讀取）
    if "show_toast" in st.session_state:
        st.toast(st.session_state["show_toast"])
        del st.session_state["show_toast"]

    # 檢查連線
    if get_spreadsheet() is None:
        st.error("無法連線到 Google Sheets，請確認 secrets.toml 設定正確")
        st.stop()

    # 連線狀態
    render_connection_status()

    st.divider()

    # Tab 導航
    tab1, tab2, tab3 = st.tabs(["📝 記帳", "🎯 目標", "🧭 策略"])

    with tab1:
        tab_expense()

    with tab2:
        tab_goals()

    with tab3:
        tab_strategy()


if __name__ == "__main__":
    main()
