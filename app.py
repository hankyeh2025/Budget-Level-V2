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


def update_bank_account(
    bank_id: str,
    name: str,
    note: str,
    status: str
) -> bool:
    """
    更新銀行帳戶

    Args:
        bank_id: 帳戶 ID
        name: 新名稱
        note: 新備註
        status: "Active" or "Inactive"

    Returns:
        bool: 是否成功
    """
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return False

    try:
        worksheet = spreadsheet.worksheet(SHEET_BANK_ACCOUNT)
        all_data = worksheet.get_all_records()

        # 找到該 Bank_ID 的 row
        for idx, row in enumerate(all_data):
            if row.get("Bank_ID") == bank_id:
                row_number = idx + 2  # +2 因為 header 佔第 1 行，idx 從 0 開始

                # 欄位順序：Bank_ID | Name | Note | Status
                # 更新 Name (B), Note (C), Status (D)
                worksheet.update(f"B{row_number}:D{row_number}", [[name, note, status]])

                st.cache_data.clear()
                return True

        st.error(f"找不到帳戶：{bank_id}")
        return False

    except Exception as e:
        st.error(f"更新銀行帳戶失敗: {e}")
        return False


def update_category(category_id: str, updates: dict) -> bool:
    """
    更新科目資料

    Args:
        category_id: 科目 ID
        updates: dict with keys like 'Budget', 'Default_Bank_ID', 'Default_Payment_Method'

    Returns:
        bool: 是否成功
    """
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return False

    try:
        worksheet = spreadsheet.worksheet(SHEET_CATEGORY)
        all_data = worksheet.get_all_records()
        headers = worksheet.row_values(1)

        # 找到該 Category_ID 的 row
        for idx, row in enumerate(all_data):
            if row.get("Category_ID") == category_id:
                row_number = idx + 2

                # 更新指定的欄位
                for key, value in updates.items():
                    if key in headers:
                        col_number = headers.index(key) + 1
                        worksheet.update_cell(row_number, col_number, value)

                st.cache_data.clear()
                return True

        st.error(f"找不到科目：{category_id}")
        return False

    except Exception as e:
        st.error(f"更新科目失敗: {e}")
        return False


def update_sub_tag(sub_tag_id: str, updates: dict) -> bool:
    """
    更新子類資料

    Args:
        sub_tag_id: 子類 ID
        updates: dict with keys like 'Budget', 'Default_Bank_ID', 'Default_Payment_Method'

    Returns:
        bool: 是否成功
    """
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return False

    try:
        worksheet = spreadsheet.worksheet(SHEET_SUB_TAG)
        all_data = worksheet.get_all_records()
        headers = worksheet.row_values(1)

        # 找到該 Sub_Tag_ID 的 row
        for idx, row in enumerate(all_data):
            if row.get("Sub_Tag_ID") == sub_tag_id:
                row_number = idx + 2

                # 更新指定的欄位
                for key, value in updates.items():
                    if key in headers:
                        col_number = headers.index(key) + 1
                        worksheet.update_cell(row_number, col_number, value)

                st.cache_data.clear()
                return True

        st.error(f"找不到子類：{sub_tag_id}")
        return False

    except Exception as e:
        st.error(f"更新子類失敗: {e}")
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
# Period 狀態函式
# =============================================================================

def is_period_overdue(period: pd.Series) -> bool:
    """
    檢查週期是否已過期（今天 > End_Date）

    Args:
        period: Period 資料列

    Returns:
        True if 今天已超過結束日
    """
    end_date = period["End_Date"]
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date).date()
    elif hasattr(end_date, 'date'):
        end_date = end_date.date()
    return get_taiwan_today() > end_date


def get_period_by_id(period_id: str) -> Optional[pd.Series]:
    """根據 ID 取得週期資料"""
    periods = load_periods()
    if periods.empty:
        return None
    match = periods[periods["Period_ID"] == period_id]
    if match.empty:
        return None
    return match.iloc[0]


def get_period_days_left(period: pd.Series) -> int:
    """
    計算週期剩餘天數（包含今天）

    Returns:
        剩餘天數，最小為 0
    """
    end_date = period["End_Date"]
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date).date()
    elif hasattr(end_date, 'date'):
        end_date = end_date.date()

    today = get_taiwan_today()
    days_left = (end_date - today).days + 1
    return max(days_left, 0)


# =============================================================================
# Living 計算函式
# =============================================================================

def get_living_remaining(period_id: str) -> float:
    """
    計算 Living 本期剩餘

    公式：Living_Budget - Σ Expense(Account='Living', Period_ID=period_id)

    Returns:
        剩餘金額（可為負數表示超支）
    """
    period = get_period_by_id(period_id)
    if period is None:
        return 0.0

    budget = float(period["Living_Budget"]) if period["Living_Budget"] else 0.0

    transactions = load_transactions()
    if transactions.empty:
        return budget

    expenses = transactions[
        (transactions["Type"] == TYPE_EXPENSE) &
        (transactions["Account"] == ACCOUNT_LIVING) &
        (transactions["Period_ID"] == period_id)
    ]
    spent = float(expenses["Amount"].sum()) if not expenses.empty else 0.0

    return budget - spent


def get_daily_available(period_id: str) -> float:
    """
    計算今日可用額度

    公式：Living 剩餘 ÷ 週期剩餘天數

    Returns:
        今日建議可用金額
    """
    remaining = get_living_remaining(period_id)
    period = get_period_by_id(period_id)
    if period is None:
        return 0.0

    days_left = get_period_days_left(period)

    # 避免除以零，若剩餘天數為 0 則回傳全部剩餘
    if days_left <= 0:
        return remaining

    return remaining / days_left


def get_category_spent(category_id: str, period_id: str) -> float:
    """計算特定科目本期支出"""
    transactions = load_transactions()
    if transactions.empty:
        return 0.0

    expenses = transactions[
        (transactions["Type"] == TYPE_EXPENSE) &
        (transactions["Category_ID"] == category_id) &
        (transactions["Period_ID"] == period_id)
    ]
    return float(expenses["Amount"].sum()) if not expenses.empty else 0.0


# =============================================================================
# 帳戶餘額計算函式
# =============================================================================

def get_backup_balance() -> float:
    """
    計算 Back Up 餘額

    公式：
    Config['Back_Up_Initial']
    + sum(Allocate to Back_Up) - 尚未實作
    - sum(Settlement_Out)
    + sum(Transfer to Back_Up)
    - sum(Transfer from Back_Up)
    """
    config = load_config()
    initial = float(config.get("Back_Up_Initial", 0) or 0)

    transactions = load_transactions()
    if transactions.empty:
        return initial

    # Settlement_Out 扣 Back Up
    settlement_out = transactions[
        transactions["Type"] == TYPE_SETTLEMENT_OUT
    ]["Amount"].sum()

    # Transfer to Back Up
    transfer_in = transactions[
        (transactions["Type"] == TYPE_TRANSFER) &
        (transactions["Target_Account"] == ACCOUNT_BACKUP)
    ]["Amount"].sum()

    # Transfer from Back Up
    transfer_out = transactions[
        (transactions["Type"] == TYPE_TRANSFER) &
        (transactions["Account"] == ACCOUNT_BACKUP)
    ]["Amount"].sum()

    return float(initial - settlement_out + transfer_in - transfer_out)


def get_free_fund_balance() -> float:
    """
    計算 Free Fund 餘額

    公式：
    Config['Free_Fund_Initial']
    + sum(Settlement_In)
    + sum(Transfer to Free_Fund)
    - sum(Transfer from Free_Fund)
    """
    config = load_config()
    initial = float(config.get("Free_Fund_Initial", 0) or 0)

    transactions = load_transactions()
    if transactions.empty:
        return initial

    # Settlement_In 進 Free Fund
    settlement_in = transactions[
        transactions["Type"] == TYPE_SETTLEMENT_IN
    ]["Amount"].sum()

    # Transfer to Free Fund
    transfer_in = transactions[
        (transactions["Type"] == TYPE_TRANSFER) &
        (transactions["Target_Account"] == ACCOUNT_FREEFUND)
    ]["Amount"].sum()

    # Transfer from Free Fund
    transfer_out = transactions[
        (transactions["Type"] == TYPE_TRANSFER) &
        (transactions["Account"] == ACCOUNT_FREEFUND)
    ]["Amount"].sum()

    return float(initial + settlement_in + transfer_in - transfer_out)


# =============================================================================
# 結算函式
# =============================================================================

def update_period_status(period_id: str, status: str, settled_at: str = "") -> bool:
    """更新週期狀態"""
    try:
        sheet = get_spreadsheet().worksheet(SHEET_PERIOD)
        records = sheet.get_all_records()

        for idx, record in enumerate(records):
            if record.get("Period_ID") == period_id:
                row_num = idx + 2  # 標題列 + 1-indexed

                # 找到 Status 欄位位置
                headers = sheet.row_values(1)
                status_col = headers.index("Status") + 1
                sheet.update_cell(row_num, status_col, status)

                # 更新 Settled_At
                if settled_at and "Settled_At" in headers:
                    settled_col = headers.index("Settled_At") + 1
                    sheet.update_cell(row_num, settled_col, settled_at)

                st.cache_data.clear()
                return True
        return False
    except Exception as e:
        st.error(f"更新週期狀態失敗：{e}")
        return False


def settle_period(period_id: str) -> dict:
    """
    結算週期

    Actions:
    1. 計算：Living_Budget - Total_Expense = Net_Result
    2. If Net > 0: 產生 Settlement_In 交易（進 Free_Fund）
    3. If Net < 0: 產生 Settlement_Out 交易（扣 Back_Up）
    4. 寫入 Settlement_Log
    5. 更新 Period status 為 'Settled'

    Returns:
        {
            'success': bool,
            'net_result': float,  # 正=結餘, 負=超支
            'settlement_id': str,
            'message': str
        }
    """
    try:
        period = get_period_by_id(period_id)
        if period is None:
            return {'success': False, 'net_result': 0, 'settlement_id': '', 'message': '找不到週期'}

        if period["Status"] == PERIOD_SETTLED:
            return {'success': False, 'net_result': 0, 'settlement_id': '', 'message': '此週期已結算'}

        # 計算結果
        budget = float(period["Living_Budget"]) if period["Living_Budget"] else 0.0
        transactions = load_transactions()

        if transactions.empty:
            total_expense = 0.0
        else:
            expenses = transactions[
                (transactions["Type"] == TYPE_EXPENSE) &
                (transactions["Account"] == ACCOUNT_LIVING) &
                (transactions["Period_ID"] == period_id)
            ]
            total_expense = float(expenses["Amount"].sum()) if not expenses.empty else 0.0

        net_result = budget - total_expense

        # 產生結算交易
        now = get_taiwan_now()
        settlement_id = f"STL{now.strftime('%Y%m%d%H%M%S')}"

        if net_result > 0:
            # 結餘進 Free Fund
            add_transaction(
                trans_type=TYPE_SETTLEMENT_IN,
                amount=net_result,
                account=ACCOUNT_FREEFUND,
                note="週期結算結餘",
                ref=period_id
            )
            impact_account = ACCOUNT_FREEFUND
        elif net_result < 0:
            # 超支扣 Back Up
            add_transaction(
                trans_type=TYPE_SETTLEMENT_OUT,
                amount=abs(net_result),
                account=ACCOUNT_BACKUP,
                note="週期結算超支",
                ref=period_id
            )
            impact_account = ACCOUNT_BACKUP
        else:
            impact_account = ""

        # 寫入 Settlement_Log
        sheet = get_spreadsheet().worksheet(SHEET_SETTLEMENT_LOG)
        sheet.append_row([
            settlement_id,
            period_id,
            budget,
            total_expense,
            net_result,
            impact_account,
            now.strftime("%Y-%m-%d %H:%M:%S")
        ], value_input_option="USER_ENTERED")

        # 更新 Period 狀態
        update_period_status(period_id, PERIOD_SETTLED, now.strftime("%Y-%m-%d %H:%M:%S"))

        st.cache_data.clear()

        return {
            'success': True,
            'net_result': net_result,
            'settlement_id': settlement_id,
            'message': f"結算完成：{'結餘' if net_result >= 0 else '超支'} ${abs(net_result):,.0f}"
        }

    except Exception as e:
        return {'success': False, 'net_result': 0, 'settlement_id': '', 'message': f'結算失敗：{str(e)}'}


def get_wallet_balance() -> float:
    """
    計算錢包餘額

    公式：Income - Allocate_Out + Transfer_In + Adjustment
    """
    logs = load_wallet_log()
    if logs.empty:
        return 0.0

    income = logs[logs["Type"] == WALLET_INCOME]["Amount"].sum()
    allocate_out = logs[logs["Type"] == WALLET_ALLOCATE_OUT]["Amount"].sum()
    transfer_in = logs[logs["Type"] == WALLET_TRANSFER_IN]["Amount"].sum()
    adjustment = logs[logs["Type"] == WALLET_ADJUSTMENT]["Amount"].sum()

    return float(income - allocate_out + transfer_in + adjustment)


def get_defaults_for_expense(category_id: str, sub_tag_id: str = "") -> dict:
    """
    取得記帳時的預設值

    Priority:
    1. Sub_Tag defaults (if sub_tag_id provided and has non-empty defaults)
    2. Category defaults
    3. Empty string (user must select)

    Returns:
        {
            'bank_id': str,
            'payment_method': str  # 'Credit' or 'Direct' or ''
        }
    """
    categories = load_categories()
    sub_tags = load_sub_tags()

    result = {'bank_id': '', 'payment_method': ''}

    # Get category defaults
    if not categories.empty and 'Category_ID' in categories.columns:
        cat = categories[categories['Category_ID'] == category_id]
        if not cat.empty:
            cat_row = cat.iloc[0]
            # Handle edge case: columns might not exist
            if 'Default_Bank_ID' in cat_row:
                result['bank_id'] = str(cat_row.get('Default_Bank_ID', '') or '')
            if 'Default_Payment_Method' in cat_row:
                result['payment_method'] = str(cat_row.get('Default_Payment_Method', '') or '')

    # Override with sub_tag defaults if available
    if sub_tag_id and not sub_tags.empty and 'Sub_Tag_ID' in sub_tags.columns:
        sub = sub_tags[sub_tags['Sub_Tag_ID'] == sub_tag_id]
        if not sub.empty:
            sub_row = sub.iloc[0]
            if 'Default_Bank_ID' in sub_row and sub_row.get('Default_Bank_ID'):
                result['bank_id'] = str(sub_row['Default_Bank_ID'])
            if 'Default_Payment_Method' in sub_row and sub_row.get('Default_Payment_Method'):
                result['payment_method'] = str(sub_row['Default_Payment_Method'])

    return result


# =============================================================================
# UI 元件 - Dialogs
# =============================================================================

@st.dialog("收入入帳")
def dialog_income():
    """收入入帳 Dialog"""
    # 金額輸入
    amount_text = st.text_input("金額 *", placeholder="輸入金額")

    # 銀行帳戶選擇
    bank_accounts = load_bank_accounts()
    bank_options = ["（不指定）"]
    bank_id_map = {"（不指定）": ""}

    if not bank_accounts.empty:
        active_banks = bank_accounts[bank_accounts["Status"] == "Active"]
        for _, bank in active_banks.iterrows():
            bank_options.append(bank["Name"])
            bank_id_map[bank["Name"]] = bank["Bank_ID"]

    selected_bank = st.selectbox("銀行帳戶", bank_options)
    bank_id = bank_id_map.get(selected_bank, "")

    # 備註
    note = st.text_input("備註（選填）")

    st.divider()

    # 按鈕
    col1, col2 = st.columns(2)
    with col1:
        if st.button("取消", use_container_width=True):
            st.rerun()
    with col2:
        if st.button("確認入帳", type="primary", use_container_width=True):
            amount = parse_amount(amount_text)
            if amount <= 0:
                st.error("請輸入有效金額")
            else:
                if add_wallet_log(WALLET_INCOME, amount, bank_id, note):
                    st.session_state["show_toast"] = f"已入帳 ${amount:,.0f}"
                    st.rerun()


@st.dialog("校正錢包")
def dialog_adjustment():
    """校正錢包 Dialog"""
    # 顯示系統餘額
    current_balance = get_wallet_balance()
    st.markdown(f"**系統餘額：** ${current_balance:,.0f}")

    st.divider()

    # 實際餘額輸入
    actual_text = st.text_input("目前實際餘額 *", placeholder="輸入實際餘額")

    # 計算差額並預覽
    actual = parse_amount(actual_text)
    if actual_text:
        difference = actual - current_balance
        if difference > 0:
            st.success(f"將調整 +${difference:,.0f}")
        elif difference < 0:
            st.warning(f"將調整 -${abs(difference):,.0f}")
        else:
            st.info("無需調整")

    st.divider()

    # 按鈕
    col1, col2 = st.columns(2)
    with col1:
        if st.button("取消", use_container_width=True, key="adj_cancel"):
            st.rerun()
    with col2:
        if st.button("確認校正", type="primary", use_container_width=True, key="adj_confirm"):
            actual = parse_amount(actual_text)
            if not actual_text:
                st.error("請輸入實際餘額")
            else:
                difference = actual - current_balance
                if difference == 0:
                    st.info("無需調整")
                else:
                    if add_wallet_log(WALLET_ADJUSTMENT, difference, note="手動校正"):
                        st.session_state["show_toast"] = "已校正"
                        st.rerun()


@st.dialog("編輯銀行帳戶")
def dialog_edit_bank_account(bank_id: str, current_name: str, current_note: str, current_status: str):
    """編輯銀行帳戶 Dialog"""
    # 名稱
    new_name = st.text_input("帳戶名稱 *", value=current_name)

    # 備註
    new_note = st.text_input("備註", value=current_note)

    # 狀態
    status_options = ["Active", "Inactive"]
    current_index = status_options.index(current_status) if current_status in status_options else 0
    new_status = st.radio(
        "狀態",
        status_options,
        index=current_index,
        format_func=lambda x: "啟用中" if x == "Active" else "已停用",
        horizontal=True
    )

    # 停用警告
    if new_status == "Inactive" and current_status == "Active":
        st.warning("停用後將無法在新交易中選擇此帳戶")

    st.divider()

    # 按鈕
    col1, col2 = st.columns(2)
    with col1:
        if st.button("取消", use_container_width=True, key="edit_bank_cancel"):
            st.rerun()
    with col2:
        if st.button("儲存", type="primary", use_container_width=True, key="edit_bank_save"):
            if not new_name.strip():
                st.error("請輸入帳戶名稱")
            else:
                if update_bank_account(bank_id, new_name.strip(), new_note, new_status):
                    st.session_state["show_toast"] = "已更新帳戶"
                    st.rerun()


# =============================================================================
# UI 元件 - Tab 1: 記帳
# =============================================================================

def tab_expense():
    """Tab 1: 記帳"""
    st.header("記帳")

    # 載入設定
    config = load_config()

    # 狀態總覽區域
    period = get_active_period()

    col1, col2 = st.columns(2)
    with col1:
        st.metric("💰 錢包", f"${get_wallet_balance():,.0f}")
    with col2:
        backup_balance = get_backup_balance()
        backup_limit = float(config.get("Back_Up_Limit", 150000) or 150000)
        backup_pct = (backup_balance / backup_limit * 100) if backup_limit > 0 else 0
        st.metric("🛡️ Back Up", f"${backup_balance:,.0f}")
        if backup_balance < 0:
            st.error(f"⚠️ 已透支！")
        else:
            st.progress(min(backup_pct / 100, 1.0))
            st.caption(f"{backup_pct:.0f}% / ${backup_limit:,.0f}")

    col3, col4 = st.columns(2)
    with col3:
        st.metric("✨ Free Fund", f"${get_free_fund_balance():,.0f}")
    with col4:
        if period is not None:
            days_left = get_period_days_left(period)
            end_date = period["End_Date"]
            if isinstance(end_date, str):
                end_date = pd.to_datetime(end_date).date()
            elif hasattr(end_date, 'date'):
                end_date = end_date.date()

            if is_period_overdue(period):
                st.warning(f"⚠️ 週期已結束，待結算")
            else:
                st.metric("📅 週期剩餘", f"{days_left} 天")
                st.caption(f"至 {end_date.strftime('%m/%d')}")
        else:
            st.info("📅 無進行中週期")

    st.divider()

    # 今日可用額度（大字顯示）
    if period is not None and not is_period_overdue(period):
        period_id = period["Period_ID"]
        daily = get_daily_available(period_id)
        remaining = get_living_remaining(period_id)
        days_left = get_period_days_left(period)

        st.markdown("### 今日可用額度")
        if daily >= 0:
            st.markdown(f"## ${daily:,.0f}")
        else:
            st.markdown(f"## :red[${daily:,.0f}]")
            st.error("Living 已超支！")
        st.caption(f"Living 剩餘 ${remaining:,.0f} ÷ {days_left} 天")
    elif period is not None and is_period_overdue(period):
        st.warning("⚠️ 週期已結束，請到「策略」頁面進行結算")
    else:
        st.warning("請先到「策略」頁面啟動週期儀式")

    st.divider()

    # 科目進度區域
    st.markdown("### 📊 各科目本期狀態")

    if period is not None:
        period_id = period["Period_ID"]
        categories = load_categories()

        if not categories.empty and "Status" in categories.columns:
            active_cats = categories[categories["Status"] == "Active"]

            if active_cats.empty:
                st.info("尚無啟用中的科目")
            else:
                for _, cat in active_cats.iterrows():
                    cat_id = cat["Category_ID"]
                    cat_name = cat["Name"]
                    budget = float(cat["Budget"]) if cat.get("Budget") else 0

                    spent = get_category_spent(cat_id, period_id)

                    if budget > 0:
                        progress = spent / budget
                        warning = " ⚠️" if progress > 0.9 else ""

                        st.write(f"**{cat_name}**{warning}")
                        st.progress(min(progress, 1.0))
                        st.caption(f"${spent:,.0f} / ${budget:,.0f} ({progress*100:.0f}%)")
                    else:
                        st.write(f"**{cat_name}** — 未設定預算")
                        if spent > 0:
                            st.caption(f"已花：${spent:,.0f}")
        else:
            st.info("尚無科目資料")
    else:
        st.info("啟動週期後顯示科目進度")

    st.divider()

    # Placeholder
    st.markdown("### 快速記帳")
    st.caption("功能建置中...")

    st.divider()

    # 本期消費紀錄
    st.markdown("### 本期消費紀錄")
    transactions = load_transactions()
    if not transactions.empty and period is not None:
        period_id = period["Period_ID"]
        expenses = transactions[
            (transactions["Type"] == TYPE_EXPENSE) &
            (transactions["Period_ID"] == period_id)
        ]
        if not expenses.empty:
            # 按日期倒序排列
            expenses_sorted = expenses.sort_values("Date", ascending=False)
            st.dataframe(expenses_sorted.head(10), use_container_width=True)
        else:
            st.info("本期尚無消費紀錄")
    elif not transactions.empty:
        expenses = transactions[transactions["Type"] == TYPE_EXPENSE]
        if not expenses.empty:
            expenses_sorted = expenses.sort_values("Date", ascending=False)
            st.dataframe(expenses_sorted.head(10), use_container_width=True)
        else:
            st.info("尚無消費紀錄")
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
    """Tab 3: 策略"""
    st.header("策略")

    # 錢包操作
    with st.expander("💰 錢包操作", expanded=True):
        wallet_balance = get_wallet_balance()
        st.markdown(f"**目前餘額：** ${wallet_balance:,.0f}")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("+ 收入入帳", use_container_width=True):
                dialog_income()
        with col2:
            if st.button("校正錢包", use_container_width=True):
                dialog_adjustment()

    st.divider()

    # 週期狀態
    st.markdown("### 💫 週期狀態")

    period = get_active_period()

    if period is not None:
        period_id = period["Period_ID"]
        start_date = period["Start_Date"]
        end_date = period["End_Date"]

        # 格式化日期
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date).date()
        elif hasattr(start_date, 'date'):
            start_date = start_date.date()

        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date).date()
        elif hasattr(end_date, 'date'):
            end_date = end_date.date()

        if is_period_overdue(period):
            st.error(f"⚠️ 週期已結束，待結算")
            st.write(f"週期：{start_date.strftime('%m/%d')} ~ {end_date.strftime('%m/%d')}")

            # 結算按鈕
            if st.button("進行結算", type="primary", key="settle_btn"):
                result = settle_period(period_id)
                if result['success']:
                    st.session_state["show_toast"] = result['message']
                    st.rerun()
                else:
                    st.error(result['message'])
        else:
            days_left = get_period_days_left(period)
            st.success(f"✓ 進行中")
            st.write(f"週期：{start_date.strftime('%m/%d')} ~ {end_date.strftime('%m/%d')}（剩 {days_left} 天）")

        # 當期總覽
        with st.expander("📊 當期總覽"):
            budget = float(period["Living_Budget"]) if period["Living_Budget"] else 0
            remaining = get_living_remaining(period_id)
            spent = budget - remaining

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Living 預算", f"${budget:,.0f}")
            with col2:
                st.metric("Living 已花", f"${spent:,.0f}")
            with col3:
                if remaining >= 0:
                    st.metric("Living 剩餘", f"${remaining:,.0f}")
                else:
                    st.metric("Living 剩餘", f"${remaining:,.0f}", delta=f"超支 ${abs(remaining):,.0f}", delta_color="inverse")

    else:
        st.info("無進行中週期")

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
    st.markdown("### 🏦 銀行帳戶管理")

    bank_accounts = load_bank_accounts()

    if bank_accounts.empty:
        st.info("尚無銀行帳戶")
    else:
        for _, bank in bank_accounts.iterrows():
            bank_id = bank["Bank_ID"]
            bank_name = bank["Name"]
            bank_note = str(bank.get("Note", "") or "")
            bank_status = bank.get("Status", "Active")
            is_active = bank_status == "Active"

            col1, col2 = st.columns([4, 1])
            with col1:
                if is_active:
                    display_text = f"**{bank_name}**"
                    if bank_note:
                        display_text += f"  {bank_note}"
                    st.markdown(display_text)
                else:
                    st.markdown(f"~~{bank_name}~~ *(已停用)*")
            with col2:
                if st.button("編輯", key=f"edit_bank_{bank_id}", use_container_width=True):
                    dialog_edit_bank_account(bank_id, bank_name, bank_note, bank_status)

    # 新增帳戶
    with st.expander("+ 新增帳戶"):
        with st.form(key="add_bank_form", clear_on_submit=True):
            bank_name_input = st.text_input("帳戶名稱")
            bank_note_input = st.text_input("備註（選填）")

            submitted = st.form_submit_button("新增帳戶")

            if submitted:
                if bank_name_input:
                    if add_bank_account(bank_name_input, bank_note_input):
                        st.session_state["show_toast"] = f"已新增帳戶：{bank_name_input}"
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
