"""
Budget Level v2 - 心理帳戶管理系統
使用信封袋理財法概念，管理五個心理帳戶
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


# 五個心理帳戶
ACCOUNT_LIVING = "Living"
ACCOUNT_SAVING = "Saving"
ACCOUNT_INVESTING = "Investing"
ACCOUNT_BACKUP = "Back_Up"
ACCOUNT_FREEFUND = "Free_Fund"

# 交易類型
TYPE_INCOME = "Income"
TYPE_EXPENSE = "Expense"
TYPE_ALLOCATE = "Allocate"
TYPE_SAVING_IN = "Saving_In"
TYPE_SAVING_COMPLETE = "Saving_Complete"
TYPE_INVESTING_CONFIRM = "Investing_Confirm"
TYPE_SETTLEMENT_IN = "Settlement_In"
TYPE_SETTLEMENT_OUT = "Settlement_Out"
TYPE_TRANSFER = "Transfer"

# Sheet 名稱
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
    """一次載入所有資料（減少 API 呼叫）"""
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return {
            "categories": pd.DataFrame(),
            "sub_tags": pd.DataFrame(),
            "saving_goals": pd.DataFrame(),
            "transactions": pd.DataFrame(),
            "config": {}
        }

    try:
        data = {}

        # Categories
        ws = spreadsheet.worksheet(SHEET_CATEGORY)
        data["categories"] = pd.DataFrame(ws.get_all_records())

        # Sub_Tags
        ws = spreadsheet.worksheet(SHEET_SUB_TAG)
        data["sub_tags"] = pd.DataFrame(ws.get_all_records())

        # Saving_Goals
        ws = spreadsheet.worksheet(SHEET_SAVING_GOAL)
        data["saving_goals"] = pd.DataFrame(ws.get_all_records())

        # Transactions
        ws = spreadsheet.worksheet(SHEET_TRANSACTION)
        df = pd.DataFrame(ws.get_all_records())
        if not df.empty and "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        data["transactions"] = df

        # Config
        ws = spreadsheet.worksheet(SHEET_CONFIG)
        config_data = ws.get_all_records()
        data["config"] = {row["Key"]: row["Value"] for row in config_data if "Key" in row}

        return data

    except Exception as e:
        st.error(f"載入資料失敗: {e}")
        return {
            "categories": pd.DataFrame(),
            "sub_tags": pd.DataFrame(),
            "saving_goals": pd.DataFrame(),
            "transactions": pd.DataFrame(),
            "config": {}
        }


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


def load_config() -> dict:
    """載入系統設定"""
    return load_all_data()["config"]


# =============================================================================
# 資料存取層 - 寫入
# =============================================================================

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
    ref: str = ""
) -> bool:
    """新增交易記錄"""
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return False

    try:
        worksheet = spreadsheet.worksheet(SHEET_TRANSACTION)

        # 產生交易 ID
        trans_id = f"TXN{get_taiwan_now().strftime('%Y%m%d%H%M%S')}"

        # 確保 amount 是 Python 原生類型
        amount = float(amount)

        # 建立交易資料 - 對齊 Sheet 欄位順序
        # Txn_ID | Timestamp | Date | Type | Amount | Account | Category_ID | Sub_Tag_ID | Goal_ID | Target_Account | Item | Note | Ref
        row = [
            trans_id,                                      # Txn_ID
            get_taiwan_now().strftime("%Y-%m-%d %H:%M:%S"),  # Timestamp (完整時間)
            get_taiwan_now().strftime("%Y-%m-%d"),           # Date (只有日期)
            trans_type,                                    # Type
            amount,                                        # Amount
            account,                                       # Account
            category_id,                                   # Category_ID
            sub_tag_id,                                    # Sub_Tag_ID
            goal_id,                                       # Goal_ID
            target_account,                                # Target_Account
            item,                                          # Item
            note,                                          # Note
            ref                                            # Ref
        ]

        worksheet.append_row(row, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return True

    except Exception as e:
        st.error(f"新增交易失敗: {e}")
        return False


def add_saving_goal(name: str, target_amount: float, deadline: str = "") -> bool:
    """新增儲蓄目標"""
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return False

    try:
        worksheet = spreadsheet.worksheet(SHEET_SAVING_GOAL)

        # 產生 Goal_ID
        goal_id = f"GOAL{get_taiwan_now().strftime('%Y%m%d%H%M%S')}"

        # 欄位順序：Goal_ID | Name | Target_Amount | Deadline | Accumulated | Status | Created_At | Completed_At
        row = [
            goal_id,
            name,
            target_amount,
            deadline,  # 空字串 = 無截止日
            0,  # Accumulated (初始為 0)
            "Active",
            get_taiwan_now().strftime("%Y-%m-%d %H:%M:%S"),
            ""  # Completed_At
        ]

        worksheet.append_row(row, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return True

    except Exception as e:
        st.error(f"新增儲蓄目標失敗: {e}")
        return False


def complete_saving_goal(goal_id: str, actual_expense: float, note: str = "") -> bool:
    """
    完成儲蓄目標
    1. 寫入 Saving_Complete 交易
    2. 若有正差額，寫入 Settlement_In（進 Free Fund）
    3. 更新 Saving_Goal 的 Status 和 Completed_At
    """
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return False

    try:
        # 1. 計算累積金額
        accumulated = get_goal_accumulated(goal_id)

        # 2. 寫入 Saving_Complete 交易
        success = add_transaction(
            trans_type=TYPE_SAVING_COMPLETE,
            amount=actual_expense,
            account=ACCOUNT_SAVING,
            goal_id=goal_id,
            item="儲蓄目標完成",
            note=note,
            ref="Goal_Complete"
        )
        if not success:
            return False

        # 3. 若有正差額，寫入 Settlement_In
        difference = accumulated - actual_expense
        if difference > 0:
            add_transaction(
                trans_type=TYPE_SETTLEMENT_IN,
                amount=difference,
                account=ACCOUNT_FREEFUND,
                goal_id=goal_id,
                item="儲蓄目標差額",
                note=f"目標完成差額 ${difference:,.0f}",
                ref="Goal_Surplus"
            )

        # 4. 更新 Saving_Goal sheet 的 Status 和 Completed_At
        worksheet = spreadsheet.worksheet(SHEET_SAVING_GOAL)
        all_data = worksheet.get_all_records()

        # 找到該 Goal 的 row（header 是第 1 行，資料從第 2 行開始）
        for idx, row in enumerate(all_data):
            if row.get("Goal_ID") == goal_id:
                row_number = idx + 2  # +2 因為 header 佔第 1 行，idx 從 0 開始

                # Status 在第 6 欄 (F)，Completed_At 在第 8 欄 (H)
                worksheet.update_cell(row_number, 6, "Completed")  # Status
                worksheet.update_cell(row_number, 8, get_taiwan_now().strftime("%Y-%m-%d %H:%M:%S"))  # Completed_At
                break

        st.cache_data.clear()
        return True

    except Exception as e:
        st.error(f"完成儲蓄目標失敗: {e}")
        return False


# =============================================================================
# 工具函式
# =============================================================================

def get_pay_day() -> int:
    """取得發薪日（預設 5 號）"""
    config = load_config()
    # 相容兩種寫法：Payday 或 Pay_Day
    return int(config.get("Payday", config.get("Pay_Day", 5)))


def get_current_period() -> tuple[date, date]:
    """取得當前發薪週期的起始和結束日期"""
    pay_day = get_pay_day()
    today = get_taiwan_today()

    # 計算本期起始日
    if today.day >= pay_day:
        period_start = date(today.year, today.month, pay_day)
    else:
        # 上個月的發薪日
        if today.month == 1:
            period_start = date(today.year - 1, 12, pay_day)
        else:
            period_start = date(today.year, today.month - 1, pay_day)

    # 計算本期結束日（下個發薪日前一天）
    if period_start.month == 12:
        next_pay_day = date(period_start.year + 1, 1, pay_day)
    else:
        next_pay_day = date(period_start.year, period_start.month + 1, pay_day)

    period_end = next_pay_day - pd.Timedelta(days=1)

    return period_start, period_end.date() if hasattr(period_end, 'date') else period_end


def get_days_left_in_period() -> int:
    """計算本期剩餘天數"""
    _, period_end = get_current_period()
    today = get_taiwan_today()
    days_left = (period_end - today).days + 1  # 包含今天
    return max(days_left, 1)


def get_period_transactions() -> pd.DataFrame:
    """取得本期的交易記錄"""
    period_start, period_end = get_current_period()
    df = load_transactions()

    if df.empty:
        return df

    # 過濾本期交易
    mask = (df["Date"].dt.date >= period_start) & (df["Date"].dt.date <= period_end)
    return df[mask]


def get_living_expenses_by_category() -> pd.DataFrame:
    """取得本期各科目的支出統計"""
    df = get_period_transactions()

    if df.empty:
        return pd.DataFrame(columns=["Category_ID", "Spent"])

    # 只計算 Expense 類型
    expenses = df[df["Type"] == TYPE_EXPENSE]

    if expenses.empty:
        return pd.DataFrame(columns=["Category_ID", "Spent"])

    # 按 Category_ID 分組統計（不是 Category）
    result = expenses.groupby("Category_ID")["Amount"].sum().reset_index()
    result.columns = ["Category_ID", "Spent"]

    return result


def get_backup_balance() -> float:
    """
    Back Up 餘額 =
        Config['Back_Up_Initial']
        + sum(Allocate to Back_Up)
        - sum(Settlement_Out)
        + sum(Transfer to Back_Up)
        - sum(Transfer from Back_Up)
    """
    config = load_config()
    initial = float(config.get("Back_Up_Initial", 0))

    df = load_transactions()
    if df.empty:
        return initial

    balance = initial

    # + sum(Allocate to Back_Up)
    allocate_in = df[(df["Type"] == TYPE_ALLOCATE) & (df["Account"] == ACCOUNT_BACKUP)]["Amount"].sum()
    balance += allocate_in

    # - sum(Settlement_Out)
    settlement_out = df[df["Type"] == TYPE_SETTLEMENT_OUT]["Amount"].sum()
    balance -= settlement_out

    # + sum(Transfer to Back_Up)
    transfer_in = df[(df["Type"] == TYPE_TRANSFER) & (df["Target_Account"] == ACCOUNT_BACKUP)]["Amount"].sum()
    balance += transfer_in

    # - sum(Transfer from Back_Up)
    transfer_out = df[(df["Type"] == TYPE_TRANSFER) & (df["Account"] == ACCOUNT_BACKUP)]["Amount"].sum()
    balance -= transfer_out

    return balance


def get_free_fund_balance() -> float:
    """
    Free Fund 餘額 =
        Config['Free_Fund_Initial']
        + sum(Settlement_In)
        + sum(Transfer to Free_Fund)
        - sum(Transfer from Free_Fund)
    """
    config = load_config()
    initial = float(config.get("Free_Fund_Initial", 0))

    df = load_transactions()
    if df.empty:
        return initial

    balance = initial

    # + sum(Settlement_In)
    settlement_in = df[df["Type"] == TYPE_SETTLEMENT_IN]["Amount"].sum()
    balance += settlement_in

    # + sum(Transfer to Free_Fund)
    transfer_in = df[(df["Type"] == TYPE_TRANSFER) & (df["Target_Account"] == ACCOUNT_FREEFUND)]["Amount"].sum()
    balance += transfer_in

    # - sum(Transfer from Free_Fund)
    transfer_out = df[(df["Type"] == TYPE_TRANSFER) & (df["Account"] == ACCOUNT_FREEFUND)]["Amount"].sum()
    balance -= transfer_out

    return balance


def get_investing_total() -> float:
    """計算投資累積總額"""
    df = load_transactions()
    if df.empty:
        return 0
    return df[df["Type"] == TYPE_INVESTING_CONFIRM]["Amount"].sum()


def get_goal_accumulated(goal_id: str) -> float:
    """計算單一儲蓄目標的累積金額"""
    df = load_transactions()
    if df.empty:
        return 0

    # + Saving_In
    saving_in = df[(df["Type"] == TYPE_SAVING_IN) & (df["Goal_ID"] == goal_id)]["Amount"].sum()

    # - Saving_Complete
    saving_complete = df[(df["Type"] == TYPE_SAVING_COMPLETE) & (df["Goal_ID"] == goal_id)]["Amount"].sum()

    return saving_in - saving_complete


def check_investing_confirmed_this_period() -> bool:
    """檢查本期是否已確認投資"""
    df = get_period_transactions()
    if df.empty:
        return False
    return not df[df["Type"] == TYPE_INVESTING_CONFIRM].empty


def get_goal_period_allocation(goal_id: str) -> float:
    """計算單一儲蓄目標在本期的框定金額（Saving_In）"""
    period_start, period_end = get_current_period()
    df = load_transactions()

    if df.empty:
        return 0

    # 過濾本期的 Saving_In
    mask = (
        (df["Type"] == TYPE_SAVING_IN) &
        (df["Goal_ID"] == goal_id) &
        (df["Date"].dt.date >= period_start) &
        (df["Date"].dt.date <= period_end)
    )

    return float(df[mask]["Amount"].sum())


# =============================================================================
# Phase 4: 結算相關函式
# =============================================================================

def get_previous_period() -> tuple[date, date]:
    """
    取得上一個發薪週期
    Returns: (period_start, period_end)
    """
    import calendar

    pay_day = get_pay_day()
    current_start, _ = get_current_period()

    # 上期結束日 = 本期開始日前一天
    prev_end = current_start - timedelta(days=1)

    # 計算上期開始日
    if prev_end.month == 1:
        prev_year = prev_end.year - 1
        prev_month = 12
    else:
        prev_year = prev_end.year
        prev_month = prev_end.month - 1

    # 處理月份天數不足的情況（例如發薪日 31 號但該月只有 30 天）
    last_day_of_month = calendar.monthrange(prev_year, prev_month)[1]
    actual_pay_day = min(pay_day, last_day_of_month)
    prev_start = date(prev_year, prev_month, actual_pay_day)

    return prev_start, prev_end


def check_period_settled(period_start: date) -> bool:
    """檢查指定週期是否已結算"""
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return True  # 無法檢查時預設已結算，避免卡住

    try:
        worksheet = spreadsheet.worksheet(SHEET_SETTLEMENT_LOG)
        data = worksheet.get_all_records()

        period_str = period_start.strftime("%Y-%m-%d")
        for row in data:
            if row.get("Period_Start") == period_str:
                return True
        return False
    except Exception:
        return True  # 出錯時預設已結算


def get_period_summary(period_start: date, period_end: date) -> dict:
    """
    計算指定週期的完整摘要
    Returns: {
        'income': float,           # 總收入
        'living_budget': float,    # Living 預算
        'living_expense': float,   # Living 實際支出
        'living_net': float,       # Living 結餘（正）或超支（負）
        'saving_in': float,        # Saving 累積
        'investing': float,        # Investing 確認金額
        'backup_allocate': float,  # Back Up 框定
    }
    """
    df = load_transactions()
    categories = load_categories()

    # 預設值
    result = {
        'income': 0,
        'living_budget': 0,
        'living_expense': 0,
        'living_net': 0,
        'saving_in': 0,
        'investing': 0,
        'backup_allocate': 0,
    }

    # Living 預算 = Category 加總
    if not categories.empty and "Budget" in categories.columns:
        result['living_budget'] = float(categories["Budget"].sum())

    if df.empty:
        result['living_net'] = result['living_budget']
        return result

    # 過濾該週期的交易
    mask = (df["Date"].dt.date >= period_start) & (df["Date"].dt.date <= period_end)
    period_df = df[mask]

    if period_df.empty:
        result['living_net'] = result['living_budget']
        return result

    # 收入
    result['income'] = float(period_df[period_df["Type"] == TYPE_INCOME]["Amount"].sum())

    # Living 支出
    living_expense = period_df[
        (period_df["Type"] == TYPE_EXPENSE) &
        (period_df["Account"] == ACCOUNT_LIVING)
    ]["Amount"].sum()
    result['living_expense'] = float(living_expense)

    # Living 結餘
    result['living_net'] = result['living_budget'] - result['living_expense']

    # Saving 累積
    result['saving_in'] = float(period_df[period_df["Type"] == TYPE_SAVING_IN]["Amount"].sum())

    # Investing 確認
    result['investing'] = float(period_df[period_df["Type"] == TYPE_INVESTING_CONFIRM]["Amount"].sum())

    # Back Up 框定
    backup_allocate = period_df[
        (period_df["Type"] == TYPE_ALLOCATE) &
        (period_df["Account"] == ACCOUNT_BACKUP)
    ]["Amount"].sum()
    result['backup_allocate'] = float(backup_allocate)

    return result


def execute_settlement(period_start: date, period_end: date, net_result: float) -> bool:
    """
    執行結算
    - net_result > 0: 結餘，產生 Settlement_In（進 Free Fund）
    - net_result < 0: 超支，產生 Settlement_Out（扣 Back Up）
    - net_result = 0: 不產生交易，只記錄 Settlement_Log
    """
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return False

    try:
        # 1. 產生結算交易（若非零）
        if net_result > 0:
            add_transaction(
                trans_type=TYPE_SETTLEMENT_IN,
                amount=net_result,
                account=ACCOUNT_FREEFUND,
                item="月結算結餘",
                ref=f"Settlement_{period_start.strftime('%Y%m')}"
            )
            impact_account = ACCOUNT_FREEFUND
        elif net_result < 0:
            add_transaction(
                trans_type=TYPE_SETTLEMENT_OUT,
                amount=abs(net_result),
                account=ACCOUNT_BACKUP,
                item="月結算超支",
                ref=f"Settlement_{period_start.strftime('%Y%m')}"
            )
            impact_account = ACCOUNT_BACKUP
        else:
            impact_account = "None"

        # 2. 寫入 Settlement_Log
        worksheet = spreadsheet.worksheet(SHEET_SETTLEMENT_LOG)
        settlement_id = f"STL{period_start.strftime('%Y%m')}"

        # 取得完整摘要用於記錄
        summary = get_period_summary(period_start, period_end)

        row = [
            settlement_id,                                    # Settlement_ID
            period_start.strftime("%Y-%m-%d"),                # Period_Start
            period_end.strftime("%Y-%m-%d"),                  # Period_End
            summary['living_budget'],                         # Total_Budget
            summary['living_expense'],                        # Total_Expense
            net_result,                                       # Net_Result
            impact_account,                                   # Impact_Account
            get_taiwan_now().strftime("%Y-%m-%d %H:%M:%S")      # Settled_At
        ]

        worksheet.append_row(row, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return True

    except Exception as e:
        st.error(f"結算失敗: {e}")
        return False


def execute_transfer(from_account: str, to_account: str, amount: float, note: str = "") -> bool:
    """執行帳戶轉帳"""
    if amount <= 0:
        return False

    return add_transaction(
        trans_type=TYPE_TRANSFER,
        amount=amount,
        account=from_account,
        target_account=to_account,
        item="帳戶轉帳",
        note=note,
        ref="Manual_Transfer"
    )


# =============================================================================
# UI 元件
# =============================================================================

@st.dialog("新增儲蓄目標")
def dialog_add_goal():
    """新增儲蓄目標 Dialog"""
    name = st.text_input("目標名稱 *")
    target_amount = st.number_input("目標金額 *", min_value=0, step=1000, value=0)
    deadline = st.date_input("截止日期（選填，有填 = Hard 目標）", value=None)

    col1, col2 = st.columns(2)
    with col1:
        if st.button("取消", use_container_width=True):
            st.rerun()
    with col2:
        if st.button("建立目標", type="primary", use_container_width=True):
            if not name:
                st.error("請輸入目標名稱")
            elif target_amount <= 0:
                st.error("請輸入有效金額")
            else:
                deadline_str = deadline.strftime("%Y-%m-%d") if deadline else ""
                if add_saving_goal(name, target_amount, deadline_str):
                    st.toast(f"已建立目標：{name}")
                    st.rerun()


@st.dialog("完成儲蓄目標")
def dialog_complete_goal(goal_id: str, goal_name: str, accumulated: float):
    """完成儲蓄目標 Dialog"""
    st.markdown(f"**目標：** {goal_name}")
    st.markdown(f"**累積金額：** ${accumulated:,.0f}")
    st.divider()

    actual_expense = st.number_input(
        "實際支出金額 *",
        min_value=0,
        step=100,
        value=int(accumulated)
    )

    # 計算差額
    difference = accumulated - actual_expense
    if difference > 0:
        st.success(f"差額 ${difference:,.0f} 將進入自由支配金")
    elif difference < 0:
        st.warning(f"超出累積 ${-difference:,.0f}，不會產生自由支配金")

    note = st.text_input("備註（選填）")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("取消", use_container_width=True):
            st.rerun()
    with col2:
        if st.button("確認完成", type="primary", use_container_width=True):
            if complete_saving_goal(goal_id, actual_expense, note):
                st.toast(f"已完成目標：{goal_name}")
                st.rerun()


# =============================================================================
# Phase 4: Dialog 元件
# =============================================================================

@st.dialog("月結算確認", width="large")
def dialog_settlement(period_start: date, period_end: date):
    """結算確認 Dialog"""
    summary = get_period_summary(period_start, period_end)
    net = summary['living_net']

    # 標題
    st.markdown(f"**期間：** {period_start.strftime('%Y/%m/%d')} ~ {period_end.strftime('%Y/%m/%d')}")

    st.divider()

    # Living 摘要
    st.markdown("### Living 執行狀況")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("預算", f"${summary['living_budget']:,.0f}")
    with col2:
        st.metric("實際支出", f"${summary['living_expense']:,.0f}")
    with col3:
        delta_label = "結餘" if net >= 0 else "超支"
        st.metric(delta_label, f"${net:,.0f}")

    st.divider()

    # 結算影響
    st.markdown("### 結算結果")

    backup_before = get_backup_balance()
    freefund_before = get_free_fund_balance()

    if net > 0:
        st.success(f"結餘 ${net:,.0f} 將進入 **Free Fund**")
        st.markdown(f"- Free Fund：${freefund_before:,.0f} → ${freefund_before + net:,.0f}")
    elif net < 0:
        st.warning(f"超支 ${abs(net):,.0f} 將從 **Back Up** 扣除")
        st.markdown(f"- Back Up：${backup_before:,.0f} → ${backup_before + net:,.0f}")
    else:
        st.info("本期收支平衡，無需調整帳戶")

    st.divider()

    # 按鈕
    col1, col2 = st.columns(2)
    with col1:
        if st.button("取消", use_container_width=True, key="settlement_cancel"):
            st.rerun()
    with col2:
        if st.button("確認結算", type="primary", use_container_width=True, key="settlement_confirm"):
            if execute_settlement(period_start, period_end, net):
                st.session_state["show_toast"] = "結算完成！"
                st.rerun()


@st.dialog("帳戶轉帳")
def dialog_transfer():
    """帳戶轉帳 Dialog"""

    # 帳戶選項
    accounts = [ACCOUNT_FREEFUND, ACCOUNT_BACKUP, ACCOUNT_SAVING, ACCOUNT_INVESTING, ACCOUNT_LIVING]
    account_labels = {
        ACCOUNT_FREEFUND: "Free Fund（自由支配金）",
        ACCOUNT_BACKUP: "Back Up（儲備）",
        ACCOUNT_SAVING: "Saving（儲蓄）",
        ACCOUNT_INVESTING: "Investing（投資）",
        ACCOUNT_LIVING: "Living（生活）"
    }

    # 來源帳戶
    from_account = st.selectbox(
        "從",
        accounts,
        format_func=lambda x: account_labels.get(x, x),
        key="transfer_from"
    )

    # 目標帳戶（排除已選的來源）
    to_options = [a for a in accounts if a != from_account]
    to_account = st.selectbox(
        "到",
        to_options,
        format_func=lambda x: account_labels.get(x, x),
        key="transfer_to"
    )

    # 金額
    amount = st.number_input("金額", min_value=0, step=100, value=0, key="transfer_amount")

    # 備註
    note = st.text_input("備註（選填）", key="transfer_note")

    # 警告訊息
    warnings = {
        ACCOUNT_BACKUP: "將動用緊急儲備",
        ACCOUNT_SAVING: "將影響儲蓄目標進度",
        ACCOUNT_INVESTING: "將減少投資累積",
        ACCOUNT_LIVING: "將減少本月可用預算"
    }

    if from_account in warnings:
        st.warning(warnings[from_account])

    st.divider()

    # 按鈕
    col1, col2 = st.columns(2)
    with col1:
        if st.button("取消", use_container_width=True, key="transfer_cancel"):
            st.rerun()
    with col2:
        if st.button("確認轉帳", type="primary", use_container_width=True, key="transfer_confirm"):
            if amount <= 0:
                st.error("請輸入有效金額")
            else:
                if execute_transfer(from_account, to_account, amount, note):
                    st.session_state["show_toast"] = f"已轉帳 ${amount:,.0f}"
                    st.rerun()


@st.dialog("確認本月投資")
def dialog_investing_confirm():
    """投資確認 Dialog"""
    config = load_config()
    monthly_target = float(config.get("Investing_Monthly_Target", 10000))

    st.markdown(f"**本月投資目標：** ${monthly_target:,.0f}")
    st.divider()

    # 實際投資金額
    actual_amount = st.number_input(
        "實際投資金額",
        min_value=0,
        step=1000,
        value=int(monthly_target),
        help="可填 $0，若本月有特殊狀況"
    )

    # 投資日期
    invest_date = st.date_input(
        "投資日期",
        value=get_taiwan_today()
    )

    # 備註
    note = st.text_input("備註（選填）")

    st.divider()

    # 按鈕
    col1, col2 = st.columns(2)
    with col1:
        if st.button("取消", use_container_width=True):
            st.rerun()
    with col2:
        if st.button("確認投資", type="primary", use_container_width=True):
            # 寫入 Investing_Confirm 交易
            success = add_transaction(
                trans_type=TYPE_INVESTING_CONFIRM,
                amount=float(actual_amount),
                account=ACCOUNT_INVESTING,
                item="本月投資確認",
                note=note
            )
            if success:
                st.session_state["show_toast"] = f"已確認投資 ${actual_amount:,.0f}"
                st.rerun()


@st.dialog("常用科目設定")
def dialog_quick_access_settings():
    """常用科目設定 Dialog"""
    categories = load_categories()

    if categories.empty:
        st.warning("尚無科目資料")
        return

    st.markdown("選擇最多 **4 個**常用科目：")
    st.caption("這些科目會顯示為快捷按鈕")

    st.divider()

    # 取得目前的快捷設定
    selected = []

    for _, cat in categories.iterrows():
        cat_id = cat["Category_ID"]
        cat_name = cat["Name"]
        is_quick = cat.get("Is_Quick_Access", False)

        # 處理可能的字串 "TRUE"/"FALSE"
        if isinstance(is_quick, str):
            is_quick = is_quick.upper() == "TRUE"

        checked = st.checkbox(cat_name, value=bool(is_quick), key=f"qa_{cat_id}")

        if checked:
            selected.append(cat_id)

    # 檢查數量
    if len(selected) > 4:
        st.error(f"已選擇 {len(selected)} 個，最多只能選 4 個")
        can_save = False
    else:
        st.caption(f"已選擇 {len(selected)} / 4 個")
        can_save = True

    st.divider()

    # 按鈕
    col1, col2 = st.columns(2)
    with col1:
        if st.button("取消", use_container_width=True):
            st.rerun()
    with col2:
        if st.button("儲存", type="primary", use_container_width=True, disabled=not can_save):
            # 更新 Google Sheets
            if update_quick_access(selected):
                st.session_state["show_toast"] = "已更新常用科目"
                st.rerun()


def update_quick_access(selected_ids: list) -> bool:
    """更新常用科目設定"""
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return False

    try:
        worksheet = spreadsheet.worksheet(SHEET_CATEGORY)
        all_data = worksheet.get_all_records()

        # Is_Quick_Access 在第 5 欄 (E)
        for idx, row in enumerate(all_data):
            row_number = idx + 2  # header 佔第 1 行
            cat_id = row.get("Category_ID", "")
            new_value = "TRUE" if cat_id in selected_ids else "FALSE"
            worksheet.update_cell(row_number, 5, new_value)

        st.cache_data.clear()
        return True

    except Exception as e:
        st.error(f"更新失敗: {e}")
        return False


def render_quick_expense_form():
    """快速記帳表單"""

    # 標題和設定按鈕
    col_title, col_settings = st.columns([4, 1])
    with col_title:
        st.subheader("快速記帳")
    with col_settings:
        if st.button("⚙️", help="設定常用科目"):
            dialog_quick_access_settings()

    # 載入科目和子類
    categories = load_categories()
    sub_tags = load_sub_tags()

    if categories.empty:
        st.warning("尚未設定科目，請先到 Google Sheets 設定 Category")
        return

    # 科目選擇
    category_list = categories["Name"].tolist() if "Name" in categories.columns else []
    if not category_list:
        st.warning("Category Sheet 需要 Name 欄位")
        return

    # ===== 快捷按鈕區 =====
    quick_access_cats = categories[categories["Is_Quick_Access"].apply(
        lambda x: str(x).upper() == "TRUE" if pd.notna(x) else False
    )] if "Is_Quick_Access" in categories.columns else pd.DataFrame()

    # 初始化選中的科目
    if "selected_category_id" not in st.session_state:
        st.session_state["selected_category_id"] = None

    if not quick_access_cats.empty:
        st.markdown("**常用科目：**")
        cols = st.columns(min(len(quick_access_cats), 4))

        for i, (_, cat) in enumerate(quick_access_cats.iterrows()):
            if i >= 4:
                break
            with cols[i]:
                cat_id = cat["Category_ID"]
                cat_name = cat["Name"]

                # 檢查是否被選中
                is_selected = st.session_state.get("selected_category_id") == cat_id
                button_type = "primary" if is_selected else "secondary"

                if st.button(cat_name, key=f"quick_{cat_id}", type=button_type, use_container_width=True):
                    st.session_state["selected_category_id"] = cat_id
                    st.rerun()

        st.divider()

    # ========== 科目和子類放在 form 外面 ==========
    col1, col2 = st.columns(2)

    with col1:
        # 如果有快捷選中的，設為預設
        default_index = 0
        if st.session_state.get("selected_category_id"):
            selected_cat = categories[categories["Category_ID"] == st.session_state["selected_category_id"]]
            if not selected_cat.empty:
                cat_name = selected_cat.iloc[0]["Name"]
                if cat_name in category_list:
                    default_index = category_list.index(cat_name)

        selected_category = st.selectbox("科目", category_list, index=default_index, key="category_select")

    with col2:
        # 取得選中科目的 Category_ID
        selected_cat_row = categories[categories["Name"] == selected_category]
        if not selected_cat_row.empty:
            selected_cat_id = selected_cat_row.iloc[0]["Category_ID"]
        else:
            selected_cat_id = None

        # 用 Category_ID 過濾子類
        if not sub_tags.empty and "Category_ID" in sub_tags.columns and selected_cat_id:
            category_sub_tags = sub_tags[sub_tags["Category_ID"] == selected_cat_id]
            sub_tag_list = category_sub_tags["Name"].tolist() if "Name" in category_sub_tags.columns else []
        else:
            sub_tag_list = []

        # 子類選擇
        if sub_tag_list:
            selected_sub_tag = st.selectbox(
                "子類",
                ["（不選擇）"] + sub_tag_list,
                key="sub_tag_select"
            )
            if selected_sub_tag == "（不選擇）":
                selected_sub_tag = ""
        else:
            st.markdown("**子類**")
            st.caption("無子類")
            selected_sub_tag = ""

    # ========== 金額、備註、按鈕放在 form 內 ==========
    with st.form("expense_form", clear_on_submit=True):
        item = st.text_input("品項 *")
        amount = st.number_input("金額", min_value=0, step=10, value=0)
        note = st.text_input("備註（選填）")

        submitted = st.form_submit_button("記錄支出", use_container_width=True)

        if submitted:
            if amount <= 0:
                st.error("請輸入有效金額")
            elif not item:
                st.error("請輸入品項")
            else:
                # 取得 Sub_Tag_ID（如果有選子類）
                if selected_sub_tag:
                    sub_tag_row = sub_tags[sub_tags["Name"] == selected_sub_tag]
                    sub_tag_id = sub_tag_row.iloc[0]["Sub_Tag_ID"] if not sub_tag_row.empty else ""
                else:
                    sub_tag_id = ""

                success = add_transaction(
                    trans_type=TYPE_EXPENSE,
                    amount=amount,
                    account=ACCOUNT_LIVING,
                    category_id=selected_cat_id,
                    sub_tag_id=sub_tag_id,
                    item=item,
                    note=note
                )
                if success:
                    st.toast(f"已記錄 {selected_category} ${amount}")
                    st.rerun()


def render_period_transactions():
    """顯示本期消費紀錄"""
    st.subheader("本期消費紀錄")

    df = get_period_transactions()

    if df.empty:
        st.info("本期尚無消費紀錄")
        return

    # 只顯示 Expense 類型
    expenses = df[df["Type"] == TYPE_EXPENSE].copy()

    if expenses.empty:
        st.info("本期尚無消費紀錄")
        return

    # 載入 Category 和 Sub_Tag 資料來取得名稱
    categories = load_categories()
    sub_tags = load_sub_tags()

    # JOIN Category 表取得科目名稱
    if not categories.empty and "Category_ID" in categories.columns:
        cat_mapping = categories[["Category_ID", "Name"]].copy()
        cat_mapping.columns = ["Category_ID", "Category_Name"]
        expenses = expenses.merge(cat_mapping, on="Category_ID", how="left")
        expenses["Category_Name"] = expenses["Category_Name"].fillna("")
    else:
        expenses["Category_Name"] = ""

    # JOIN Sub_Tag 表取得子類名稱
    if not sub_tags.empty and "Sub_Tag_ID" in sub_tags.columns:
        tag_mapping = sub_tags[["Sub_Tag_ID", "Name"]].copy()
        tag_mapping.columns = ["Sub_Tag_ID", "Sub_Tag_Name"]
        expenses = expenses.merge(tag_mapping, on="Sub_Tag_ID", how="left")
        expenses["Sub_Tag_Name"] = expenses["Sub_Tag_Name"].fillna("—")
    else:
        expenses["Sub_Tag_Name"] = "—"

    # 格式化顯示
    expenses = expenses.sort_values("Date", ascending=False)

    # 選擇要顯示的欄位
    display_cols = ["Date", "Category_Name", "Sub_Tag_Name", "Item", "Amount", "Note"]
    display_df = expenses[[c for c in display_cols if c in expenses.columns]].copy()

    if "Date" in display_df.columns:
        display_df["Date"] = display_df["Date"].dt.strftime("%m/%d")

    # 重新命名欄位
    display_df.columns = ["日期", "科目", "子類", "品項", "金額", "備註"][:len(display_df.columns)]

    st.dataframe(display_df, use_container_width=True, hide_index=True)


def render_status_overview():
    """狀態總覽"""
    # 取得本期資料
    period_start, period_end = get_current_period()
    days_left = get_days_left_in_period()

    # 計算本期支出
    df = get_period_transactions()
    if not df.empty:
        total_expense = df[df["Type"] == TYPE_EXPENSE]["Amount"].sum()
    else:
        total_expense = 0

    # 取得預算（從 Category 加總或 Config）
    categories = load_categories()
    if not categories.empty and "Budget" in categories.columns:
        total_budget = categories["Budget"].sum()
    else:
        config = load_config()
        total_budget = float(config.get("Living_Budget", 0))

    living_remaining = total_budget - total_expense
    daily_available = living_remaining / days_left if days_left > 0 else 0

    # 取得 Back Up 和 Free Fund 餘額
    backup_balance = get_backup_balance()
    free_fund_balance = get_free_fund_balance()

    # 取得 Back Up 上限
    config = load_config()
    backup_limit = float(config.get("Back_Up_Limit", 150000))

    # 第一行：Back Up 血量和 Free Fund
    col_backup, col_freefund = st.columns(2)

    with col_backup:
        st.markdown("**Back Up 血量**")
        progress = max(0, min(backup_balance / backup_limit, 1.0)) if backup_limit > 0 else 0
        st.progress(progress)
        if backup_balance >= 0:
            st.caption(f"${backup_balance:,.0f} / ${backup_limit:,.0f} ({progress*100:.0f}%)")
        else:
            st.warning(f"${backup_balance:,.0f} 需從其他帳戶轉帳補平")

    with col_freefund:
        st.metric("Free Fund", f"${free_fund_balance:,.0f}")

    st.divider()

    # ===== 提醒區 =====
    # 檢查是否有未結算的上期
    prev_start, prev_end = get_previous_period()
    is_settled = check_period_settled(prev_start)

    # 檢查本期投資是否已確認
    is_investing_confirmed = check_investing_confirmed_this_period()

    has_alerts = (not is_settled) or (not is_investing_confirmed)

    if has_alerts:
        st.markdown("**📌 待處理事項**")

        col1, col2 = st.columns(2)

        with col1:
            if not is_settled:
                if st.button("⚠️ 上期未結算", use_container_width=True):
                    # 導向 Tab 3（無法直接切換 Tab，改用提示）
                    st.info("請到「🧭 策略」頁面進行結算")

        with col2:
            if not is_investing_confirmed:
                monthly_target = float(config.get("Investing_Monthly_Target", 10000))
                if st.button(f"📈 確認投資 (${monthly_target:,.0f})", use_container_width=True):
                    dialog_investing_confirm()

        st.divider()

    # 第二行：本期資訊
    st.markdown(f"**本期：{period_start.strftime('%m/%d')} ~ {period_end.strftime('%m/%d')}** （剩餘 {days_left} 天）")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Living 剩餘", f"${living_remaining:,.0f}")

    with col2:
        st.metric("今日可用", f"${daily_available:,.0f}")

    with col3:
        st.metric("本期已花", f"${total_expense:,.0f}")


def render_category_progress():
    """科目進度條"""
    categories = load_categories()
    expenses_by_cat = get_living_expenses_by_category()

    if categories.empty:
        return

    if "Name" not in categories.columns or "Budget" not in categories.columns:
        return

    st.subheader("科目進度")

    for _, cat in categories.iterrows():
        cat_id = cat["Category_ID"]
        cat_name = cat["Name"]
        budget = float(cat.get("Budget", 0))

        if budget <= 0:
            continue

        # 用 Category_ID 比對
        if not expenses_by_cat.empty:
            spent_row = expenses_by_cat[expenses_by_cat["Category_ID"] == cat_id]
            spent = float(spent_row["Spent"].values[0]) if not spent_row.empty else 0
        else:
            spent = 0

        remaining = budget - spent
        progress = min(spent / budget, 1.0) if budget > 0 else 0

        warning = " ⚠️" if progress > 0.9 else ""

        col1, col2 = st.columns([3, 1])
        with col1:
            st.progress(progress, text=f"{cat_name}{warning}")
        with col2:
            st.write(f"${spent:,.0f} / ${budget:,.0f}")


# =============================================================================
# 主要頁面
# =============================================================================

def tab_expense():
    """Tab 1: 記帳"""
    render_status_overview()
    st.divider()
    render_quick_expense_form()
    st.divider()
    render_category_progress()
    st.divider()
    render_period_transactions()


def tab_goals():
    """Tab 2: 目標"""

    # ===== 投資卡片（置頂）=====
    config = load_config()
    investing_total = get_investing_total()
    long_term_target = float(config.get("Investing_Long_Term_Target", 500000))
    is_confirmed = check_investing_confirmed_this_period()

    with st.container(border=True):
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown("### 投資累積")
        with col2:
            if is_confirmed:
                st.success("本月已確認")
            else:
                st.warning("待確認")

        st.markdown(f"## ${investing_total:,.0f}")

        progress = min(investing_total / long_term_target, 1.0) if long_term_target > 0 else 0
        st.progress(progress)
        st.caption(f"長期目標 ${long_term_target:,.0f} ({progress*100:.0f}%)")

    st.divider()

    # ===== 進行中的儲蓄目標 =====
    st.markdown("### 進行中的儲蓄目標")

    goals = load_saving_goals()

    if goals.empty:
        st.info("尚無儲蓄目標")
    else:
        active_goals = goals[goals["Status"] == "Active"]
        completed_goals = goals[goals["Status"] == "Completed"]

        if active_goals.empty:
            st.info("目前沒有進行中的目標")
        else:
            for _, goal in active_goals.iterrows():
                goal_id = goal["Goal_ID"]
                goal_name = goal["Name"]
                target_amount = float(goal.get("Target_Amount", 0))
                deadline = goal.get("Deadline", "")

                # 計算即時累積（從交易記錄）
                accumulated = get_goal_accumulated(goal_id)

                # 計算本月框定
                period_allocation = get_goal_period_allocation(goal_id)

                # 判斷是否為灰色狀態（本月框定 $0）
                is_inactive = period_allocation == 0

                with st.container(border=True):
                    # 標題（灰色狀態加上提示）
                    if is_inactive:
                        st.markdown(f"#### {goal_name} 🔇")
                        st.caption("本月未框定")
                    else:
                        st.markdown(f"#### {goal_name}")

                    st.markdown(f"## ${accumulated:,.0f}")

                    # 進度條
                    progress = min(accumulated / target_amount, 1.0) if target_amount > 0 else 0
                    st.progress(progress)

                    # 目標資訊
                    info_text = f"目標 ${target_amount:,.0f} ({progress*100:.0f}%)"
                    if deadline:
                        info_text += f" | 截止 {deadline}（Hard）"
                    else:
                        info_text += " | 無截止日"
                    st.caption(info_text)

                    # 本月框定顯示
                    if period_allocation > 0:
                        st.markdown(f"**本月框定：** +${period_allocation:,.0f}")
                    else:
                        st.markdown("**本月框定：** $0")

                    # 完成按鈕
                    if st.button("完成目標", key=f"complete_{goal_id}"):
                        dialog_complete_goal(goal_id, goal_name, accumulated)

    # ===== 新增目標按鈕 =====
    st.divider()
    if st.button("新增儲蓄目標", use_container_width=True):
        dialog_add_goal()

    # ===== 已完成目標 =====
    if not goals.empty:
        completed_goals = goals[goals["Status"] == "Completed"]
        if not completed_goals.empty:
            st.divider()
            with st.expander("已完成"):
                for _, goal in completed_goals.iterrows():
                    completed_at = goal.get("Completed_At", "")
                    target = float(goal.get("Target_Amount", 0))
                    st.markdown(f"**{goal['Name']}** — ${target:,.0f} — {completed_at}")


def render_settlement_alert():
    """渲染結算提示區"""
    prev_start, prev_end = get_previous_period()
    is_settled = check_period_settled(prev_start)

    if not is_settled:
        with st.container(border=True):
            st.markdown("### 上期未結算")
            st.markdown(f"**期間：** {prev_start.strftime('%m/%d')} ~ {prev_end.strftime('%m/%d')}")

            summary = get_period_summary(prev_start, prev_end)
            net = summary['living_net']

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Living 預算", f"${summary['living_budget']:,.0f}")
            with col2:
                st.metric("實際支出", f"${summary['living_expense']:,.0f}")
            with col3:
                if net >= 0:
                    st.metric("結餘", f"${net:,.0f}", delta="→ Free Fund")
                else:
                    st.metric("超支", f"${net:,.0f}", delta="→ 扣 Back Up")

            if st.button("查看明細並確認結算", use_container_width=True, type="primary"):
                dialog_settlement(prev_start, prev_end)

        st.divider()


def render_allocation_overview():
    """渲染框定總覽"""
    st.markdown("### 框定總覽")

    config = load_config()

    # 取得本期資料
    period_start, period_end = get_current_period()
    current_summary = get_period_summary(period_start, period_end)

    # 計算各項目
    monthly_income = float(config.get("Monthly_Income", 0))
    total_income = monthly_income + current_summary['income']  # 定期 + 非定期

    living_budget = current_summary['living_budget']
    saving_in = current_summary['saving_in']
    investing_target = float(config.get("Investing_Monthly_Target", 10000))
    backup_allocate = current_summary['backup_allocate']

    total_allocate = living_budget + saving_in + investing_target + backup_allocate
    free_fund_allocate = total_income - total_allocate

    with st.container(border=True):
        # 收入
        st.markdown(f"**本期收入**")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"定期收入：${monthly_income:,.0f}")
        with col2:
            st.markdown(f"非定期收入：${current_summary['income']:,.0f}")
        st.markdown(f"**合計：${total_income:,.0f}**")

        st.divider()

        # 框定明細
        st.markdown("**框定分配**")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"Living：${living_budget:,.0f}")
            st.markdown(f"Saving：${saving_in:,.0f}")
        with col2:
            st.markdown(f"Investing：${investing_target:,.0f}")
            st.markdown(f"Back Up：${backup_allocate:,.0f}")

        st.divider()

        # 總覽
        st.markdown(f"**框定合計：${total_allocate:,.0f}**")

        if free_fund_allocate >= 0:
            st.success(f"→ Free Fund：${free_fund_allocate:,.0f}")
        else:
            st.error(f"框定超過收入 ${abs(free_fund_allocate):,.0f}")

    st.divider()


def render_account_balances():
    """渲染帳戶餘額"""
    st.markdown("### 帳戶餘額")

    config = load_config()
    backup_balance = get_backup_balance()
    freefund_balance = get_free_fund_balance()
    investing_total = get_investing_total()
    backup_limit = float(config.get("Back_Up_Limit", 150000))
    investing_target_long = float(config.get("Investing_Long_Term_Target", 500000))

    col1, col2 = st.columns(2)

    with col1:
        # Back Up
        st.markdown("**Back Up**")
        progress = max(0, min(backup_balance / backup_limit, 1.0)) if backup_limit > 0 else 0
        st.progress(progress)
        if backup_balance >= 0:
            st.caption(f"${backup_balance:,.0f} / ${backup_limit:,.0f} ({progress*100:.0f}%)")
        else:
            st.warning(f"${backup_balance:,.0f} 負數")

    with col2:
        # Free Fund
        st.metric("Free Fund", f"${freefund_balance:,.0f}")

    # Investing
    st.markdown("**Investing 累積**")
    inv_progress = min(investing_total / investing_target_long, 1.0) if investing_target_long > 0 else 0
    st.progress(inv_progress)
    st.caption(f"${investing_total:,.0f} / ${investing_target_long:,.0f} ({inv_progress*100:.0f}%)")

    st.divider()


def render_settings_and_export():
    """渲染設定與匯出"""
    config = load_config()

    # 系統設定（唯讀）
    with st.expander("系統設定"):
        st.markdown("**目前設定值：**")

        col1, col2 = st.columns(2)
        with col1:
            pay_day = config.get("Payday", config.get("Pay_Day", 5))
            st.markdown(f"發薪日：每月 **{pay_day}** 號")
            st.markdown(f"定期收入：**${float(config.get('Monthly_Income', 0)):,.0f}**")
        with col2:
            st.markdown(f"Back Up 上限：**${float(config.get('Back_Up_Limit', 150000)):,.0f}**")
            st.markdown(f"投資月目標：**${float(config.get('Investing_Monthly_Target', 10000)):,.0f}**")

        st.caption("如需修改設定，請直接編輯 Google Sheets 的 Config 表")

    # 資料匯出
    with st.expander("資料匯出"):
        df = load_transactions()
        if not df.empty:
            # 轉換日期格式
            export_df = df.copy()
            if "Date" in export_df.columns:
                export_df["Date"] = export_df["Date"].dt.strftime("%Y-%m-%d")

            csv = export_df.to_csv(index=False).encode('utf-8-sig')  # 使用 utf-8-sig 讓 Excel 正確顯示中文
            st.download_button(
                label="下載完整交易記錄 (CSV)",
                data=csv,
                file_name=f"budget_level_export_{get_taiwan_today().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.info("尚無交易記錄可匯出")


def tab_strategy():
    """Tab 3: 策略"""
    render_settlement_alert()
    render_allocation_overview()
    render_account_balances()

    st.markdown("### 帳戶轉帳")
    if st.button("進行轉帳", use_container_width=True):
        dialog_transfer()

    st.divider()
    render_settings_and_export()


# =============================================================================
# 主程式
# =============================================================================

def main():
    st.set_page_config(
        page_title="Budget Level v2",
        page_icon="💰",
        layout="wide"
    )

    st.title("Budget Level v2")
    st.caption("心理帳戶管理系統")

    # 顯示 Toast 訊息（從 session_state 讀取）
    if "show_toast" in st.session_state:
        st.toast(st.session_state["show_toast"])
        del st.session_state["show_toast"]

    # 檢查連線
    if get_spreadsheet() is None:
        st.error("無法連線到 Google Sheets，請確認 secrets.toml 設定正確")
        st.stop()

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
