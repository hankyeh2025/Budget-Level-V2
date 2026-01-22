"""
Budget Level v2 - 心理帳戶管理系統
使用信封袋理財法概念，管理五個心理帳戶
"""

import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, date
from typing import Optional

# =============================================================================
# 常數定義
# =============================================================================

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
def load_categories() -> pd.DataFrame:
    """載入 Living 科目"""
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return pd.DataFrame()
    try:
        worksheet = spreadsheet.worksheet(SHEET_CATEGORY)
        data = worksheet.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"載入科目失敗: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_sub_tags() -> pd.DataFrame:
    """載入科目子類"""
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return pd.DataFrame()
    try:
        worksheet = spreadsheet.worksheet(SHEET_SUB_TAG)
        data = worksheet.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"載入子類失敗: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_saving_goals() -> pd.DataFrame:
    """載入儲蓄目標"""
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return pd.DataFrame()
    try:
        worksheet = spreadsheet.worksheet(SHEET_SAVING_GOAL)
        data = worksheet.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"載入儲蓄目標失敗: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_transactions() -> pd.DataFrame:
    """載入所有交易記錄"""
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return pd.DataFrame()
    try:
        worksheet = spreadsheet.worksheet(SHEET_TRANSACTION)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
        if not df.empty and "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        return df
    except Exception as e:
        st.error(f"載入交易記錄失敗: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_config() -> dict:
    """載入系統設定"""
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return {}
    try:
        worksheet = spreadsheet.worksheet(SHEET_CONFIG)
        data = worksheet.get_all_records()
        if data:
            # 假設 Config 是 key-value 格式
            config = {}
            for row in data:
                if "Key" in row and "Value" in row:
                    config[row["Key"]] = row["Value"]
            return config
        return {}
    except Exception as e:
        st.error(f"載入設定失敗: {e}")
        return {}


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
        trans_id = f"TXN{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # 建立交易資料 - 對齊 Sheet 欄位順序
        # Txn_ID | Timestamp | Date | Type | Amount | Account | Category_ID | Sub_Tag_ID | Goal_ID | Target_Account | Item | Note | Ref
        row = [
            trans_id,                                      # Txn_ID
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Timestamp (完整時間)
            datetime.now().strftime("%Y-%m-%d"),           # Date (只有日期)
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


# =============================================================================
# 工具函式
# =============================================================================

def get_pay_day() -> int:
    """取得發薪日（預設 5 號）"""
    config = load_config()
    return int(config.get("Pay_Day", 5))


def get_current_period() -> tuple[date, date]:
    """取得當前發薪週期的起始和結束日期"""
    pay_day = get_pay_day()
    today = date.today()

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
    today = date.today()
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


# =============================================================================
# UI 元件
# =============================================================================

def render_quick_expense_form():
    """快速記帳表單"""
    st.subheader("快速記帳")

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

    # ========== 科目和子類放在 form 外面 ==========
    col1, col2 = st.columns(2)

    with col1:
        selected_category = st.selectbox("科目", category_list, key="category_select")

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

    # 格式化顯示
    expenses = expenses.sort_values("Date", ascending=False)

    # 選擇要顯示的欄位
    display_cols = ["Date", "Category", "Sub_Tag", "Amount", "Note"]
    display_df = expenses[[c for c in display_cols if c in expenses.columns]].copy()

    if "Date" in display_df.columns:
        display_df["Date"] = display_df["Date"].dt.strftime("%m/%d")

    # 重新命名欄位
    display_df.columns = ["日期", "科目", "子類", "金額", "備註"][:len(display_df.columns)]

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

    # 顯示狀態卡片
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

        col1, col2 = st.columns([3, 1])
        with col1:
            st.progress(progress, text=f"{cat_name}")
        with col2:
            st.write(f"${remaining:,.0f}")


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
    """Tab 2: 目標（Phase 3 實作）"""
    st.subheader("目標管理")
    st.info("此功能將在 Phase 3 實作")

    # 預留：顯示儲蓄目標
    goals = load_saving_goals()
    if not goals.empty:
        st.dataframe(goals, use_container_width=True)
    else:
        st.write("尚無儲蓄目標")


def tab_strategy():
    """Tab 3: 策略（Phase 4 實作）"""
    st.subheader("策略管理")
    st.info("此功能將在 Phase 4 實作")

    # 預留：顯示設定
    config = load_config()
    if config:
        st.json(config)
    else:
        st.write("尚無系統設定")


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
