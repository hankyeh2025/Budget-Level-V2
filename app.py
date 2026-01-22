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


def add_saving_goal(name: str, target_amount: float, deadline: str = "") -> bool:
    """新增儲蓄目標"""
    spreadsheet = get_spreadsheet()
    if spreadsheet is None:
        return False

    try:
        worksheet = spreadsheet.worksheet(SHEET_SAVING_GOAL)

        # 產生 Goal_ID
        goal_id = f"GOAL{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # 欄位順序：Goal_ID | Name | Target_Amount | Deadline | Accumulated | Status | Created_At | Completed_At
        row = [
            goal_id,
            name,
            target_amount,
            deadline,  # 空字串 = 無截止日
            0,  # Accumulated (初始為 0)
            "Active",
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
                worksheet.update_cell(row_number, 8, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # Completed_At
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

                with st.container(border=True):
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
