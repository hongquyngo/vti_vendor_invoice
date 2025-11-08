# pages/1_📝_Create_Invoice.py

import streamlit as st
import pandas as pd
from datetime import datetime, date
import time
import logging
from typing import Dict, Set, Optional, List
from dataclasses import dataclass, field

# Import utils
from utils.auth import AuthManager
from utils.invoice_data import (
    get_uninvoiced_ans, 
    get_filter_options,
    get_invoice_details,
    validate_invoice_selection,
    create_purchase_invoice,
    generate_invoice_number,
    get_payment_terms,
    calculate_days_from_term_name,
    get_po_line_summary
)
from utils.invoice_service import InvoiceService
from utils.currency_utils import (
    get_available_currencies,
    calculate_exchange_rates,
    validate_exchange_rates,
    format_exchange_rate,
    get_invoice_amounts_in_currency
)
from utils.invoice_attachments import (
    validate_uploaded_files,
    prepare_files_for_upload,
    format_file_size,
    get_file_icon,
    summarize_files,
    save_media_records,
    cleanup_failed_uploads
)
from utils.s3_utils import S3Manager

# Setup logging
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Create Purchase Invoice",
    page_icon="📄",
    layout="wide"
)

# ============================================================================
# STATE MANAGEMENT CLASSES
# ============================================================================

@dataclass
class InvoiceState:
    """Data class for invoice creation state"""
    selected_ans: Set[int] = field(default_factory=set)
    wizard_step: str = 'select'
    current_page: int = 1
    items_per_page: int = 50
    invoice_data: Optional[Dict] = None
    details_df: Optional[pd.DataFrame] = None
    selected_df: Optional[pd.DataFrame] = None
    is_advance_payment: bool = False
    show_po_analysis: bool = True  # CHANGED: Default to True
    hide_completed_po_lines: bool = True  # NEW: Hide completed PO lines by default
    invoice_creating: bool = False
    last_created_invoice: Optional[Dict] = None
    filters: Dict = field(default_factory=dict)
    selected_payment_term: Optional[str] = None
    invoice_date: date = field(default_factory=date.today)
    invoice_currency_id: Optional[int] = None
    invoice_currency_code: Optional[str] = None
    exchange_rates: Optional[Dict] = None
    invoice_totals: Optional[Dict] = None
    commercial_invoice_no: Optional[str] = None
    email_to_accountant: bool = False
    due_date: Optional[date] = None
    payment_term_id: Optional[int] = None
    # NEW: File attachment fields
    uploaded_files: List = field(default_factory=list)
    uploaded_files_metadata: List[Dict] = field(default_factory=list)
    upload_errors: List[str] = field(default_factory=list)
    s3_upload_success: bool = False
    s3_keys: List[str] = field(default_factory=list)
    media_ids: List[int] = field(default_factory=list)


class StateManager:
    """Centralized session state management"""
    
    @staticmethod
    def initialize():
        """Initialize session state with defaults"""
        if 'invoice_state' not in st.session_state:
            st.session_state.invoice_state = InvoiceState()
        
        # Ensure auth state persists
        persistent_keys = ['username', 'authenticated', 'role']
        for key in persistent_keys:
            if key not in st.session_state:
                st.session_state[key] = None
    
    @staticmethod
    def get_state() -> InvoiceState:
        """Get current invoice state"""
        if 'invoice_state' not in st.session_state:
            StateManager.initialize()
        return st.session_state.invoice_state
    
    @staticmethod
    def reset_wizard():
        """Reset wizard state while keeping filters and auth"""
        state = StateManager.get_state()
        state.wizard_step = 'select'
        state.selected_ans = set()
        state.invoice_data = None
        state.details_df = None
        state.selected_df = None
        state.is_advance_payment = False
        state.invoice_creating = False
        state.selected_payment_term = None
        state.invoice_date = date.today()
        state.invoice_currency_id = None
        state.invoice_currency_code = None
        state.exchange_rates = None
        state.invoice_totals = None
        state.commercial_invoice_no = None
        state.email_to_accountant = False
        state.due_date = None
        state.payment_term_id = None
        # Clear attachment fields
        state.uploaded_files = []
        state.uploaded_files_metadata = []
        state.upload_errors = []
        state.s3_upload_success = False
        state.s3_keys = []
        state.media_ids = []
    
    @staticmethod
    def reset_filters():
        """Reset all filters"""
        state = StateManager.get_state()
        state.filters = {}
        state.current_page = 1
        state.selected_ans = set()
    
    @staticmethod
    def get_selected_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Get DataFrame of selected items"""
        state = StateManager.get_state()
        if not state.selected_ans:
            return pd.DataFrame()
        
        selected_ids = list(state.selected_ans)
        selected_df = df[df['can_line_id'].isin(selected_ids)].copy()
        
        # Ensure no duplicates
        if not selected_df['can_line_id'].is_unique:
            logger.warning(f"Duplicate can_line_ids found: {selected_df['can_line_id'].duplicated().sum()}")
            selected_df = selected_df.drop_duplicates(subset=['can_line_id'])
        
        return selected_df

# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    # Initialize
    StateManager.initialize()
    auth = AuthManager()
    auth.require_auth()
    
    state = StateManager.get_state()
    
    st.title("📄 Create Purchase Invoice")
    
    # Progress indicator
    show_progress_indicator()
    
    # Route to appropriate step
    if state.wizard_step == 'select':
        show_an_selection()
    elif state.wizard_step == 'preview':
        show_invoice_preview()
    elif state.wizard_step == 'confirm':
        show_invoice_confirm()

def show_progress_indicator():
    """Show wizard progress"""
    state = StateManager.get_state()
    
    steps = {
        'select': 1,
        'preview': 2,
        'confirm': 3
    }
    
    current_step = steps.get(state.wizard_step, 1)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if current_step >= 1:
            st.success("✅ Step 1: Select ANs")
        else:
            st.info("⭕ Step 1: Select ANs")
    
    with col2:
        if current_step >= 2:
            st.success("✅ Step 2: Review Invoice")
        elif current_step == 2:
            st.info("🔵 Step 2: Review Invoice")
        else:
            st.info("⭕ Step 2: Review Invoice")
    
    with col3:
        if current_step >= 3:
            st.success("✅ Step 3: Confirm & Submit")
        elif current_step == 3:
            st.info("🔵 Step 3: Confirm & Submit")
        else:
            st.info("⭕ Step 3: Confirm & Submit")
    
    st.markdown("---")

# ============================================================================
# STEP 1: AN SELECTION
# ============================================================================

def show_an_selection():
    """Step 1: AN Selection with improved state management"""
    state = StateManager.get_state()
    service = InvoiceService()
    
    # Show success message if just created an invoice
    if state.last_created_invoice:
        invoice_info = state.last_created_invoice
        st.success(f"""
        ✅ **Invoice Created Successfully!**
        - Invoice Number: {invoice_info['number']}
        - Invoice ID: {invoice_info['id']}
        - Total Amount: {invoice_info['amount']:,.2f} {invoice_info['currency']}
        """)
        
        state.last_created_invoice = None
        st.markdown("---")
    
    # Filters section
    show_filters()
    
    # Get data with filters
    df = get_uninvoiced_ans(state.filters)
    
    # Display results with pagination
    display_an_results(df)
    
    # Summary and actions
    if state.selected_ans:
        show_selection_summary(df, service)

def show_filters():
    """Display filter controls"""
    state = StateManager.get_state()
    
    with st.expander("🔍 Filters", expanded=True):
        filter_options = get_filter_options()
        
        # Row 1: Vendor, Legal Entity
        col1, col2 = st.columns(2)
        
        with col1:
            vendor_options = [f"{code} - {name}" for code, name in filter_options['vendors']]
            selected_vendors = st.multiselect(
                "Vendor",
                options=vendor_options,
                placeholder="Choose an option",
                key="filter_vendor_multi"
            )
            if selected_vendors:
                state.filters['vendors'] = [v.split(' - ')[0] for v in selected_vendors]
            elif 'vendors' in state.filters:
                del state.filters['vendors']
        
        with col2:
            entity_options = [f"{code} - {name}" for code, name in filter_options['entities']]
            selected_entities = st.multiselect(
                "Legal Entity",
                options=entity_options,
                placeholder="Choose an option",
                key="filter_entity_multi"
            )
            if selected_entities:
                state.filters['entities'] = [e.split(' - ')[0] for e in selected_entities]
            elif 'entities' in state.filters:
                del state.filters['entities']
        
        # Row 2: AN Number, PO Number, Creator, Brand
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            an_numbers = st.multiselect(
                "Search AN Number",
                options=filter_options['an_numbers'],
                placeholder="Choose an option",
                key="filter_an_multi"
            )
            if an_numbers:
                state.filters['an_numbers'] = an_numbers
            elif 'an_numbers' in state.filters:
                del state.filters['an_numbers']
        
        with col2:
            po_numbers = st.multiselect(
                "Search PO Number", 
                options=filter_options['po_numbers'],
                placeholder="Choose an option",
                key="filter_po_multi"
            )
            if po_numbers:
                state.filters['po_numbers'] = po_numbers
            elif 'po_numbers' in state.filters:
                del state.filters['po_numbers']
        
        with col3:
            creators = st.multiselect(
                "Creator",
                options=filter_options['creators'],
                placeholder="Choose an option",
                key="filter_creator_multi"
            )
            if creators:
                state.filters['creators'] = creators
            elif 'creators' in state.filters:
                del state.filters['creators']
        
        with col4:
            brands = st.multiselect(
                "Brand",
                options=filter_options['brands'],
                placeholder="Choose an option",
                key="filter_brand_multi"
            )
            if brands:
                state.filters['brands'] = brands
            elif 'brands' in state.filters:
                del state.filters['brands']
        
        # Date Filters Section
        st.markdown("##### Date Filters")
        col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 0.5])
        
        with col1:
            arrival_date_from = st.date_input(
                "Arrival From",
                value=None,
                key="filter_arrival_from_date"
            )
            if arrival_date_from:
                state.filters['arrival_date_from'] = arrival_date_from
            elif 'arrival_date_from' in state.filters:
                del state.filters['arrival_date_from']
        
        with col2:
            arrival_date_to = st.date_input(
                "Arrival To",
                value=None,
                key="filter_arrival_to_date"
            )
            if arrival_date_to:
                state.filters['arrival_date_to'] = arrival_date_to
            elif 'arrival_date_to' in state.filters:
                del state.filters['arrival_date_to']
        
        with col3:
            created_date_from = st.date_input(
                "Created From",
                value=None,
                key="filter_created_from_date"
            )
            if created_date_from:
                state.filters['created_date_from'] = created_date_from
            elif 'created_date_from' in state.filters:
                del state.filters['created_date_from']
        
        with col4:
            created_date_to = st.date_input(
                "Created To",
                value=None,
                key="filter_created_to_date"
            )
            if created_date_to:
                state.filters['created_date_to'] = created_date_to
            elif 'created_date_to' in state.filters:
                del state.filters['created_date_to']
        
        with col5:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("🔄 Reset Filters", use_container_width=True):
                StateManager.reset_filters()
                st.rerun()

def display_an_results(df: pd.DataFrame):
    """Display AN results with pagination and filtering"""
    state = StateManager.get_state()
    
    # NEW: Filter out completed PO lines if enabled
    if state.hide_completed_po_lines:
        # Only filter if PO analysis columns exist
        if 'po_line_pending_invoiced_qty' in df.columns:
            original_count = len(df)
            df = df[df['po_line_pending_invoiced_qty'] > 0].copy()
            filtered_count = original_count - len(df)
            if filtered_count > 0:
                st.info(f"ℹ️ Hiding {filtered_count} AN line(s) from completed PO lines (PO Pend = 0)")
    
    total_items = len(df)
    
    # Header with controls
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    
    with col1:
        st.markdown(f"### 📊 Available ANs ({total_items} items)")
    
    with col2:
        items_per_page_options = [25, 50, 100, 200]
        items_per_page = st.selectbox(
            "Items per page",
            options=items_per_page_options,
            index=items_per_page_options.index(state.items_per_page),
            key="items_per_page_selector"
        )
        if items_per_page != state.items_per_page:
            state.items_per_page = items_per_page
            state.current_page = 1
            st.rerun()
    
    with col3:
        state.show_po_analysis = st.checkbox(
            "Show PO Analysis",
            value=state.show_po_analysis,
            help="Display detailed PO line level information including quantities and legacy invoices"
        )
    
    with col4:
        # NEW: Checkbox to hide completed PO lines
        state.hide_completed_po_lines = st.checkbox(
            "Hide Completed POs",
            value=state.hide_completed_po_lines,
            help="Hide AN lines where PO has been fully invoiced (PO Pend = 0)"
        )
    
    if df.empty:
        st.info("No uninvoiced ANs found with the selected filters.")
        return
    
    # Calculate pagination
    total_pages = max(1, (total_items + state.items_per_page - 1) // state.items_per_page)
    
    # Ensure current page is valid
    state.current_page = max(1, min(state.current_page, total_pages))
    
    # Get current page data
    start_idx = (state.current_page - 1) * state.items_per_page
    end_idx = min(start_idx + state.items_per_page, total_items)
    page_df = df.iloc[start_idx:end_idx]
    
    # Display table
    display_an_table(page_df)
    
    # Pagination controls
    display_pagination_controls(total_pages)

def display_an_table(page_df: pd.DataFrame):
    """Display the AN table with selection - FIXED for Streamlit Cloud"""
    state = StateManager.get_state()
    
    with st.container():
        # Determine columns based on view mode
        if state.show_po_analysis:
            # Increase AN Number column width from 1 to 1.3
            cols = st.columns([0.5, 1.3, 1, 1.5, 1.5, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.9, 0.9])
        else:
            cols = st.columns([0.5, 1.2, 1.2, 2, 2, 1.2, 1, 1, 1, 1, 1.5])
        
        # Calculate current page selection state
        page_ids = page_df['can_line_id'].tolist()
        page_selected = [id for id in page_ids if id in state.selected_ans]
        is_all_selected = len(page_selected) == len(page_ids) and len(page_ids) > 0
        
        # Store page_ids in session state for callback access
        st.session_state.current_page_ids = page_ids
        
        # Callback for select all checkbox
        def handle_select_all_change():
            """Callback for select all checkbox"""
            state = StateManager.get_state()
            page_ids = st.session_state.current_page_ids
            
            if st.session_state.get('select_all_checkbox'):
                # Select all items on current page
                state.selected_ans.update(page_ids)
            else:
                # Deselect all items on current page
                state.selected_ans -= set(page_ids)
        
        # Select all checkbox with callback
        cols[0].checkbox(
            "All",
            key="select_all_checkbox",
            value=is_all_selected,
            on_change=handle_select_all_change
        )
        
        # Column headers
        if state.show_po_analysis:
            display_po_analysis_headers(cols[1:])
        else:
            display_standard_headers(cols[1:])
        
        st.markdown("---")
        
        # Data rows
        for idx, row in page_df.iterrows():
            if state.show_po_analysis:
                display_row_with_po_analysis(row)
            else:
                display_standard_row(row)

def display_standard_headers(cols):
    """Display standard column headers"""
    cols[0].markdown("**AN Number**")
    cols[1].markdown("**PO Number**")
    cols[2].markdown("**Vendor**")
    cols[3].markdown("**Product**")
    cols[4].markdown("**Uninv Qty**")
    cols[5].markdown("**Unit Cost**")
    cols[6].markdown("**VAT**")
    cols[7].markdown("**Est. Value**")
    cols[8].markdown("**Payment**")
    cols[9].markdown("**PO Status**")

def display_po_analysis_headers(cols):
    """Display PO analysis column headers with tooltips"""
    cols[0].markdown("**AN Number**")
    cols[1].markdown("**PO Number**")
    cols[2].markdown("**Vendor**")
    cols[3].markdown("**Product**")
    
    # NEW: Add tooltips to quantity columns
    cols[4].markdown(
        "**PO Qty**",
        help="Original PO quantity (buying UOM)\nFormula: ppo.purchase_quantity"
    )
    cols[5].markdown(
        "**PO Pend**",
        help="PO remaining to invoice (all ANs)\nFormula: PO Qty - Total Invoiced at PO level\nIncludes legacy invoices"
    )
    cols[6].markdown(
        "**AN Uninv**",
        help="AN uninvoiced quantity (this AN only)\nFormula: Arrival Qty - Invoiced from this AN"
    )
    cols[7].markdown(
        "**Legacy**",
        help="Legacy invoices (not linked to AN)\nFormula: SUM(invoiced_qty WHERE arrival_detail_id IS NULL)"
    )
    cols[8].markdown(
        "**True Qty**",
        help="Actual quantity can be invoiced\nFormula: GREATEST(0, LEAST(AN Uninv, PO Pend))\nCannot exceed PO remaining quota"
    )
    
    cols[9].markdown("**Unit Cost**")
    cols[10].markdown("**VAT**")
    cols[11].markdown("**Est. Value**")
    cols[12].markdown(
        "**Status/Risk**",
        help="⚠️LEG: Has legacy invoices\n⚠️ADJ: True Qty < AN Uninv\n⚠️EXC: PO Pend < AN Uninv\n🔴OI: Over-invoiced\n🔴OD: Over-delivered\n✅OK: Normal"
    )

def display_standard_row(row):
    """Display standard row with FIXED checkbox handling"""
    state = StateManager.get_state()
    cols = st.columns([0.5, 1.2, 1.2, 2, 2, 1.2, 1, 1, 1, 1, 1.5])
    
    # Unique key for checkbox
    checkbox_key = f"cb_{row['can_line_id']}"
    
    # Check if item is selected
    is_selected = row['can_line_id'] in state.selected_ans
    
    # Store row ID in session state for callback access
    st.session_state[f"row_id_{checkbox_key}"] = row['can_line_id']
    
    # Callback for individual checkbox
    def handle_selection_change():
        """Callback for individual checkbox"""
        state = StateManager.get_state()
        row_id = st.session_state[f"row_id_{checkbox_key}"]
        
        if st.session_state.get(checkbox_key):
            state.selected_ans.add(row_id)
        else:
            state.selected_ans.discard(row_id)
    
    # Checkbox with callback
    cols[0].checkbox(
        "Select",
        key=checkbox_key,
        value=is_selected,
        on_change=handle_selection_change,
        label_visibility="collapsed"
    )
    
    # Display row data
    cols[1].text(row['arrival_note_number'])
    cols[2].text(row['po_number'])
    cols[3].text(f"{row['vendor_code']} - {row['vendor'][:20]}")
    cols[4].text(f"{row['pt_code']} - {row['product_name'][:20]}")
    # IMPROVED: Add thousand separator for quantity
    cols[5].text(f"{row['uninvoiced_quantity']:,.2f} {row['buying_uom']}")
    cols[6].text(row['buying_unit_cost'])
    
    vat_percent = row.get('vat_percent', 0)
    cols[7].text(f"{vat_percent:.0f}%")
    
    currency = row['buying_unit_cost'].split()[-1] if ' ' in str(row['buying_unit_cost']) else 'USD'
    cols[8].text(f"{row['estimated_invoice_value']:,.2f} {currency}")
    cols[9].text(row.get('payment_term', 'N/A'))
    
    po_status = row.get('po_line_status', 'UNKNOWN')
    status_color = get_status_color(po_status)
    
    indicators = []
    if row.get('po_line_is_over_delivered') == 'Y':
        indicators.append('OD')
    if row.get('po_line_is_over_invoiced') == 'Y':
        indicators.append('OI')
    if row.get('has_legacy_invoices') == 'Y':
        indicators.append('LEG')
    
    status_text = f"{status_color} {po_status[:8]}"
    if indicators:
        status_text += f" ({','.join(indicators)})"
    
    cols[10].text(status_text)

def display_row_with_po_analysis(row):
    """Display row with PO analysis - FIXED checkbox handling"""
    state = StateManager.get_state()
    # Increase AN Number column width from 1 to 1.3
    cols = st.columns([0.5, 1.3, 1, 1.5, 1.5, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.9, 0.9])
    
    # Unique key for checkbox
    checkbox_key = f"cb_{row['can_line_id']}"
    is_selected = row['can_line_id'] in state.selected_ans
    
    # Store row ID for callback access
    st.session_state[f"row_id_{checkbox_key}"] = row['can_line_id']
    
    # Callback for individual checkbox
    def handle_selection_change():
        """Callback for individual checkbox"""
        state = StateManager.get_state()
        row_id = st.session_state[f"row_id_{checkbox_key}"]
        
        if st.session_state.get(checkbox_key):
            state.selected_ans.add(row_id)
        else:
            state.selected_ans.discard(row_id)
    
    # Checkbox with callback
    cols[0].checkbox(
        "Select",
        key=checkbox_key,
        value=is_selected,
        on_change=handle_selection_change,
        label_visibility="collapsed"
    )
    
    # Display row data
    # IMPROVED: Show AN number prioritizing part after "-"
    an_number = row['arrival_note_number']
    if '-' in an_number:
        # Split and show prefix + last part (numbers after last -)
        parts = an_number.split('-')
        if len(parts) >= 2:
            # Show first part and last part: "AN2025117-001"
            cols[1].text(f"{parts[0]}-{parts[-1]}")
        else:
            cols[1].text(an_number)
    else:
        cols[1].text(an_number)
    
    cols[2].text(row['po_number'][:10])
    cols[3].text(f"{row['vendor_code'][:3]}-{row['vendor'][:12]}")
    cols[4].text(f"{row['pt_code'][:8]}-{row['product_name'][:12]}")
    
    po_qty = row.get('po_buying_quantity', 0)
    po_pending = row.get('po_line_pending_invoiced_qty', 0)
    an_uninv = row['uninvoiced_quantity']
    legacy_qty = row.get('legacy_invoice_qty', 0)
    true_remaining = row.get('true_remaining_qty', an_uninv)
    
    # IMPROVED: Add thousand separator for quantities
    cols[5].text(f"{po_qty:,.0f}")
    cols[6].text(f"{po_pending:,.0f}")
    cols[7].text(f"{an_uninv:,.0f}")
    cols[8].text(f"{legacy_qty:,.0f}" if legacy_qty > 0 else "-")
    cols[9].text(f"{true_remaining:,.0f}")
    
    # IMPROVED: Format unit cost with thousand separator
    unit_cost_str = row['buying_unit_cost']
    if ' ' in str(unit_cost_str):
        cost_parts = unit_cost_str.split()
        cost_value = float(cost_parts[0])
        cols[10].text(f"{cost_value:,.0f}")
    else:
        cols[10].text(unit_cost_str)
    
    vat_percent = row.get('vat_percent', 0)
    cols[11].text(f"{vat_percent:.0f}%")
    
    # Already has thousand separator
    cols[12].text(f"{row['estimated_invoice_value']:,.0f}")
    
    # Risk indicators
    risk_status = []
    if row.get('po_line_is_over_delivered') == 'Y':
        risk_status.append("🔴OD")
    if row.get('po_line_is_over_invoiced') == 'Y':
        risk_status.append("🔴OI")
    if legacy_qty > 0:
        risk_status.append("⚠️LEG")
    if true_remaining < an_uninv:
        risk_status.append("⚠️ADJ")
    if po_pending < an_uninv:
        risk_status.append("⚠️EXC")
    
    cols[13].text(" ".join(risk_status) if risk_status else "✅OK")

def get_status_color(status: str) -> str:
    """Get status color emoji"""
    return {
        'COMPLETED': '🟢',
        'OVER_DELIVERED': '🔴',
        'PENDING': '⚪',
        'PENDING_INVOICING': '🟡',
        'PENDING_RECEIPT': '🟠',
        'IN_PROCESS': '🔵',
        'UNKNOWN_STATUS': '⚫'
    }.get(status, '⚫')

def display_pagination_controls(total_pages: int):
    """Display pagination controls"""
    state = StateManager.get_state()
    
    st.markdown("---")
    col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
    
    with col1:
        if st.button("⮜ First", disabled=state.current_page == 1, use_container_width=True):
            state.current_page = 1
            st.rerun()
    
    with col2:
        if st.button("◀️ Previous", disabled=state.current_page == 1, use_container_width=True):
            state.current_page -= 1
            st.rerun()
    
    with col3:
        st.markdown(
            f"<div style='text-align: center; padding: 8px;'>Page {state.current_page} of {total_pages}</div>",
            unsafe_allow_html=True
        )
    
    with col4:
        if st.button("Next ▶️", disabled=state.current_page == total_pages, use_container_width=True):
            state.current_page += 1
            st.rerun()
    
    with col5:
        if st.button("Last ⮞", disabled=state.current_page == total_pages, use_container_width=True):
            state.current_page = total_pages
            st.rerun()

def show_selection_summary(df: pd.DataFrame, service: InvoiceService):
    """Show summary of selected items"""
    state = StateManager.get_state()
    
    selected_df = StateManager.get_selected_dataframe(df)
    
    if selected_df.empty:
        return
    
    # Calculate totals
    if 'product_purchase_order_id' in selected_df.columns:
        po_line_ids = selected_df['product_purchase_order_id'].unique().tolist()
        po_summary_df = get_po_line_summary(po_line_ids)
    else:
        po_summary_df = pd.DataFrame()
    
    totals = service.calculate_invoice_totals(selected_df)
    
    # Display metrics
    st.markdown("---")
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Selected ANs", totals['an_count'])
    col2.metric("Total Quantity", f"{totals['total_quantity']:,.2f}")
    col3.metric("Total Lines", totals['total_lines'])
    col4.metric("Est. Total Value", f"{totals['total_value']:,.2f} {totals['currency']}")
    
    if 'vat_amount' in selected_df.columns:
        total_vat = selected_df['vat_amount'].sum()
        col5.metric("Total VAT", f"{total_vat:,.2f} {totals['currency']}")
    
    # Show warnings
    payment_terms = selected_df['payment_term'].dropna().unique()
    if len(payment_terms) > 1:
        st.warning(f"⚠️ Multiple payment terms found: {', '.join(payment_terms)}. The most common term will be used.")
    
    vat_rates = selected_df['vat_percent'].unique()
    if len(vat_rates) > 1:
        st.info(f"ℹ️ Multiple VAT rates found: {', '.join([f'{v:.0f}%' for v in vat_rates])}. Each line will retain its respective VAT rate.")
    
    # Validate selection
    is_valid, error_msg = validate_invoice_selection(selected_df)
    
    st.markdown("---")
    if not is_valid:
        st.error(f"❌ {error_msg}")
    else:
        validation_result, validation_msgs = service.validate_invoice_with_po_level(selected_df)
        
        if not validation_result['can_invoice']:
            st.error(f"❌ {validation_msgs['error']}")
        else:
            if validation_msgs.get('warnings'):
                for warning in validation_msgs['warnings']:
                    st.warning(f"⚠️ {warning}")
            
            st.success("✅ Selected items can be invoiced together")
            
            if st.button("➡️ Proceed to Preview", type="primary", use_container_width=True):
                # Store selected dataframe
                state.selected_df = selected_df
                state.wizard_step = 'preview'
                st.rerun()

# ============================================================================
# STEP 2: INVOICE PREVIEW
# ============================================================================

def show_invoice_preview():
    """Step 2: Invoice Preview"""
    state = StateManager.get_state()
    service = InvoiceService()
    
    # Validate data exists
    if state.selected_df is None or state.selected_df.empty:
        st.error("No data found. Please go back and select ANs.")
        if st.button("⬅️ Back to Selection"):
            state.wizard_step = 'select'
            st.rerun()
        return
    
    # Get invoice details
    unique_can_ids = list(state.selected_ans)
    
    with st.spinner("Loading invoice details..."):
        details_df = get_invoice_details(unique_can_ids)
    
    if details_df.empty:
        st.error("Could not load invoice details. Please try again.")
        if st.button("⬅️ Back to Selection"):
            state.wizard_step = 'select'
            st.rerun()
        return
    
    # Ensure no duplicates
    details_df = details_df.drop_duplicates(subset=['arrival_detail_id'])
    state.details_df = details_df
    
    # Get currency info
    po_currency_id = details_df['po_currency_id'].iloc[0] if not details_df.empty else 1
    po_currency_code = details_df['po_currency_code'].iloc[0] if not details_df.empty else 'USD'
    
    st.markdown("### 📄 Invoice Information")
    
    # Initialize payment term if needed
    if state.selected_payment_term is None:
        unique_payment_terms = state.selected_df['payment_term'].dropna().unique().tolist()
        if unique_payment_terms:
            most_common = state.selected_df['payment_term'].mode()
            state.selected_payment_term = most_common.iloc[0] if not most_common.empty else unique_payment_terms[0]
        else:
            state.selected_payment_term = 'Net 30'
    
    # Invoice type selection
    col1, col2 = st.columns(2)
    
    with col1:
        advance_payment = st.checkbox(
            "Advance Payment Invoice",
            value=state.is_advance_payment,
            help="Check this for advance payment invoices (PI). This will change the invoice number suffix."
        )
        
        if advance_payment != state.is_advance_payment:
            state.is_advance_payment = advance_payment
            st.rerun()
    
    # Generate invoice number
    vendor_id = details_df['vendor_id'].iloc[0] if not details_df.empty else None
    buyer_id = details_df['entity_id'].iloc[0] if not details_df.empty else None
    invoice_number = generate_invoice_number(vendor_id, buyer_id, state.is_advance_payment)
    
    with col2:
        if state.is_advance_payment:
            st.info("🔵 **Invoice Type: Advance Payment (PI)**")
        else:
            st.success("🟢 **Invoice Type: Commercial Invoice (CI)**")
    
    # Currency selection
    show_currency_selection(po_currency_code, po_currency_id)
    
    # Payment terms
    show_payment_terms()
    
    # Invoice form
    show_invoice_form(invoice_number, po_currency_code, service)

def show_currency_selection(po_currency_code: str, po_currency_id: int):
    """Show currency selection and exchange rates"""
    state = StateManager.get_state()
    
    st.markdown("### 💱 Currency Selection")
    
    currencies_df = get_available_currencies()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info(f"**PO Currency:** {po_currency_code}")
    
    with col2:
        currency_options = currencies_df['code'].tolist()
        currency_display = [f"{row['code']} - {row['name']}" for _, row in currencies_df.iterrows()]
        
        default_index = 0
        if po_currency_code in currency_options:
            default_index = currency_options.index(po_currency_code)
        elif 'USD' in currency_options:
            default_index = currency_options.index('USD')
        
        selected_currency_display = st.selectbox(
            "Invoice Currency",
            options=currency_display,
            index=default_index,
            help="Select the currency for this invoice"
        )
        
        invoice_currency_code = selected_currency_display.split(' - ')[0]
        invoice_currency_id = currencies_df[currencies_df['code'] == invoice_currency_code]['id'].iloc[0]
        
        state.invoice_currency_code = invoice_currency_code
        state.invoice_currency_id = invoice_currency_id
    
    with col3:
        # Calculate exchange rates
        with st.spinner("Fetching exchange rates..."):
            rates = calculate_exchange_rates(po_currency_code, invoice_currency_code)
        
        # Validate rates
        rates_valid, rate_warnings = validate_exchange_rates(rates, po_currency_code, invoice_currency_code)
        
        st.markdown("**Exchange Rates:**")
        
        # Show rates
        if po_currency_code != invoice_currency_code:
            if rates['po_to_invoice_rate'] is not None:
                st.text(f"1 {po_currency_code} = {format_exchange_rate(rates['po_to_invoice_rate'])} {invoice_currency_code}")
            else:
                st.error(f"⚠️ Could not fetch {po_currency_code}/{invoice_currency_code} rate")
        else:
            st.success("✅ Same currency - No conversion needed")
        
        # USD rate
        if invoice_currency_code != 'USD':
            if rates['usd_exchange_rate'] is not None:
                st.text(f"1 USD = {format_exchange_rate(rates['usd_exchange_rate'])} {invoice_currency_code}")
            else:
                st.warning("⚠️ USD exchange rate not available")
        else:
            st.info("💵 Invoice currency is USD")
        
        state.exchange_rates = rates

def show_payment_terms():
    """Show payment terms selection"""
    state = StateManager.get_state()
    service = InvoiceService()
    
    unique_payment_terms = state.selected_df['payment_term'].dropna().unique().tolist()
    
    term_options = {}
    
    if unique_payment_terms:
        all_payment_terms_df = get_payment_terms()
        
        for term_name in unique_payment_terms:
            db_match = all_payment_terms_df[all_payment_terms_df['name'] == term_name]
            
            if not db_match.empty:
                row = db_match.iloc[0]
                term_options[term_name] = {
                    'id': int(row['id']),
                    'days': int(row['days']),
                    'description': row.get('description', '')
                }
            else:
                days = calculate_days_from_term_name(term_name)
                term_options[term_name] = {
                    'id': 1,
                    'days': days,
                    'description': f'{term_name} ({days} days)'
                }
    else:
        term_options = {
            'Net 30': {'id': 1, 'days': 30, 'description': 'Payment due in 30 days'}
        }
    
    st.markdown("---")
    col1, col2 = st.columns(2)
    
    with col1:
        term_names = list(term_options.keys())
        
        default_index = 0
        if state.selected_payment_term in term_names:
            default_index = term_names.index(state.selected_payment_term)
        
        selected_term = st.selectbox(
            "Payment Terms",
            options=term_names,
            index=default_index,
            help=f"Payment terms from selected ANs ({len(term_names)} option(s) available)"
        )
        
        state.selected_payment_term = selected_term
        
        if term_options[selected_term].get('description'):
            st.caption(term_options[selected_term]['description'])
    
    with col2:
        state.invoice_date = st.date_input(
            "Invoice Date",
            value=state.invoice_date
        )
    
    # Calculate due date
    term_days = term_options[state.selected_payment_term]['days']
    state.due_date = service.calculate_due_date(state.invoice_date, term_days)
    
    # Store payment term ID
    state.payment_term_id = term_options[state.selected_payment_term]['id']

def show_invoice_form(invoice_number: str, po_currency_code: str, service: InvoiceService):
    """Show invoice form with summary"""
    state = StateManager.get_state()
    
    with st.form("invoice_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.text_input("Invoice Number", value=invoice_number, disabled=True)
            st.text_input("Invoice Date", value=str(state.invoice_date), disabled=True)
            st.text_input("Payment Terms", value=state.selected_payment_term, disabled=True)
        
        with col2:
            state.commercial_invoice_no = st.text_input(
                "Commercial Invoice No.",
                value=state.commercial_invoice_no or "",
                disabled=state.is_advance_payment,
                placeholder="Required for Commercial Invoices" if not state.is_advance_payment else "Not required for Advance Payment"
            )
            
            if state.is_advance_payment:
                st.caption("💡 Commercial invoice number not required for advance payments")
            else:
                st.caption("⚠️ Required field for commercial invoices")
            
            st.date_input(
                "Due Date",
                value=state.due_date,
                disabled=True,
                help=f"Auto-calculated: Invoice Date + payment term days"
            )
            
            state.email_to_accountant = st.checkbox(
                "Email to Accountant",
                value=state.email_to_accountant
            )
        
        # Invoice summary
        st.markdown("### 📊 Invoice Summary")
        
        # Calculate totals
        if po_currency_code != state.invoice_currency_code:
            converted_amounts = get_invoice_amounts_in_currency(
                state.selected_df,
                po_currency_code,
                state.invoice_currency_code
            )
            totals = converted_amounts if converted_amounts else service.calculate_invoice_totals_with_vat(state.selected_df)
        else:
            totals = service.calculate_invoice_totals_with_vat(state.selected_df)
            totals['currency'] = state.invoice_currency_code
        
        # Display summary
        summary_df = service.prepare_invoice_summary(state.selected_df)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
        
        # Show totals
        col1, col2, col3 = st.columns([2, 1, 1])
        with col3:
            st.markdown("**Invoice Totals**")
            st.text(f"Lines: {len(state.selected_df)}")
            st.text(f"Quantity: {state.selected_df['uninvoiced_quantity'].sum():,.2f}")
            st.text(f"Subtotal: {totals['subtotal']:,.2f} {totals['currency']}")
            st.text(f"VAT: {totals['total_vat']:,.2f} {totals['currency']}")
            st.text(f"Total: {totals['total_with_vat']:,.2f} {totals['currency']}")
        
        state.invoice_totals = totals
        
        # Form buttons
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            back_btn = st.form_submit_button("⬅️ Back", use_container_width=True)
        
        with col3:
            proceed_btn = st.form_submit_button(
                "✅ Review & Confirm", 
                type="primary", 
                use_container_width=True
            )
    
    # Handle form submission
    if back_btn:
        state.wizard_step = 'select'
        st.rerun()
    
    if proceed_btn:
        if not state.is_advance_payment and not state.commercial_invoice_no:
            st.error("❌ Commercial Invoice Number is required for Commercial Invoices")
            return
        
        # Prepare invoice data
        usd_rate = 1.0 if state.invoice_currency_code == 'USD' else state.exchange_rates.get('usd_exchange_rate', None)
        
        state.invoice_data = {
            'invoice_number': invoice_number,
            'commercial_invoice_no': state.commercial_invoice_no if not state.is_advance_payment else '',
            'invoiced_date': state.invoice_date,
            'due_date': state.due_date,
            'total_invoiced_amount': totals['total_with_vat'],
            'currency_id': state.invoice_currency_id,
            'usd_exchange_rate': usd_rate,
            'seller_id': state.details_df['vendor_id'].iloc[0],
            'buyer_id': state.details_df['entity_id'].iloc[0],
            'payment_term_id': state.payment_term_id,
            'email_to_accountant': 1 if state.email_to_accountant else 0,
            'created_by': st.session_state.username,
            'invoice_type': 'PROFORMA_INVOICE' if state.is_advance_payment else 'COMMERCIAL_INVOICE',
            'advance_payment': 1 if state.is_advance_payment else 0,
            'po_currency_code': po_currency_code,
            'invoice_currency_code': state.invoice_currency_code,
            'po_to_invoice_rate': state.exchange_rates.get('po_to_invoice_rate', 1.0)
        }
        
        state.wizard_step = 'confirm'
        st.rerun()

# ============================================================================
# FILE ATTACHMENT UI COMPONENTS
# ============================================================================

def render_file_upload_section():
    """Render file upload widget and display uploaded files"""
    state = StateManager.get_state()
    
    st.markdown("### 📎 Invoice Attachments (Optional)")
    st.info("💡 Attach vendor invoice files (PDF, PNG, JPG). Files will be uploaded when you create the invoice.")
    
    # File uploader
    uploaded_files = st.file_uploader(
        "Upload vendor invoice files",
        type=['pdf', 'png', 'jpg', 'jpeg'],
        accept_multiple_files=True,
        help="Maximum 10 files, 10MB each",
        key="invoice_file_uploader"
    )
    
    # Handle file uploads
    if uploaded_files:
        # Validate files
        is_valid, errors, metadata = validate_uploaded_files(uploaded_files)
        
        if not is_valid:
            st.error("❌ File validation failed:")
            for error in errors:
                st.error(f"  • {error}")
            state.upload_errors = errors
            state.uploaded_files = []
            state.uploaded_files_metadata = []
        else:
            # Store valid files
            state.uploaded_files = uploaded_files
            state.uploaded_files_metadata = metadata
            state.upload_errors = []
            
            # Display summary
            summary = summarize_files(metadata)
            st.success(f"✅ {summary['count']} file(s) ready to upload ({summary['total_size_formatted']} total)")
            
            # Display file list
            display_uploaded_files_table(metadata)
    else:
        # Clear state if no files
        state.uploaded_files = []
        state.uploaded_files_metadata = []
        state.upload_errors = []

def display_uploaded_files_table(metadata: List[Dict]):
    """Display table of uploaded files"""
    if not metadata:
        return
    
    st.markdown("#### 📄 Files to Upload:")
    
    for file_meta in metadata:
        col1, col2, col3 = st.columns([3, 1, 1])
        
        with col1:
            icon = get_file_icon(file_meta['filename'])
            st.text(f"{icon} {file_meta['filename']}")
        
        with col2:
            st.text(f"{file_meta['size_mb']} MB")
        
        with col3:
            st.text(f"✅ {file_meta['type']}")

# ============================================================================
# STEP 3: CONFIRMATION
# ============================================================================

def show_invoice_confirm():
    """Step 3: Confirm and Submit"""
    state = StateManager.get_state()
    service = InvoiceService()
    
    # Validate data
    if not state.invoice_data or state.details_df is None:
        st.error("No invoice data found. Please go back and complete the preview.")
        if st.button("⬅️ Back to Preview"):
            state.wizard_step = 'preview'
            st.rerun()
        return
    
    invoice_data = state.invoice_data
    details_df = state.details_df
    
    st.markdown("### ✅ Confirm and Submit")
    
    # Get payment term name
    payment_terms_dict = service.get_payment_terms_dict()
    payment_term_name = payment_terms_dict.get(
        invoice_data['payment_term_id'], {}
    ).get('name', 'N/A')
    
    # Display invoice details
    with st.container():
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📋 Invoice Details")
            invoice_type = "Advance Payment (PI)" if invoice_data.get('invoice_type') == 'PROFORMA_INVOICE' else "Commercial Invoice (CI)"
            st.text(f"Invoice Type: {invoice_type}")
            st.text(f"Invoice Number: {invoice_data['invoice_number']}")
            st.text(f"Invoice Date: {invoice_data['invoiced_date']}")
            st.text(f"Due Date: {invoice_data['due_date']}")
            st.text(f"Payment Terms: {payment_term_name}")
            if invoice_data.get('commercial_invoice_no'):
                st.text(f"Commercial Invoice: {invoice_data['commercial_invoice_no']}")
        
        with col2:
            st.markdown("#### 💰 Summary")
            st.text(f"Total Amount: {invoice_data['total_invoiced_amount']:,.2f}")
            st.text(f"Invoice Currency: {invoice_data['invoice_currency_code']}")
            st.text(f"PO Currency: {invoice_data['po_currency_code']}")
            if invoice_data['po_currency_code'] != invoice_data['invoice_currency_code']:
                st.text(f"Exchange Rate: {format_exchange_rate(invoice_data['po_to_invoice_rate'])}")
            st.text(f"Lines: {len(details_df)}")
            st.text(f"Email to Accountant: {'Yes' if invoice_data['email_to_accountant'] else 'No'}")
    
    # Exchange rate info
    if invoice_data.get('usd_exchange_rate') is not None:
        if invoice_data['invoice_currency_code'] != 'USD':
            st.info(f"💱 USD Exchange Rate: 1 USD = {format_exchange_rate(invoice_data['usd_exchange_rate'])} {invoice_data['invoice_currency_code']}")
        else:
            st.info("💵 Invoice currency is USD (Rate: 1.0)")
    else:
        st.warning("⚠️ USD exchange rate not available for this invoice")
    
    # Display line items
    display_confirmation_line_items(details_df, state.selected_df, invoice_data)
    
    # Display totals
    if state.invoice_totals:
        totals = state.invoice_totals
        col1, col2, col3 = st.columns(3)
        with col3:
            st.markdown("**Final Totals**")
            st.text(f"Subtotal: {totals['subtotal']:,.2f} {totals['currency']}")
            st.text(f"VAT: {totals['total_vat']:,.2f} {totals['currency']}")
            st.text(f"Total: {totals['total_with_vat']:,.2f} {totals['currency']}")
    
    # File attachments section
    st.markdown("---")
    render_file_upload_section()
    
    st.markdown("---")
    st.warning("⚠️ Please review the information above carefully. This action cannot be undone.")
    
    # Action buttons
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("⬅️ Back to Preview", use_container_width=True):
            state.wizard_step = 'preview'
            st.rerun()
    
    with col3:
        button_text = "💾 Create Invoice & Upload Files" if state.uploaded_files else "💾 Create Invoice"
        if st.button(button_text, type="primary", use_container_width=True):
            create_invoice_final(invoice_data, details_df)

def display_confirmation_line_items(details_df: pd.DataFrame, selected_df: pd.DataFrame, invoice_data: Dict):
    """Display line items for confirmation"""
    st.markdown("### 📋 Line Items")
    
    # Merge dataframes
    df_display = pd.merge(
        details_df[['arrival_detail_id', 'arrival_note_number', 'po_number', 'product_name', 'uninvoiced_quantity', 'buying_unit_cost']],
        selected_df[['can_line_id', 'vat_percent']],
        left_on='arrival_detail_id',
        right_on='can_line_id',
        how='left'
    )
    
    # Add ID column
    df_display.insert(0, 'id', range(1, len(df_display) + 1))
    
    # Select columns to display
    df_display = df_display[['id', 'arrival_note_number', 'po_number', 'product_name', 
                            'uninvoiced_quantity', 'buying_unit_cost', 'vat_percent']].copy()
    
    # Format based on currency conversion
    if invoice_data['po_currency_code'] != invoice_data['invoice_currency_code']:
        df_display['converted_unit_cost'] = df_display['buying_unit_cost'].apply(
            lambda x: f"{float(x.split()[0]) * invoice_data['po_to_invoice_rate']:,.2f} {invoice_data['invoice_currency_code']}"
        )
        df_display['vat_percent'] = df_display['vat_percent'].apply(lambda x: f"{x:.0f}%")
        df_display.columns = ['ID', 'AN Number', 'PO Number', 'Product', 'Quantity', 'Original Cost', 'VAT', 'Invoice Cost']
        display_cols = ['ID', 'AN Number', 'PO Number', 'Product', 'Quantity', 'Original Cost', 'Invoice Cost', 'VAT']
    else:
        df_display['vat_percent'] = df_display['vat_percent'].apply(lambda x: f"{x:.0f}%")
        df_display.columns = ['ID', 'AN Number', 'PO Number', 'Product', 'Quantity', 'Unit Cost', 'VAT']
        display_cols = ['ID', 'AN Number', 'PO Number', 'Product', 'Quantity', 'Unit Cost', 'VAT']
    
    st.dataframe(df_display[display_cols], use_container_width=True, hide_index=True)

def create_invoice_final(invoice_data: Dict, details_df: pd.DataFrame):
    """Create the invoice with file attachments and proper error handling"""
    state = StateManager.get_state()
    auth = AuthManager()
    
    # Prevent duplicate submissions
    if state.invoice_creating:
        st.warning("⏳ Invoice is being created. Please wait...")
        return
    
    state.invoice_creating = True
    s3_keys_uploaded = []
    media_ids_created = []
    
    try:
        # Get user's keycloak_id
        keycloak_id = auth.get_user_keycloak_id()
        if not keycloak_id:
            st.error("❌ Could not retrieve user information. Please log in again.")
            return
        
        # Step 1: Handle file uploads if any
        if state.uploaded_files:
            with st.spinner(f"📤 Uploading {len(state.uploaded_files)} file(s) to S3..."):
                try:
                    # Initialize S3 manager
                    s3_manager = S3Manager()
                    
                    # Prepare files for upload
                    prepared_files = prepare_files_for_upload(
                        state.uploaded_files,
                        invoice_data['invoice_number']
                    )
                    
                    # Upload files to S3
                    files_for_s3 = [(f['content'], f['sanitized_name']) for f in prepared_files]
                    upload_results = s3_manager.batch_upload_invoice_files(files_for_s3)
                    
                    if not upload_results['success']:
                        error_msg = "Failed to upload some files:\n"
                        for failed in upload_results['failed']:
                            error_msg += f"  • {failed['filename']}: {failed['error']}\n"
                        st.error(f"❌ {error_msg}")
                        return
                    
                    s3_keys_uploaded = upload_results['uploaded']
                    st.success(f"✅ Uploaded {len(s3_keys_uploaded)} file(s) successfully")
                    
                except Exception as e:
                    logger.error(f"S3 upload error: {e}")
                    st.error(f"❌ Failed to upload files: {str(e)}")
                    return
        
        # Step 2: Create media records if files were uploaded
        if s3_keys_uploaded:
            with st.spinner("💾 Creating media records..."):
                try:
                    success, media_ids, error_msg = save_media_records(s3_keys_uploaded, keycloak_id)
                    
                    if not success:
                        st.error(f"❌ Failed to create media records: {error_msg}")
                        # Cleanup S3 files
                        cleanup_failed_uploads(s3_keys_uploaded, s3_manager)
                        return
                    
                    media_ids_created = media_ids
                    st.success(f"✅ Created {len(media_ids)} media record(s)")
                    
                except Exception as e:
                    logger.error(f"Media record error: {e}")
                    st.error(f"❌ Failed to create media records: {str(e)}")
                    # Cleanup S3 files
                    if s3_keys_uploaded:
                        cleanup_failed_uploads(s3_keys_uploaded, S3Manager())
                    return
        
        # Step 3: Create invoice with media links
        with st.spinner("📝 Creating invoice..."):
            success, message, invoice_id = create_purchase_invoice(
                invoice_data,
                details_df,
                keycloak_id,  # Pass keycloak_id directly, not username
                media_ids=media_ids_created if media_ids_created else None
            )
            
            if success:
                st.success(f"✅ {message}")
                if media_ids_created:
                    st.success(f"✅ Linked {len(media_ids_created)} attachment(s) to invoice")
                st.balloons()
                
                # Store success info
                state.last_created_invoice = {
                    'id': invoice_id,
                    'number': invoice_data['invoice_number'],
                    'amount': invoice_data['total_invoiced_amount'],
                    'currency': invoice_data['invoice_currency_code'],
                    'attachments': len(media_ids_created)
                }
                
                # Reset wizard state
                StateManager.reset_wizard()
                
                # Show redirect message
                with st.empty():
                    for i in range(3, 0, -1):
                        st.info(f"🔄 Redirecting to home page in {i} seconds...")
                        time.sleep(1)
                
                st.rerun()
            else:
                st.error(f"❌ {message}")
                
                # Cleanup on invoice creation failure
                if s3_keys_uploaded:
                    st.warning("⚠️ Cleaning up uploaded files...")
                    cleanup_failed_uploads(s3_keys_uploaded, S3Manager())
                
    except Exception as e:
        logger.error(f"Error creating invoice: {e}")
        st.error(f"❌ Error creating invoice: {str(e)}")
        
        # Cleanup on any error
        if s3_keys_uploaded:
            try:
                st.warning("⚠️ Cleaning up uploaded files...")
                cleanup_failed_uploads(s3_keys_uploaded, S3Manager())
            except Exception as cleanup_error:
                logger.error(f"Cleanup error: {cleanup_error}")
                
    finally:
        state.invoice_creating = False

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()