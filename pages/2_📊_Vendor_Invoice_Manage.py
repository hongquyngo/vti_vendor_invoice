# pages/2_📊_Vendor_Invoice_Manage.py

import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
import io
from typing import Optional, Dict, List

# Import utils
from utils.auth import AuthManager
from utils.invoice_data import get_recent_invoices, get_invoice_by_id, update_invoice, delete_invoice
from utils.invoice_service import InvoiceService
from utils.db import get_db_engine
from sqlalchemy import text

# Page config
st.set_page_config(
    page_title="Vendor Invoice Management",
    page_icon="📊",
    layout="wide"
)

# Initialize
auth = AuthManager()
auth.require_auth()
service = InvoiceService()

# Session state for selected invoice
if 'selected_invoice_id' not in st.session_state:
    st.session_state.selected_invoice_id = None
if 'edit_mode' not in st.session_state:
    st.session_state.edit_mode = False
if 'show_details' not in st.session_state:
    st.session_state.show_details = False

def main():
    st.title("📊 Vendor Invoice Management")
    
    # Tab layout for different views
    tab1, tab2, tab3 = st.tabs(["📋 Invoice List", "🔍 Invoice Details", "📈 Analytics"])
    
    with tab1:
        show_invoice_list()
    
    with tab2:
        show_invoice_details()
    
    with tab3:
        show_analytics()

def show_invoice_list():
    """Display list of invoices with search and actions"""
    
    # Filters section
    with st.expander("🔍 Search & Filters", expanded=True):
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            date_filter = st.selectbox(
                "Date Range",
                ["Last 7 days", "Last 30 days", "Last 90 days", "This Month", "All Time", "Custom"],
                key="date_filter"
            )
            
            if date_filter == "Custom":
                date_from = st.date_input("From Date", key="custom_from")
                date_to = st.date_input("To Date", key="custom_to")
        
        with col2:
            invoice_search = st.text_input(
                "Invoice Number",
                placeholder="Search invoice number...",
                key="invoice_search"
            )
            
            commercial_search = st.text_input(
                "Commercial Invoice #",
                placeholder="Search commercial invoice...",
                key="commercial_search"
            )
        
        with col3:
            vendor_search = st.text_input(
                "Vendor",
                placeholder="Search vendor...",
                key="vendor_search"
            )
            
            # Invoice Type filter
            invoice_type_filter = st.selectbox(
                "Invoice Type",
                ["All", "Commercial Invoice", "Advance Payment"],
                key="invoice_type_filter"
            )
        
        with col4:
            # Status filter - using payment_status from view
            status_filter = st.selectbox(
                "Payment Status",
                ["All", "Unpaid", "Partially Paid", "Fully Paid", "Overdue"],
                key="status_filter"
            )
            
            limit = st.number_input(
                "Max Records",
                min_value=10,
                max_value=500,
                value=100,
                step=50,
                key="record_limit"
            )
    
    # Get and filter data
    df = get_filtered_invoices(
        date_filter, invoice_search, vendor_search, 
        invoice_type_filter, commercial_search, status_filter, int(limit)
    )
    
    if df.empty:
        st.info("No invoices found matching the criteria.")
        return
    
    # Summary metrics
    show_summary_metrics(df)
    
    # Invoice table with actions
    st.markdown("### 📋 Invoice List")
    
    # Add action column
    df_display = prepare_display_dataframe(df)
    
    # Create columns for headers
    header_cols = st.columns([0.8, 1.5, 1, 1.5, 1.5, 1.2, 1, 1, 1, 1, 1.5])
    header_cols[0].markdown("**Select**")
    header_cols[1].markdown("**Invoice #**")
    header_cols[2].markdown("**Type**")
    header_cols[3].markdown("**Vendor**")
    header_cols[4].markdown("**Commercial #**")
    header_cols[5].markdown("**Amount**")
    header_cols[6].markdown("**Invoice Date**")
    header_cols[7].markdown("**Due Date**")
    header_cols[8].markdown("**Status**")
    header_cols[9].markdown("**Days Overdue**")
    header_cols[10].markdown("**Actions**")
    
    st.markdown("---")
    
    # Create rows for each invoice
    for idx, row in df_display.iterrows():
        with st.container():
            cols = st.columns([0.8, 1.5, 1, 1.5, 1.5, 1.2, 1, 1, 1, 1, 1.5])
            
            # Checkbox for bulk actions
            cols[0].checkbox("", key=f"select_{row['id']}", label_visibility="collapsed")
            
            # Invoice data
            cols[1].text(row['Invoice #'][:20])
            cols[2].text(row['Type'])
            cols[3].text(row['Vendor'][:20])
            cols[4].text(row['Commercial #'][:20] if pd.notna(row['Commercial #']) else '-')
            cols[5].text(row['Amount'])
            cols[6].text(row['Invoice Date'])
            cols[7].text(row['Due Date'])
            
            # Payment Status with color coding
            payment_status = row.get('Payment Status', 'Unknown')
            if payment_status == 'Fully Paid':
                cols[8].success(payment_status[:10])
            elif payment_status == 'Partially Paid':
                cols[8].warning(payment_status[:10])
            elif payment_status == 'Overdue' or row.get('Days Overdue', 0) > 0:
                cols[8].error('Overdue')
            else:
                cols[8].text(payment_status[:10])
            
            # Days Overdue
            days_overdue = row.get('Days Overdue', 0)
            if days_overdue > 0:
                cols[9].error(f"{days_overdue} days")
            else:
                cols[9].text("-")
            
            # Action buttons
            action_col = cols[10]
            c1, c2, c3 = action_col.columns(3)
            
            if c1.button("👁️", key=f"view_{row['id']}", help="View Details"):
                st.session_state.selected_invoice_id = row['id']
                st.session_state.show_details = True
                st.session_state.edit_mode = False
                st.rerun()
            
            if c2.button("✏️", key=f"edit_{row['id']}", help="Edit Invoice"):
                st.session_state.selected_invoice_id = row['id']
                st.session_state.show_details = True
                st.session_state.edit_mode = True
                st.rerun()
            
            # Only show void for unpaid/partially paid invoices
            if payment_status != 'Fully Paid':
                if c3.button("🚫", key=f"void_{row['id']}", help="Void Invoice"):
                    if st.session_state.get(f"confirm_void_{row['id']}"):
                        void_invoice(row['id'])
                        st.success(f"Invoice {row['Invoice #']} has been voided.")
                        st.rerun()
                    else:
                        st.session_state[f"confirm_void_{row['id']}"] = True
                        st.warning("Click again to confirm void action.")
    
    # Bulk actions
    st.markdown("---")
    show_bulk_actions(df)
    
    # Export options
    show_export_options(df_display)

def show_invoice_details():
    """Show detailed view of selected invoice"""
    
    if not st.session_state.selected_invoice_id:
        st.info("Select an invoice from the list to view details.")
        return
    
    # Get invoice details
    invoice_data = get_invoice_details_by_id(st.session_state.selected_invoice_id)
    
    if not invoice_data:
        st.error("Invoice not found.")
        return
    
    # Header with navigation
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col1:
        if st.button("← Back to List"):
            st.session_state.selected_invoice_id = None
            st.session_state.show_details = False
            st.session_state.edit_mode = False
            st.rerun()
    
    with col2:
        st.markdown(f"### Invoice: {invoice_data['invoice_number']}")
    
    with col3:
        if not st.session_state.edit_mode:
            if st.button("✏️ Edit Invoice", type="primary"):
                st.session_state.edit_mode = True
                st.rerun()
        else:
            if st.button("Cancel Edit"):
                st.session_state.edit_mode = False
                st.rerun()
    
    # Display or edit invoice based on mode
    if st.session_state.edit_mode:
        show_edit_form(invoice_data)
    else:
        show_invoice_view(invoice_data)

def show_invoice_view(invoice_data: Dict):
    """Display invoice in read-only view"""
    
    # Invoice header information
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### 📄 Invoice Information")
        st.text(f"Invoice Number: {invoice_data['invoice_number']}")
        st.text(f"Type: {invoice_data.get('invoice_type', 'Commercial Invoice')}")
        st.text(f"Commercial Invoice: {invoice_data.get('commercial_invoice_no', 'N/A')}")
        payment_status = invoice_data.get('payment_status', 'Unknown')
        if payment_status == 'Fully Paid':
            st.success(f"Payment Status: {payment_status}")
        elif payment_status == 'Partially Paid':
            st.warning(f"Payment Status: {payment_status}")
        else:
            st.error(f"Payment Status: {payment_status}")
    
    with col2:
        st.markdown("#### 📅 Dates")
        st.text(f"Invoice Date: {invoice_data['invoiced_date']}")
        st.text(f"Due Date: {invoice_data['due_date']}")
        days_overdue = invoice_data.get('days_overdue', 0)
        if days_overdue and days_overdue > 0:
            st.error(f"Days Overdue: {days_overdue}")
        st.text(f"Created By: {invoice_data.get('created_by', 'N/A')}")
    
    with col3:
        st.markdown("#### 💰 Financial")
        st.text(f"Total Amount: {invoice_data['total_invoiced_amount']:,.2f}")
        st.text(f"Currency: {invoice_data.get('currency_code', 'USD')}")
        st.text(f"Payment Terms: {invoice_data.get('payment_term', 'N/A')}")
        if 'total_outstanding_amount' in invoice_data:
            st.warning(f"Outstanding: {invoice_data['total_outstanding_amount']:,.2f}")
    
    # Vendor and buyer information
    st.markdown("---")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 🏢 Vendor")
        st.text(f"Name: {invoice_data.get('vendor_name', 'N/A')}")
        st.text(f"Code: {invoice_data.get('vendor_code', 'N/A')}")
    
    with col2:
        st.markdown("#### 🏢 Buyer")
        st.text(f"Name: {invoice_data.get('buyer_name', 'N/A')}")
        st.text(f"Code: {invoice_data.get('buyer_code', 'N/A')}")
    
    # Payment Information if available
    if 'payment_ratio' in invoice_data:
        st.markdown("---")
        st.markdown("#### 💳 Payment Information")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Payment Ratio", f"{invoice_data.get('payment_ratio', 0)*100:.1f}%")
        with col2:
            st.metric("Total Paid", f"{invoice_data.get('total_payment_made', 0):,.2f}")
        with col3:
            st.metric("Risk Level", invoice_data.get('risk_level', 'Unknown'))
    
    # Line items
    st.markdown("---")
    st.markdown("#### 📋 Invoice Line Items")
    
    from utils.invoice_data import get_invoice_line_items
    line_items = get_invoice_line_items(st.session_state.selected_invoice_id)
    
    if not line_items.empty:
        # Format line items for display
        line_items['amount_display'] = line_items.apply(
            lambda r: f"{r['amount']:,.2f}", axis=1
        )
        line_items['quantity_display'] = line_items.apply(
            lambda r: f"{r['purchased_invoice_quantity']:,.2f}", axis=1
        )
        
        display_cols = ['po_number', 'product_name', 'quantity_display', 
                       'amount_display', 'vat_gst', 'arrival_note_number']
        
        column_names = {
            'po_number': 'PO Number',
            'product_name': 'Product',
            'quantity_display': 'Quantity',
            'amount_display': 'Amount',
            'vat_gst': 'VAT %',
            'arrival_note_number': 'AN Number'
        }
        
        # Filter out columns that don't exist
        available_cols = [col for col in display_cols if col in line_items.columns]
        display_df = line_items[available_cols].rename(columns=column_names)
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No line items found for this invoice.")
    
    # Actions
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("📄 Print Invoice", use_container_width=True):
            st.info("Print functionality coming soon...")
    
    with col2:
        if st.button("📧 Email Invoice", use_container_width=True):
            st.info("Email functionality coming soon...")
    
    with col3:
        if invoice_data.get('payment_status') != 'Fully Paid':
            if st.button("🚫 Void Invoice", use_container_width=True):
                if st.session_state.get('confirm_void_detail'):
                    void_invoice(st.session_state.selected_invoice_id)
                    st.success("Invoice has been voided.")
                    st.rerun()
                else:
                    st.session_state.confirm_void_detail = True
                    st.warning("Click again to confirm void.")
    
    with col4:
        if st.button("🗑️ Delete Invoice", use_container_width=True, type="secondary"):
            st.error("Delete functionality requires admin approval.")

def show_edit_form(invoice_data: Dict):
    """Show editable form for invoice"""
    
    st.markdown("### ✏️ Edit Invoice")
    
    with st.form("edit_invoice_form"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("#### Invoice Information")
            commercial_invoice = st.text_input(
                "Commercial Invoice #",
                value=invoice_data.get('commercial_invoice_no', ''),
                key="edit_commercial_invoice"
            )
            
            invoice_date = st.date_input(
                "Invoice Date",
                value=pd.to_datetime(invoice_data['invoiced_date']).date(),
                key="edit_invoice_date"
            )
        
        with col2:
            st.markdown("#### Financial")
            # Note: Currency and amounts should typically not be editable
            st.text_input(
                "Total Amount",
                value=f"{invoice_data['total_invoiced_amount']:,.2f}",
                disabled=True
            )
            
            st.text_input(
                "Currency",
                value=invoice_data.get('currency_code', 'USD'),
                disabled=True
            )
        
        with col3:
            st.markdown("#### Other")
            due_date = st.date_input(
                "Due Date",
                value=pd.to_datetime(invoice_data['due_date']).date(),
                key="edit_due_date"
            )
            
            email_to_accountant = st.checkbox(
                "Email to Accountant",
                value=bool(invoice_data.get('email_to_accountant', 0)),
                key="edit_email_accountant"
            )
        
        st.markdown("---")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            cancel_btn = st.form_submit_button("Cancel", use_container_width=True)
        
        with col3:
            save_btn = st.form_submit_button(
                "💾 Save Changes", 
                type="primary",
                use_container_width=True
            )
        
        if cancel_btn:
            st.session_state.edit_mode = False
            st.rerun()
        
        if save_btn:
            # Update invoice
            update_data = {
                'commercial_invoice_no': commercial_invoice,
                'invoiced_date': invoice_date,
                'due_date': due_date,
                'email_to_accountant': 1 if email_to_accountant else 0,
                'modified_date': datetime.now()
            }
            
            success = update_invoice_data(
                st.session_state.selected_invoice_id, 
                update_data
            )
            
            if success:
                st.success("Invoice updated successfully!")
                st.session_state.edit_mode = False
                st.rerun()
            else:
                st.error("Failed to update invoice. Please try again.")

def show_analytics():
    """Show analytics dashboard"""
    
    # Get data for analytics
    df = get_recent_invoices(limit=1000)
    
    if df.empty:
        st.info("No data available for analytics.")
        return
    
    # Date range selector
    col1, col2 = st.columns([1, 3])
    
    with col1:
        date_range = st.selectbox(
            "Select Period",
            ["Last 30 days", "Last 90 days", "Last 6 months", "Last Year"],
            key="analytics_date_range"
        )
    
    # Filter data based on date range
    today = pd.Timestamp.now()
    if date_range == "Last 30 days":
        date_threshold = today - timedelta(days=30)
    elif date_range == "Last 90 days":
        date_threshold = today - timedelta(days=90)
    elif date_range == "Last 6 months":
        date_threshold = today - timedelta(days=180)
    else:
        date_threshold = today - timedelta(days=365)
    
    df['invoiced_date'] = pd.to_datetime(df['invoiced_date'])
    df = df[df['invoiced_date'] >= date_threshold]
    
    # Summary cards
    st.markdown("### 📊 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_invoices = len(df)
        st.metric("Total Invoices", f"{total_invoices:,}")
    
    with col2:
        unique_vendors = df['vendor'].nunique()
        st.metric("Unique Vendors", f"{unique_vendors:,}")
    
    with col3:
        # Calculate total by currency
        main_currency = df.groupby('currency')['total_invoiced_amount'].sum().idxmax() if not df.empty else 'USD'
        total_amount = df[df['currency'] == main_currency]['total_invoiced_amount'].sum()
        st.metric(f"Total {main_currency}", f"{total_amount:,.2f}")
    
    with col4:
        avg_invoice = df['total_invoiced_amount'].mean()
        st.metric("Average Invoice", f"{avg_invoice:,.2f}")
    
    # Payment Status Analysis
    st.markdown("### 💳 Payment Status Overview")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if 'payment_status' in df.columns:
            payment_summary = df.groupby('payment_status')['total_invoiced_amount'].agg(['sum', 'count'])
            payment_summary.columns = ['Total Amount', 'Count']
            st.dataframe(payment_summary, use_container_width=True)
    
    with col2:
        if 'aging_status' in df.columns:
            aging_summary = df.groupby('aging_status')['total_outstanding_amount'].sum()
            aging_df = pd.DataFrame({
                'Aging Status': aging_summary.index,
                'Outstanding Amount': aging_summary.values
            })
            st.dataframe(aging_df, use_container_width=True, hide_index=True)
    
    # Charts
    st.markdown("### 📈 Trends & Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Invoice trend over time
        st.markdown("#### Invoice Volume Over Time")
        df['month'] = df['invoiced_date'].dt.to_period('M')
        monthly_counts = df.groupby('month').size().reset_index(name='count')
        monthly_counts['month'] = monthly_counts['month'].astype(str)
        
        # Simple line chart using st.line_chart
        if not monthly_counts.empty:
            chart_data = monthly_counts.set_index('month')['count']
            st.line_chart(chart_data)
    
    with col2:
        # Top vendors
        st.markdown("#### Top 10 Vendors")
        top_vendors = df.groupby('vendor')['total_invoiced_amount'].agg(['sum', 'count']).round(2)
        top_vendors.columns = ['Total Amount', 'Invoice Count']
        top_vendors = top_vendors.sort_values('Total Amount', ascending=False).head(10)
        st.dataframe(top_vendors, use_container_width=True)
    
    # Invoice type distribution
    st.markdown("#### Invoice Type Distribution")
    
    col1, col2, col3 = st.columns(3)
    
    df['invoice_type'] = df['invoice_number'].apply(
        lambda x: 'Advance Payment' if x.endswith('-A') else 'Commercial Invoice'
    )
    
    type_dist = df['invoice_type'].value_counts()
    
    with col1:
        st.metric("Commercial Invoices", type_dist.get('Commercial Invoice', 0))
    
    with col2:
        st.metric("Advance Payments", type_dist.get('Advance Payment', 0))
    
    with col3:
        if len(type_dist) > 0:
            ci_percent = (type_dist.get('Commercial Invoice', 0) / len(df)) * 100
            st.metric("CI Percentage", f"{ci_percent:.1f}%")

# Helper functions

def get_filtered_invoices(date_filter, invoice_search, vendor_search, 
                          invoice_type_filter, commercial_search, status_filter, limit):
    """Get invoices with filters applied"""
    
    df = get_recent_invoices(limit=limit)
    
    if df.empty:
        return df
    
    # Apply filters
    if invoice_search:
        df = df[df['invoice_number'].str.contains(invoice_search, case=False, na=False)]
    
    if commercial_search and 'commercial_invoice_no' in df.columns:
        df = df[df['commercial_invoice_no'].str.contains(commercial_search, case=False, na=False)]
    
    if vendor_search:
        df = df[df['vendor'].str.contains(vendor_search, case=False, na=False)]
    
    # Filter by invoice type
    if invoice_type_filter == "Commercial Invoice":
        df = df[df['invoice_number'].str.endswith('-P')]
    elif invoice_type_filter == "Advance Payment":
        df = df[df['invoice_number'].str.endswith('-A')]
    
    # Status filter - now using payment_status from view
    if status_filter != "All":
        if status_filter == "Overdue":
            if 'days_overdue' in df.columns:
                df = df[df['days_overdue'] > 0]
        elif 'payment_status' in df.columns:
            df = df[df['payment_status'] == status_filter]
    
    # Date filtering - using invoiced_date instead of created_date
    if date_filter != "All Time" and 'invoiced_date' in df.columns:
        today = pd.Timestamp.now()
        if date_filter == "Last 7 days":
            date_threshold = today - timedelta(days=7)
        elif date_filter == "Last 30 days":
            date_threshold = today - timedelta(days=30)
        elif date_filter == "Last 90 days":
            date_threshold = today - timedelta(days=90)
        elif date_filter == "This Month":
            date_threshold = today.replace(day=1)
        elif date_filter == "Custom":
            # Handle custom date range
            pass
        else:
            date_threshold = None
        
        if 'date_threshold' in locals() and date_threshold is not None:
            df['invoiced_date'] = pd.to_datetime(df['invoiced_date'])
            df = df[df['invoiced_date'] >= date_threshold]
    
    return df

def prepare_display_dataframe(df):
    """Prepare dataframe for display"""
    df_display = df.copy()
    
    # Add invoice type
    df_display['invoice_type'] = df_display['invoice_number'].apply(
        lambda x: 'AP' if x.endswith('-A') else 'CI'
    )
    
    # Format dates
    if 'invoiced_date' in df_display.columns:
        df_display['invoiced_date'] = pd.to_datetime(df_display['invoiced_date']).dt.strftime('%Y-%m-%d')
    if 'due_date' in df_display.columns:
        df_display['due_date'] = pd.to_datetime(df_display['due_date']).dt.strftime('%Y-%m-%d')
    
    # Format amount
    df_display['amount_display'] = df_display.apply(
        lambda row: f"{row['total_invoiced_amount']:,.0f} {row.get('currency', 'USD')}", axis=1
    )
    
    # Add or format payment status
    if 'payment_status' not in df_display.columns:
        df_display['payment_status'] = 'Unknown'
    
    # Add days overdue if not present
    if 'days_overdue' not in df_display.columns:
        df_display['days_overdue'] = 0
    
    # Select and rename columns
    display_columns = {
        'id': 'id',
        'invoice_number': 'Invoice #',
        'invoice_type': 'Type',
        'vendor': 'Vendor',
        'commercial_invoice_no': 'Commercial #',
        'amount_display': 'Amount',
        'invoiced_date': 'Invoice Date',
        'due_date': 'Due Date',
        'payment_status': 'Payment Status',
        'days_overdue': 'Days Overdue',
        'created_by': 'Created By'
    }
    
    # Keep only existing columns
    existing_cols = {k: v for k, v in display_columns.items() if k in df_display.columns}
    
    return df_display[list(existing_cols.keys())].rename(columns=existing_cols)

def show_summary_metrics(df):
    """Show summary metrics for invoices"""
    st.markdown("### 📈 Summary")
    
    # Group by currency
    currency_groups = df.groupby('currency')['total_invoiced_amount'].agg(['sum', 'count', 'mean'])
    
    cols = st.columns(min(len(currency_groups) + 1, 5))
    
    # Total invoices
    cols[0].metric("Total Invoices", len(df))
    
    # Metrics for each currency
    for idx, (currency, stats) in enumerate(currency_groups.iterrows()):
        col_idx = (idx + 1) % 5
        with cols[col_idx]:
            st.metric(
                f"{currency}",
                f"{stats['sum']:,.0f}",
                f"{stats['count']} invoices"
            )

def show_bulk_actions(df):
    """Show bulk action options"""
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    
    with col1:
        # Count selected items
        selected_count = sum(1 for key in st.session_state.keys() 
                           if key.startswith('select_') and st.session_state[key])
        st.text(f"Selected: {selected_count} invoices")
    
    with col2:
        if st.button("Select All"):
            for _, row in df.iterrows():
                st.session_state[f"select_{row['id']}"] = True
            st.rerun()
    
    with col3:
        if st.button("Clear Selection"):
            for key in list(st.session_state.keys()):
                if key.startswith('select_'):
                    del st.session_state[key]
            st.rerun()
    
    with col4:
        if selected_count > 0:
            if st.button(f"Export {selected_count} Selected"):
                st.info("Exporting selected invoices...")

def show_export_options(df):
    """Show export options"""
    st.markdown("---")
    st.markdown("### 📥 Export Options")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Export to Excel
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Invoices', index=False)
        
        st.download_button(
            label="📊 Download Excel",
            data=buffer.getvalue(),
            file_name=f"invoices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    
    with col2:
        # Export to CSV
        csv = df.to_csv(index=False)
        st.download_button(
            label="📄 Download CSV",
            data=csv,
            file_name=f"invoices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col3:
        # Summary report
        if st.button("📊 Generate Report", use_container_width=True):
            st.info("Report generation coming soon...")

def get_invoice_details_by_id(invoice_id: int) -> Optional[Dict]:
    """Get detailed invoice information by ID using the view"""
    try:
        invoice = get_invoice_by_id(invoice_id)
        return invoice
    except Exception as e:
        st.error(f"Error getting invoice details: {str(e)}")
        return None

def update_invoice_data(invoice_id: int, update_data: Dict) -> bool:
    """Update invoice data"""
    try:
        success, message = update_invoice(invoice_id, update_data)
        if not success:
            st.error(message)
        return success
    except Exception as e:
        st.error(f"Error updating invoice: {str(e)}")
        return False

def void_invoice(invoice_id: int) -> bool:
    """Void an invoice (soft delete)"""
    try:
        success, message = delete_invoice(invoice_id, hard_delete=False)
        if not success:
            st.error(message)
        return success
    except Exception as e:
        st.error(f"Error voiding invoice: {str(e)}")
        return False

if __name__ == "__main__":
    main()