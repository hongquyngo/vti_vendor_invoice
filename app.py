# app.py

import streamlit as st
from utils.auth import AuthManager
from utils.db import get_db_engine
from sqlalchemy import text
import pandas as pd
from datetime import datetime, date

# Page config
st.set_page_config(
    page_title="Purchase Invoice Management",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize auth
auth = AuthManager()

def show_login_form():
    """Display the login form"""
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.title("📄 Purchase Invoice Management")
        # st.markdown("---")
        
        with st.form("login_form"):
            username = st.text_input("Username", placeholder="Enter your username")
            password = st.text_input("Password", type="password", placeholder="Enter your password")
            submitted = st.form_submit_button("Login", use_container_width=True, type="primary")
            
            if submitted:
                if username and password:
                    success, user_info = auth.authenticate(username, password)
                    
                    if success:
                        auth.login(user_info)
                        st.success("✅ Login successful!")
                        st.rerun()
                    else:
                        st.error(user_info.get("error", "Authentication failed"))
                else:
                    st.warning("Please enter both username and password")

def format_large_number(value: float, currency: str) -> str:
    """Format large numbers with M/B suffix for better readability"""
    if value >= 1_000_000_000:
        return f"{value/1_000_000_000:.2f}B {currency}"
    elif value >= 1_000_000:
        return f"{value/1_000_000:.2f}M {currency}"
    elif value >= 1_000:
        return f"{value/1_000:.1f}K {currency}"
    else:
        return f"{value:.2f} {currency}"

@st.cache_data(ttl=60)  # Cache for 1 minute
def get_dashboard_stats():
    """Get statistics for dashboard"""
    try:
        engine = get_db_engine()
        
        stats = {}
        
        # 1. Pending ANs count
        pending_query = text("""
        SELECT COUNT(DISTINCT can_line_id) as pending_count
        FROM can_tracking_full_view
        WHERE uninvoiced_quantity > 0
        """)
        
        # 2. Today's invoices - Group by currency
        today_query = text("""
        SELECT 
            COUNT(*) as count,
            COALESCE(SUM(pi.total_invoiced_amount), 0) as total,
            c.code as currency
        FROM purchase_invoices pi
        JOIN currencies c ON pi.currency_id = c.id
        WHERE DATE(pi.created_date) = CURDATE()
        AND pi.delete_flag = 0
        GROUP BY c.code
        ORDER BY total DESC
        """)
        
        # 3. This month's invoices - Get all currencies
        month_query = text("""
        SELECT 
            COUNT(*) as count,
            COALESCE(SUM(pi.total_invoiced_amount), 0) as total,
            c.code as currency
        FROM purchase_invoices pi
        JOIN currencies c ON pi.currency_id = c.id
        WHERE YEAR(pi.created_date) = YEAR(CURDATE())
        AND MONTH(pi.created_date) = MONTH(CURDATE())
        AND pi.delete_flag = 0
        GROUP BY c.code
        ORDER BY total DESC
        """)
        
        # 4. Invoice type breakdown
        type_query = text("""
        SELECT 
            CASE 
                WHEN invoice_number LIKE '%-A' THEN 'Advance Payment'
                ELSE 'Commercial Invoice'
            END as invoice_type,
            COUNT(*) as type_count
        FROM purchase_invoices
        WHERE delete_flag = 0
        AND YEAR(created_date) = YEAR(CURDATE())
        AND MONTH(created_date) = MONTH(CURDATE())
        GROUP BY invoice_type
        """)
        
        with engine.connect() as conn:
            # Get pending ANs
            result = conn.execute(pending_query).fetchone()
            stats['pending_ans'] = result[0] if result else 0
            
            # Get today's stats - handle multiple currencies
            today_results = conn.execute(today_query).fetchall()
            stats['today_by_currency'] = []
            stats['today_count'] = 0
            
            if today_results:
                for row in today_results:
                    stats['today_by_currency'].append({
                        'count': row[0],
                        'total': float(row[1]),
                        'currency': row[2]
                    })
                    stats['today_count'] += row[0]
                
                # Keep the largest for backward compatibility
                largest = max(today_results, key=lambda x: x[1])
                stats['today_total'] = float(largest[1])
                stats['today_currency'] = largest[2]
            else:
                stats['today_total'] = 0
                stats['today_currency'] = 'USD'
            
            # Get month stats - handle multiple currencies
            month_results = conn.execute(month_query).fetchall()
            stats['month_by_currency'] = []
            stats['month_count'] = 0
            
            if month_results:
                for row in month_results:
                    stats['month_by_currency'].append({
                        'count': row[0],
                        'total': float(row[1]),
                        'currency': row[2]
                    })
                    stats['month_count'] += row[0]
                
                # Keep the largest for display
                largest = max(month_results, key=lambda x: x[1])
                stats['month_total'] = float(largest[1])
                stats['month_currency'] = largest[2]
            else:
                stats['month_count'] = 0
                stats['month_total'] = 0
                stats['month_currency'] = 'USD'
            
            # Get invoice type breakdown
            type_results = conn.execute(type_query).fetchall()
            stats['invoice_types'] = {row[0]: row[1] for row in type_results}
        
        return stats
        
    except Exception as e:
        st.error(f"Error loading statistics: {str(e)}")
        return {
            'pending_ans': 0,
            'today_count': 0,
            'today_total': 0,
            'today_currency': 'USD',
            'today_by_currency': [],
            'month_count': 0,
            'month_total': 0,
            'month_currency': 'USD',
            'month_by_currency': [],
            'invoice_types': {}
        }

def main():
    """Main application"""
    # Check authentication
    if not auth.check_session():
        show_login_form()
        return
    
    # Sidebar
    with st.sidebar:
        st.title("📄 Purchase Invoice")
        st.write(f"Welcome, **{auth.get_user_display_name()}**")
        
        st.markdown("---")

        
        # System info
        st.markdown("### System Info")
        st.text(f"User: {st.session_state.username}")
        st.text(f"Role: {st.session_state.user_role}")
        st.text(f"Date: {date.today().strftime('%Y-%m-%d')}")
        
        st.markdown("---")
        
        # Logout button
        if st.button("🚪 Logout", use_container_width=True):
            auth.logout()
            st.rerun()
    
    # Main content
    st.title("Purchase Invoice Management System")
    
    # Load statistics
    stats = get_dashboard_stats()
    
    # Welcome section
    st.markdown("### 📊 Dashboard Overview")
    
    # Quick stats
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Pending ANs", 
            f"{stats['pending_ans']:,}", 
            help="Arrival Notes with uninvoiced quantity"
        )
    
    with col2:
        # Display today's invoices with correct currency
        if stats['today_count'] > 0:
            # Show main currency in delta, or multiple if needed
            if len(stats.get('today_by_currency', [])) > 1:
                delta_text = "Multiple currencies"
            elif stats['today_total'] > 0:
                delta_text = format_large_number(stats['today_total'], stats['today_currency'])
            else:
                delta_text = None
        else:
            delta_text = None
        
        st.metric(
            "Today's Invoices", 
            stats['today_count'],
            delta_text,
            help="Invoices created today"
        )
    
    with col3:
        st.metric(
            "This Month", 
            stats['month_count'],
            help=f"Total invoices in {datetime.now().strftime('%B %Y')}"
        )
    
    with col4:
        # Format month value better
        if stats['month_count'] > 0:
            if len(stats.get('month_by_currency', [])) > 1:
                # Multiple currencies - show main one
                value_text = format_large_number(stats['month_total'], "")
                delta_text = f"{stats['month_currency']} + others"
            else:
                value_text = format_large_number(stats['month_total'], "")
                delta_text = stats['month_currency']
        else:
            value_text = "0"
            delta_text = None
            
        st.metric(
            "Month Value", 
            value_text,
            delta_text,
            help=f"Total invoice value this month"
        )
    
    # Invoice type breakdown
    if stats['invoice_types']:
        st.markdown("### 📈 This Month's Invoice Breakdown")
        col1, col2 = st.columns(2)
        
        with col1:
            ci_count = stats['invoice_types'].get('Commercial Invoice', 0)
            st.info(f"""
            **Commercial Invoices (CI)**
            - Count: {ci_count}
            - Type: Standard purchase invoices
            """)
        
        with col2:
            pi_count = stats['invoice_types'].get('Advance Payment', 0)
            st.info(f"""
            **Advance Payments (PI)**
            - Count: {pi_count}
            - Type: Proforma/advance payment invoices
            """)
    
    # Action cards
    st.markdown("### 🚀 Quick Actions")
    
    col1, col2 = st.columns(2)
    
    with col1:
        with st.container():
            st.markdown("""
            #### 📝 Create New Invoice
            - Select uninvoiced Arrival Notes
            - Support multiple currencies
            - Both CI and PI invoice types
            - Automatic VAT calculation
            """)
            if st.button("Go to Create Invoice →", use_container_width=True, type="primary"):
                st.switch_page("pages/1_📝_Create_Invoice.py")
    
    with col2:
        with st.container():
            st.markdown("""
            #### 📊 View Invoice History
            - Search and filter invoices
            - Export to Excel/CSV
            - Track by vendor or date
            - Generate summary reports
            """)
            if st.button("Go to Invoice History →", use_container_width=True):
                st.switch_page("pages/2_📊_Invoice_History.py")
    
    # Recent activity - Show with correct currency
    if stats['today_count'] > 0 and 'today_by_currency' in stats:
        st.markdown("### 📅 Today's Activity")
        
        # Display each currency separately
        for item in stats['today_by_currency']:
            st.success(f"✅ {item['count']} invoice(s) created with total value of {format_large_number(item['total'], item['currency'])}")
    
    # Monthly breakdown if multiple currencies
    if len(stats.get('month_by_currency', [])) > 1:
        st.markdown("### 💰 Monthly Breakdown by Currency")
        cols = st.columns(min(len(stats['month_by_currency']), 3))
        
        for idx, item in enumerate(stats['month_by_currency']):
            with cols[idx % 3]:
                st.info(f"""
                **{item['currency']}**
                - Count: {item['count']}
                - Total: {format_large_number(item['total'], item['currency'])}
                """)

if __name__ == "__main__":
    main()