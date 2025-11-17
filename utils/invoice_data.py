# utils/invoice_data.py
# Complete implementation with all invoice management functions

import pandas as pd
from sqlalchemy import text
import streamlit as st
from datetime import datetime, date, timedelta
import logging
from typing import List, Dict, Optional, Tuple
from .db import get_db_engine
import re

logger = logging.getLogger(__name__)

# ============================================================================
# CORE INVOICE DATA FUNCTIONS
# ============================================================================

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_uninvoiced_ans(filters: Dict = None) -> pd.DataFrame:
    """
    Get all ANs with uninvoiced quantity
    Enhanced with PO level data and legacy invoice detection
    """
    try:
        engine = get_db_engine()
        
        # Enhanced query with legacy invoice detection
        query = """
        WITH legacy_invoices AS (
            -- Calculate legacy invoices per PO line (arrival_detail_id IS NULL)
            SELECT 
                pid.product_purchase_order_id,
                SUM(pid.purchased_invoice_quantity) as legacy_invoice_qty,
                COUNT(DISTINCT pid.purchase_invoice_id) as legacy_invoice_count
            FROM purchase_invoice_details pid
            JOIN purchase_invoices pi ON pid.purchase_invoice_id = pi.id
            WHERE pid.arrival_detail_id IS NULL  -- Legacy invoices only
                AND pid.delete_flag = 0
                AND pi.delete_flag = 0
            GROUP BY pid.product_purchase_order_id
        )
        SELECT 
            -- AN/CAN Info
            can.can_line_id,
            can.arrival_note_number,
            can.arrival_date,
            can.creator,
            can.days_since_arrival,
            can.created_date,
            
            -- Vendor Info
            can.vendor,
            can.vendor_code,
            can.vendor_type,
            can.vendor_location_type,
            
            -- Entity Info
            can.consignee AS legal_entity,
            can.consignee_code AS legal_entity_code,
            
            -- PO Info
            can.po_number,
            can.po_type,
            can.external_ref_number,
            can.payment_term,
            can.product_purchase_order_id,
            
            -- Product Info
            can.product_name,
            can.pt_code,
            can.brand,
            can.package_size,
            can.standard_uom,
            can.buying_uom,
            can.uom_conversion,
            
            -- AN Level Quantity Info
            can.arrival_quantity,
            can.uninvoiced_quantity,
            can.total_invoiced_quantity,
            can.invoice_status,
            
            -- Cost Info
            can.buying_unit_cost,
            can.standard_unit_cost,
            can.landed_cost,
            can.landed_cost_usd,
            
            -- Calculate invoice value
            ROUND(can.uninvoiced_quantity * 
                  CAST(SUBSTRING_INDEX(can.buying_unit_cost, ' ', 1) AS DECIMAL(15,2)), 2
            ) AS estimated_invoice_value,

            -- Extract currency
            SUBSTRING_INDEX(can.buying_unit_cost, ' ', -1) AS currency,
            
            -- VAT information
            COALESCE(ppo.vat_gst, 0) AS vat_percent,
            ROUND(can.uninvoiced_quantity * 
                  CAST(SUBSTRING_INDEX(can.buying_unit_cost, ' ', 1) AS DECIMAL(15,2)) * 
                  COALESCE(ppo.vat_gst, 0) / 100, 2
            ) AS vat_amount,
            
            -- PO Line Level Status Information
            can.po_line_status,
            can.po_line_is_over_delivered,
            can.po_line_is_over_invoiced,
            can.po_line_arrival_completion_percent,
            can.po_line_invoice_completion_percent,
            can.po_line_pending_invoiced_qty,

            -- PO Quantities
            ppo.purchase_quantity AS po_buying_quantity,
            ppo.quantity AS po_standard_quantity,
            
            -- Legacy Invoice Information
            COALESCE(li.legacy_invoice_qty, 0) AS legacy_invoice_qty,
            COALESCE(li.legacy_invoice_count, 0) AS legacy_invoice_count,
            
            -- Calculate true remaining considering legacy
            GREATEST(
                0,
                LEAST(
                    can.uninvoiced_quantity,
                    can.po_line_pending_invoiced_qty
                )
            ) AS true_remaining_qty,
            
            -- Flag if has legacy invoices
            CASE 
                WHEN COALESCE(li.legacy_invoice_qty, 0) > 0 THEN 'Y' 
                ELSE 'N' 
            END AS has_legacy_invoices
            
        FROM can_tracking_full_view can
        JOIN product_purchase_orders ppo ON can.product_purchase_order_id = ppo.id
        LEFT JOIN legacy_invoices li ON li.product_purchase_order_id = ppo.id
        WHERE can.uninvoiced_quantity > 0
        """
        
        # Add filters
        conditions = []
        params = {}
        
        if filters:
            if filters.get('creators'):
                conditions.append("can.creator IN :creators")
                params['creators'] = tuple(filters['creators'])
            
            if filters.get('vendor_types'):
                conditions.append("can.vendor_type IN :vendor_types")
                params['vendor_types'] = tuple(filters['vendor_types'])
            
            if filters.get('vendors'):
                conditions.append("can.vendor_code IN :vendors")
                params['vendors'] = tuple(filters['vendors'])
            
            if filters.get('entities'):
                conditions.append("can.consignee_code IN :entities")
                params['entities'] = tuple(filters['entities'])
            
            if filters.get('brands'):
                conditions.append("can.brand IN :brands")
                params['brands'] = tuple(filters['brands'])
            
            if filters.get('arrival_date_from'):
                conditions.append("can.arrival_date >= :arrival_date_from")
                params['arrival_date_from'] = filters['arrival_date_from']
            
            if filters.get('arrival_date_to'):
                conditions.append("can.arrival_date <= :arrival_date_to")
                params['arrival_date_to'] = filters['arrival_date_to']
            
            if filters.get('created_date_from'):
                conditions.append("can.created_date >= :created_date_from")
                params['created_date_from'] = filters['created_date_from']
            
            if filters.get('created_date_to'):
                conditions.append("can.created_date <= :created_date_to")
                params['created_date_to'] = filters['created_date_to']
            
            if filters.get('an_numbers'):
                conditions.append("can.arrival_note_number IN :an_numbers")
                params['an_numbers'] = tuple(filters['an_numbers'])
            
            if filters.get('po_numbers'):
                conditions.append("can.po_number IN :po_numbers")
                params['po_numbers'] = tuple(filters['po_numbers'])
        
        # Add conditions to query
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        query += " ORDER BY can.arrival_date DESC, can.arrival_note_number DESC"
        
        # Execute query
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching uninvoiced ANs: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_filter_options() -> Dict:
    """Get unique values for filters"""
    try:
        engine = get_db_engine()
        
        query = text("""
        SELECT 
            DISTINCT creator,
            vendor_type,
            vendor_code,
            vendor,
            consignee_code,
            consignee,
            brand,
            arrival_note_number,
            po_number,
            po_line_status
        FROM can_tracking_full_view
        WHERE uninvoiced_quantity > 0
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        options = {
            'creators': sorted(df['creator'].dropna().unique().tolist()),
            'vendor_types': sorted(df['vendor_type'].dropna().unique().tolist()),
            'vendors': sorted([(row['vendor_code'], row['vendor']) 
                              for _, row in df[['vendor_code', 'vendor']].drop_duplicates().iterrows()]),
            'entities': sorted([(row['consignee_code'], row['consignee']) 
                               for _, row in df[['consignee_code', 'consignee']].drop_duplicates().iterrows()]),
            'brands': sorted(df['brand'].dropna().unique().tolist()),
            'an_numbers': sorted(df['arrival_note_number'].dropna().unique().tolist()),
            'po_numbers': sorted(df['po_number'].dropna().unique().tolist()),
            'po_line_statuses': sorted(df['po_line_status'].dropna().unique().tolist())
        }
        
        return options
        
    except Exception as e:
        logger.error(f"Error getting filter options: {e}")
        return {
            'creators': [], 'vendor_types': [], 'vendors': [], 'entities': [],
            'brands': [], 'an_numbers': [], 'po_numbers': [], 'po_line_statuses': []
        }

def get_invoice_details(can_line_ids: List[int]) -> pd.DataFrame:
    """
    Get detailed information for selected CAN lines
    
    CRITICAL FIX: Use direct FK from arrival_details.product_purchase_order_id
    instead of complex join via products table which causes wrong mapping
    """
    try:
        engine = get_db_engine()
        
        # FIXED QUERY - Use direct FKs, no complex product matching
        query = """
        SELECT 
            ad.id AS can_line_id,
            ad.id AS arrival_detail_id,
            
            -- Direct FK from arrival_details (CORRECT!)
            ad.product_purchase_order_id,
            ppo.purchase_order_id,
            
            -- AN Info
            a.arrival_note_number,
            
            -- PO Info
            po.po_number,
            po.currency_id AS po_currency_id,
            c.code AS po_currency_code,
            po.seller_company_id AS vendor_id,
            po.buyer_company_id AS entity_id,
            po.payment_term_id,
            
            -- Payment Terms
            pt.name AS payment_term_name,
            
            -- Product Info (from ppo, not from can_tracking_full_view)
            p.name AS product_name,
            p.pt_code,
            
            -- Vendor Info
            seller.english_name AS vendor,
            seller.company_code AS vendor_code,
            
            -- Quantity & Cost
            ad.arrival_quantity AS uninvoiced_quantity,
            ppo.purchaseuom AS buying_uom,
            CONCAT(
                ROUND(ppo.purchase_unit_cost, 2), 
                ' ', 
                c.code
            ) AS buying_unit_cost,
            
            -- Payment term from view (for compatibility)
            pt.name AS payment_term
            
        FROM arrival_details ad
        
        -- Core joins using direct FKs (CORRECT PATH)
        INNER JOIN arrivals a 
            ON a.id = ad.arrival_id 
            AND a.delete_flag = 0
        
        INNER JOIN product_purchase_orders ppo 
            ON ppo.id = ad.product_purchase_order_id  -- Direct FK!
            AND ppo.delete_flag = 0
        
        INNER JOIN purchase_orders po 
            ON po.id = ppo.purchase_order_id
            AND po.delete_flag = 0
        
        INNER JOIN products p 
            ON p.id = ppo.product_id
        
        INNER JOIN companies seller 
            ON seller.id = po.seller_company_id
        
        INNER JOIN currencies c 
            ON c.id = po.currency_id
        
        LEFT JOIN payment_terms pt 
            ON pt.id = po.payment_term_id
        
        WHERE ad.id IN :can_line_ids
            AND ad.delete_flag = 0
        
        ORDER BY a.arrival_note_number, ad.id
        """
        
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params={'can_line_ids': tuple(can_line_ids)})
        
        if not df.empty:
            df['payment_term_days'] = df['payment_term_name'].apply(calculate_days_from_term_name)
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting invoice details: {e}")
        return pd.DataFrame()

def validate_invoice_selection(selected_df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Basic validation for selected ANs
    
    Returns:
        (is_valid, error_message)
    """
    if selected_df.empty:
        return False, "No items selected"
    
    # Check single vendor
    vendors = selected_df['vendor_code'].unique()
    if len(vendors) > 1:
        return False, f"Multiple vendors selected: {', '.join(vendors)}. Please select ANs from a single vendor."
    
    # Check vendor type consistency
    vendor_types = selected_df['vendor_type'].unique()
    if len(vendor_types) > 1:
        return False, "Cannot mix Internal and External vendors in the same invoice"
    
    # Check single legal entity
    entities = selected_df['legal_entity_code'].unique()
    if len(entities) > 1:
        return False, f"Multiple legal entities selected: {', '.join(entities)}. Please select ANs from a single entity."
    
    return True, ""

def create_purchase_invoice(
    invoice_data: Dict, 
    details_df: pd.DataFrame, 
    keycloak_id: str,
    media_ids: List[int] = None
) -> Tuple[bool, str, Optional[int]]:
    """
    Create purchase invoice with proper VAT field handling and optional file attachments
    
    Args:
        invoice_data: Invoice header data dictionary
        details_df: DataFrame with invoice line items
        keycloak_id: User's keycloak_id (not username)
        media_ids: Optional list of media IDs to link to invoice
        
    Returns:
        Tuple of (success, message, invoice_id)
    """
    engine = get_db_engine()
    
    try:
        with engine.begin() as conn:
            # Calculate total amounts excluding VAT
            total_amount_exclude_vat = 0
            po_to_invoice_rate = invoice_data.get('po_to_invoice_rate', 1.0)
            
            # First pass: calculate totals
            for _, row in details_df.iterrows():
                unit_cost_str = row['buying_unit_cost']
                unit_cost = float(unit_cost_str.split()[0])
                quantity = row['uninvoiced_quantity']
                
                # Calculate base amount in invoice currency (excluding VAT)
                base_amount_in_invoice_currency = unit_cost * quantity * po_to_invoice_rate
                total_amount_exclude_vat += base_amount_in_invoice_currency
            
            # Prepare header data
            header_params = {
                'invoice_number': invoice_data['invoice_number'],
                'invoiced_date': invoice_data['invoiced_date'],
                'due_date': invoice_data['due_date'],
                'total_invoiced_amount': invoice_data['total_invoiced_amount'],
                'total_invoiced_amount_exclude_vat': round(total_amount_exclude_vat, 2),
                'seller_id': invoice_data['seller_id'],
                'buyer_id': invoice_data['buyer_id'],
                'currency_id': invoice_data['currency_id'],
                'payment_term_id': invoice_data['payment_term_id'],
                'created_by': keycloak_id
            }
            
            # Add optional fields
            if invoice_data.get('commercial_invoice_no'):
                header_params['commercial_invoice_no'] = invoice_data['commercial_invoice_no']
            
            if invoice_data.get('usd_exchange_rate') is not None:
                header_params['usd_exchange_rate'] = invoice_data['usd_exchange_rate']
            
            if invoice_data.get('invoice_type'):
                header_params['invoice_type'] = invoice_data['invoice_type']
            
            if invoice_data.get('email_to_accountant') is not None:
                header_params['email_to_accountant'] = invoice_data['email_to_accountant']
            
            if invoice_data.get('advance_payment') is not None:
                header_params['advance_payment'] = invoice_data['advance_payment']
            
            # Build INSERT query
            columns = list(header_params.keys())
            values = [f":{key}" for key in columns]
            
            header_query = text(f"""
            INSERT INTO purchase_invoices (
                {', '.join(columns)},
                created_date,
                delete_flag
            ) VALUES (
                {', '.join(values)},
                NOW(),
                0
            )
            """)
            
            result = conn.execute(header_query, header_params)
            invoice_id = result.lastrowid
            
            # Insert purchase_invoice_details with VAT fields
            for _, row in details_df.iterrows():
                # Extract unit cost
                unit_cost_str = row['buying_unit_cost']
                unit_cost = float(unit_cost_str.split()[0])
                
                # Get VAT percentage from product_purchase_orders
                vat_percent = 0
                if 'product_purchase_order_id' in row and row['product_purchase_order_id']:
                    vat_query = text("""
                    SELECT vat_gst 
                    FROM product_purchase_orders 
                    WHERE id = :ppo_id 
                    AND delete_flag = 0
                    """)
                    vat_result = conn.execute(vat_query, {'ppo_id': row['product_purchase_order_id']}).fetchone()
                    if vat_result and vat_result[0] is not None:
                        vat_percent = float(vat_result[0])
                
                # Calculate amounts
                quantity = row['uninvoiced_quantity']
                
                # Amount excluding VAT in invoice currency
                amount_exclude_vat = round(unit_cost * quantity * po_to_invoice_rate, 2)
                
                # Amount including VAT
                vat_multiplier = 1 + (vat_percent / 100)
                amount_include_vat = round(amount_exclude_vat * vat_multiplier, 2)
                
                detail_params = {
                    'purchase_invoice_id': invoice_id,
                    'purchase_order_id': row['purchase_order_id'],
                    'product_purchase_order_id': row['product_purchase_order_id'],
                    'arrival_detail_id': row['arrival_detail_id'],
                    'purchased_invoice_quantity': quantity,
                    'invoiced_quantity': quantity,
                    'amount': amount_include_vat,
                    'amount_exclude_vat': amount_exclude_vat,
                    'vat_gst': vat_percent,
                    'exchange_rate': po_to_invoice_rate
                }
                
                detail_query = text("""
                INSERT INTO purchase_invoice_details (
                    purchase_invoice_id,
                    purchase_order_id,
                    product_purchase_order_id,
                    arrival_detail_id,
                    purchased_invoice_quantity,
                    invoiced_quantity,
                    amount,
                    amount_exclude_vat,
                    vat_gst,
                    exchange_rate,
                    delete_flag
                ) VALUES (
                    :purchase_invoice_id,
                    :purchase_order_id,
                    :product_purchase_order_id,
                    :arrival_detail_id,
                    :purchased_invoice_quantity,
                    :invoiced_quantity,
                    :amount,
                    :amount_exclude_vat,
                    :vat_gst,
                    :exchange_rate,
                    0
                )
                """)
                
                conn.execute(detail_query, detail_params)
            
            # Link media files if provided
            if media_ids:
                for media_id in media_ids:
                    media_link_query = text("""
                    INSERT INTO purchase_invoice_medias (
                        purchase_invoice_id,
                        media_id,
                        created_by,
                        created_date,
                        delete_flag,
                        version
                    ) VALUES (
                        :purchase_invoice_id,
                        :media_id,
                        :created_by,
                        NOW(),
                        0,
                        0
                    )
                    """)
                    
                    conn.execute(media_link_query, {
                        'purchase_invoice_id': invoice_id,
                        'media_id': media_id,
                        'created_by': keycloak_id
                    })
                    
                    logger.info(f"Linked media {media_id} to invoice {invoice_id}")
            
            logger.info(f"Invoice {invoice_data['invoice_number']} created successfully with ID {invoice_id}")
            return True, f"Invoice {invoice_data['invoice_number']} created successfully", invoice_id
            
    except Exception as e:
        logger.error(f"Error creating invoice: {e}")
        return False, f"Error creating invoice: {str(e)}", None

def generate_invoice_number(vendor_id: int, buyer_id: int, is_advance_payment: bool = False) -> str:
    """Generate unique invoice number"""
    try:
        engine = get_db_engine()
        
        today = datetime.now()
        date_str = today.strftime("%Y%m%d")
        
        vendor_id = int(vendor_id) if vendor_id is not None else 0
        buyer_id = int(buyer_id) if buyer_id is not None else 0
        
        query = text("""
        SELECT MAX(id) as max_id
        FROM purchase_invoices
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query).fetchone()
            last_id = result[0] if result and result[0] else 0
        
        seq = last_id + 1
        suffix = 'A' if is_advance_payment else 'P'
        
        invoice_number = f"V-INV{date_str}-{vendor_id}{buyer_id}{seq}-{suffix}"
        
        return invoice_number
        
    except Exception as e:
        logger.error(f"Error generating invoice number: {e}")
        vendor_id = int(vendor_id) if vendor_id is not None else 0
        buyer_id = int(buyer_id) if buyer_id is not None else 0
        suffix = 'A' if is_advance_payment else 'P'
        timestamp = datetime.now().strftime('%H%M%S')
        return f"V-INV{datetime.now().strftime('%Y%m%d')}-{vendor_id}{buyer_id}{timestamp}-{suffix}"

@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_payment_terms() -> pd.DataFrame:
    """Get available payment terms from database"""
    try:
        engine = get_db_engine()
        
        query = text("""
        SELECT 
            id,
            name,
            COALESCE(description, name) AS description
        FROM payment_terms
        WHERE delete_flag = 0
        ORDER BY name ASC
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        if not df.empty:
            df['days'] = df['name'].apply(calculate_days_from_term_name)
            df = df.sort_values(['days', 'name'])
        
        if df.empty:
            df = pd.DataFrame([
                {'id': 1, 'name': 'Net 30', 'days': 30, 'description': 'Payment due in 30 days'},
                {'id': 2, 'name': 'Net 60', 'days': 60, 'description': 'Payment due in 60 days'},
                {'id': 3, 'name': 'Net 90', 'days': 90, 'description': 'Payment due in 90 days'},
                {'id': 4, 'name': 'COD', 'days': 0, 'description': 'Cash on delivery'}
            ])
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting payment terms: {e}")
        return pd.DataFrame([
            {'id': 1, 'name': 'Net 30', 'days': 30, 'description': 'Payment due in 30 days'},
            {'id': 2, 'name': 'Net 60', 'days': 60, 'description': 'Payment due in 60 days'},
            {'id': 3, 'name': 'Net 90', 'days': 90, 'description': 'Payment due in 90 days'},
            {'id': 4, 'name': 'COD', 'days': 0, 'description': 'Cash on delivery'}
        ])

def calculate_days_from_term_name(term_name: str) -> int:
    """
    Calculate days from payment term name - ENHANCED VERSION
    Uses PaymentTermParser for accurate parsing
    """
    if pd.isna(term_name):
        return 30
    
    term_name = str(term_name).strip()
    term_upper = term_name.upper()
    
    # Check immediate payment
    immediate_terms = ['COD', 'CIA', 'TT IN ADVANCE', 'ADVANCE', 'PREPAID']
    if any(term in term_upper for term in immediate_terms):
        return 0
    
    # Extract number with regex - NET pattern
    net_pattern = r'NET\s+(\d+)'
    match = re.search(net_pattern, term_upper)
    if match:
        return int(match.group(1))
    
    # AMS pattern
    ams_pattern = r'AMS\s+(\d+)'
    match = re.search(ams_pattern, term_upper)
    if match:
        return int(match.group(1)) + 15  # Approximate
    
    # Days pattern
    days_pattern = r'(\d+)\s*DAYS?'
    match = re.search(days_pattern, term_upper)
    if match:
        return int(match.group(1))
    
    # Any number
    number_pattern = r'\d+'
    match = re.search(number_pattern, term_name)
    if match:
        return int(match.group(0))
    
    return 30

@st.cache_data(ttl=60)
def get_po_line_summary(po_line_ids: List[int]) -> pd.DataFrame:
    """
    Get PO line level summary including legacy invoice information
    """
    try:
        if not po_line_ids:
            return pd.DataFrame()
        
        engine = get_db_engine()
        
        query = text("""
        WITH legacy_invoices AS (
            SELECT 
                pid.product_purchase_order_id,
                SUM(pid.purchased_invoice_quantity) as legacy_invoice_qty,
                COUNT(DISTINCT pid.purchase_invoice_id) as legacy_invoice_count
            FROM purchase_invoice_details pid
            JOIN purchase_invoices pi ON pid.purchase_invoice_id = pi.id
            WHERE pid.arrival_detail_id IS NULL
                AND pid.delete_flag = 0
                AND pi.delete_flag = 0
                AND pid.product_purchase_order_id IN :po_line_ids
            GROUP BY pid.product_purchase_order_id
        ),
        new_invoices AS (
            SELECT 
                pid.product_purchase_order_id,
                SUM(pid.purchased_invoice_quantity) as new_invoice_qty
            FROM purchase_invoice_details pid
            JOIN purchase_invoices pi ON pid.purchase_invoice_id = pi.id
            WHERE pid.arrival_detail_id IS NOT NULL
                AND pid.delete_flag = 0
                AND pi.delete_flag = 0
                AND pid.product_purchase_order_id IN :po_line_ids
            GROUP BY pid.product_purchase_order_id
        )
        SELECT 
            ppo.id as product_purchase_order_id,
            po.po_number,
            p.pt_code,
            p.name as product_name,
            ppo.purchase_quantity as po_buying_qty,
            COALESCE(li.legacy_invoice_qty, 0) as legacy_invoice_qty,
            COALESCE(ni.new_invoice_qty, 0) as new_invoice_qty,
            ppo.purchase_quantity - (COALESCE(li.legacy_invoice_qty, 0) + COALESCE(ni.new_invoice_qty, 0)) as po_remaining_qty
        FROM product_purchase_orders ppo
        JOIN purchase_orders po ON ppo.purchase_order_id = po.id
        JOIN products p ON ppo.product_id = p.id
        LEFT JOIN legacy_invoices li ON li.product_purchase_order_id = ppo.id
        LEFT JOIN new_invoices ni ON ni.product_purchase_order_id = ppo.id
        WHERE ppo.id IN :po_line_ids
            AND ppo.delete_flag = 0
            AND po.delete_flag = 0
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={'po_line_ids': tuple(po_line_ids)})
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting PO line summary: {e}")
        return pd.DataFrame()

# ============================================================================
# INVOICE MANAGEMENT FUNCTIONS (CRUD)
# ============================================================================

@st.cache_data(ttl=60)
def get_recent_invoices(limit: int = 100) -> pd.DataFrame:
    """
    Get recent invoices using the purchase_invoice_full_view
    """
    try:
        engine = get_db_engine()
        
        query = text("""
        SELECT DISTINCT
            pi_id as id,
            inv_number as invoice_number,
            commercial_inv_number as commercial_invoice_no,
            inv_date as invoiced_date,
            due_date,
            total_invoiced_amount,
            vendor,
            vendor_code,
            legal_entity as buyer,
            legal_entity_code as buyer_code,
            invoiced_currency as currency,
            payment_term,
            created_by,
            inv_type as invoice_type,
            is_advance_payment as advance_payment,
            payment_status,
            total_outstanding_amount,
            aging_status,
            risk_level,
            days_overdue,
            payment_count,
            last_payment_date
        FROM purchase_invoice_full_view
        ORDER BY inv_date DESC, inv_number DESC
        LIMIT :limit
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={'limit': limit})
            
            # Remove duplicates and add line count
            if not df.empty:
                df = df.drop_duplicates(subset=['id'])
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching recent invoices: {e}")
        return pd.DataFrame()

def get_invoice_by_id(invoice_id: int) -> Optional[Dict]:
    """
    Get single invoice by ID with full details
    """
    try:
        engine = get_db_engine()
        
        query = text("""
        SELECT DISTINCT
            pi_id as id,
            inv_number as invoice_number,
            commercial_inv_number as commercial_invoice_no,
            inv_date as invoiced_date,
            due_date,
            total_invoiced_amount,
            vendor_code,
            vendor as vendor_name,
            legal_entity_code as buyer_code,
            legal_entity as buyer_name,
            invoiced_currency as currency_code,
            payment_term as payment_term_name,
            created_by,
            inv_type as invoice_type,
            is_advance_payment,
            email_to_accountant,
            payment_status,
            total_outstanding_amount,
            total_payment_made,
            payment_ratio,
            aging_status,
            risk_level
        FROM purchase_invoice_full_view
        WHERE pi_id = :invoice_id
        LIMIT 1
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {'invoice_id': invoice_id}).fetchone()
            if result:
                return dict(result._mapping)
        
        return None
        
    except Exception as e:
        logger.error(f"Error fetching invoice {invoice_id}: {e}")
        return None

def update_invoice(invoice_id: int, update_data: Dict) -> Tuple[bool, str]:
    """
    Update invoice header information
    """
    try:
        engine = get_db_engine()
        
        # Build dynamic update query
        update_fields = []
        params = {'invoice_id': invoice_id}
        
        # Whitelist of updatable fields
        updatable_fields = [
            'commercial_invoice_no', 'invoiced_date', 'due_date',
            'email_to_accountant', 'modified_date'
        ]
        
        for field, value in update_data.items():
            if field in updatable_fields:
                update_fields.append(f"{field} = :{field}")
                params[field] = value
        
        if not update_fields:
            return False, "No valid fields to update"
        
        # Add modified date if not provided
        if 'modified_date' not in params:
            update_fields.append("modified_date = NOW()")
        
        query = text(f"""
        UPDATE purchase_invoices
        SET {', '.join(update_fields)}
        WHERE id = :invoice_id
            AND delete_flag = 0
        """)
        
        with engine.begin() as conn:
            result = conn.execute(query, params)
            
            if result.rowcount > 0:
                logger.info(f"Updated invoice {invoice_id}")
                return True, "Invoice updated successfully"
            else:
                return False, "Invoice not found or already deleted"
                
    except Exception as e:
        logger.error(f"Error updating invoice {invoice_id}: {e}")
        return False, f"Error: {str(e)}"

def delete_invoice(invoice_id: int, hard_delete: bool = False) -> Tuple[bool, str]:
    """
    Delete or void an invoice
    """
    try:
        engine = get_db_engine()
        
        if hard_delete:
            # Check if invoice can be deleted (no dependencies)
            check_query = text("""
            SELECT COUNT(*) as detail_count
            FROM purchase_invoice_details
            WHERE purchase_invoice_id = :invoice_id
                AND delete_flag = 0
            """)
            
            with engine.connect() as conn:
                result = conn.execute(check_query, {'invoice_id': invoice_id}).fetchone()
                if result['detail_count'] > 0:
                    return False, "Cannot delete invoice with line items. Void it instead."
            
            # Hard delete
            delete_query = text("""
            DELETE FROM purchase_invoices
            WHERE id = :invoice_id
            """)
        else:
            # Soft delete (void)
            delete_query = text("""
            UPDATE purchase_invoices
            SET delete_flag = 1,
                modified_date = NOW()
            WHERE id = :invoice_id
                AND delete_flag = 0
            """)
        
        with engine.begin() as conn:
            result = conn.execute(delete_query, {'invoice_id': invoice_id})
            
            if result.rowcount > 0:
                action = "deleted" if hard_delete else "voided"
                logger.info(f"Invoice {invoice_id} {action}")
                return True, f"Invoice {action} successfully"
            else:
                return False, "Invoice not found or already deleted"
                
    except Exception as e:
        logger.error(f"Error deleting invoice {invoice_id}: {e}")
        return False, f"Error: {str(e)}"

def get_invoice_line_items(invoice_id: int) -> pd.DataFrame:
    """
    Get line items for an invoice using the view
    """
    try:
        engine = get_db_engine()
        
        query = text("""
        SELECT 
            pi_line_id,
            po_number,
            pt_code,
            product_name,
            vendor_product_code,
            brand,
            invoiced_quantity as purchased_invoice_quantity,
            buying_uom,
            inv_unit_price,
            invoiced_amount as amount,
            vat_percent as vat_gst,
            can_number as arrival_note_number,
            arrival_date,
            
            -- PO quantities with cancellation context
            po_original_buying_quantity,
            po_cancelled_buying_quantity,
            buying_ordered_quantity as effective_po_quantity,
            remaining_buying_qty_to_invoice,
            invoice_completion_percent,
            
            -- Status indicators
            invoice_status,
            po_cancellation_status,
            is_over_invoiced
            
        FROM purchase_invoice_full_view
        WHERE pi_id = :invoice_id
        ORDER BY pi_line_id
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={'invoice_id': invoice_id})
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting invoice line items: {e}")
        return pd.DataFrame()

def get_invoice_summary_by_vendor(start_date: date = None, end_date: date = None) -> pd.DataFrame:
    """
    Get invoice summary grouped by vendor using the view
    """
    try:
        engine = get_db_engine()
        
        query = """
        SELECT 
            vendor_code,
            vendor as vendor_name,
            invoiced_currency as currency,
            COUNT(DISTINCT pi_id) as invoice_count,
            SUM(DISTINCT total_invoiced_amount) as total_amount,
            AVG(DISTINCT total_invoiced_amount) as avg_amount,
            MAX(inv_date) as last_invoice_date,
            COUNT(DISTINCT CASE WHEN is_advance_payment = 1 THEN pi_id END) as advance_payment_count,
            COUNT(DISTINCT CASE WHEN is_advance_payment = 0 THEN pi_id END) as commercial_invoice_count,
            SUM(DISTINCT total_outstanding_amount) as total_outstanding,
            AVG(DISTINCT payment_ratio) as avg_payment_ratio
        FROM purchase_invoice_full_view
        WHERE 1=1
        """
        
        params = {}
        if start_date:
            query += " AND inv_date >= :start_date"
            params['start_date'] = start_date
        if end_date:
            query += " AND inv_date <= :end_date"
            params['end_date'] = end_date
        
        query += """
        GROUP BY vendor_code, vendor, invoiced_currency
        ORDER BY total_amount DESC
        """
        
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting vendor summary: {e}")
        return pd.DataFrame()

def get_invoice_aging_report() -> pd.DataFrame:
    """
    Get aging report using the view which already has payment status
    """
    try:
        engine = get_db_engine()
        
        query = text("""
        SELECT DISTINCT
            inv_number as invoice_number,
            vendor,
            total_invoiced_amount,
            invoiced_currency as currency,
            inv_date as invoiced_date,
            due_date,
            days_overdue,
            aging_status,
            payment_status,
            total_outstanding_amount,
            payment_ratio,
            risk_level
        FROM purchase_invoice_full_view
        WHERE payment_status != 'Fully Paid'
        ORDER BY days_overdue DESC
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            
            if not df.empty:
                df = df.drop_duplicates(subset=['invoice_number'])
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting aging report: {e}")
        return pd.DataFrame()

def validate_invoice_edit(invoice_id: int, update_data: Dict) -> Tuple[bool, str]:
    """
    Validate invoice edit before updating
    """
    try:
        # Get current invoice
        invoice = get_invoice_by_id(invoice_id)
        if not invoice:
            return False, "Invoice not found"
        
        # Business rules validation
        if 'invoiced_date' in update_data:
            new_date = pd.to_datetime(update_data['invoiced_date'])
            if new_date > datetime.now():
                return False, "Invoice date cannot be in the future"
        
        if 'due_date' in update_data:
            due_date = pd.to_datetime(update_data['due_date'])
            invoice_date = pd.to_datetime(update_data.get('invoiced_date', invoice['invoiced_date']))
            if due_date < invoice_date:
                return False, "Due date cannot be before invoice date"
        
        # Check if invoice is already fully paid
        if invoice.get('payment_status') == 'Fully Paid':
            return False, "Cannot edit fully paid invoice"
        
        return True, "Validation passed"
        
    except Exception as e:
        logger.error(f"Error validating invoice edit: {e}")
        return False, f"Validation error: {str(e)}"