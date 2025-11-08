# utils/currency_utils.py

import requests
import logging
import streamlit as st
import pandas as pd
from typing import Dict, Optional, Tuple, List
from datetime import datetime, timedelta
from .db import get_db_engine
from sqlalchemy import text
import os

logger = logging.getLogger(__name__)

# Cache for exchange rates (in memory)
_rate_cache = {}
_cache_expiry = {}

def get_latest_exchange_rate(from_currency: str, to_currency: str) -> Optional[float]:
    """
    Get exchange rate from API with caching, fallback to database if API fails
    
    Args:
        from_currency: Source currency code (e.g., 'USD')
        to_currency: Target currency code (e.g., 'VND')
    
    Returns:
        Exchange rate (from_currency/to_currency) or None if not available
    """
    # Check if same currency
    if from_currency == to_currency:
        return 1.0
    
    # Check cache first
    cache_key = f"{from_currency}-{to_currency}"
    now = datetime.now()
    
    if cache_key in _rate_cache and cache_key in _cache_expiry:
        if _cache_expiry[cache_key] > now:
            return _rate_cache[cache_key]
    
    # Try API first
    try:
        api_key = os.getenv('EXCHANGE_RATE_API_KEY')
        if not api_key:
            logger.warning("No API key found, falling back to database")
            return get_rate_from_database(from_currency, to_currency)
        
        # API supports all currency conversions
        url = f"http://api.exchangeratesapi.io/v1/convert?access_key={api_key}&from={from_currency}&to={to_currency}&amount=1"
        
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('success', False):
            rate = data.get('result')
            
            if rate is not None:
                # Cache the result for 1 hour
                _rate_cache[cache_key] = rate
                _cache_expiry[cache_key] = now + timedelta(hours=1)
                
                logger.info(f"Successfully fetched rate {from_currency}/{to_currency}: {rate}")
                return rate
        else:
            logger.error(f"API error: {data.get('error', {}).get('info', 'Unknown error')}")
            
    except Exception as e:
        logger.error(f"Error fetching exchange rate from API: {e}")
    
    # Fallback to database
    return get_rate_from_database(from_currency, to_currency)

def get_rate_from_database(from_currency: str, to_currency: str) -> Optional[float]:
    """Get latest exchange rate from database"""
    try:
        engine = get_db_engine()
        
        # Get the most recent rate for this currency pair
        query = text("""
        SELECT rate_value 
        FROM exchange_rates 
        WHERE from_currency_code = :from_curr 
        AND to_currency_code = :to_curr
        AND delete_flag = 0
        ORDER BY rate_date DESC, created_date DESC
        LIMIT 1
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {
                'from_curr': from_currency,
                'to_curr': to_currency
            }).fetchone()
            
            if result and result[0] is not None:
                rate = float(result[0])
                logger.info(f"Database rate {from_currency}/{to_currency}: {rate}")
                return rate
            
            # Try inverse rate
            inverse_query = text("""
            SELECT rate_value 
            FROM exchange_rates 
            WHERE from_currency_code = :to_curr 
            AND to_currency_code = :from_curr
            AND delete_flag = 0
            ORDER BY rate_date DESC, created_date DESC
            LIMIT 1
            """)
            
            result = conn.execute(inverse_query, {
                'from_curr': from_currency,
                'to_curr': to_currency
            }).fetchone()
            
            if result and result[0] is not None and result[0] > 0:
                rate = 1.0 / float(result[0])
                logger.info(f"Database inverse rate {from_currency}/{to_currency}: {rate}")
                return rate
                
    except Exception as e:
        logger.error(f"Error getting rate from database: {e}")
    
    logger.warning(f"No rate found for {from_currency}/{to_currency}")
    return None

@st.cache_data(ttl=3600)
def get_available_currencies() -> pd.DataFrame:
    """Get available currencies from database"""
    try:
        engine = get_db_engine()
        
        query = text("""
        SELECT 
            id,
            code,
            name
        FROM currencies
        WHERE delete_flag = 0
        AND code IS NOT NULL
        ORDER BY 
            CASE code 
                WHEN 'USD' THEN 1
                WHEN 'VND' THEN 2
                WHEN 'EUR' THEN 3
                WHEN 'SGD' THEN 4
                ELSE 5
            END,
            code
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting currencies: {e}")
        # Return empty DataFrame instead of defaults
        return pd.DataFrame()

def calculate_exchange_rates(po_currency_code: str, invoice_currency_code: str) -> Dict[str, Optional[float]]:
    """
    Calculate all necessary exchange rates for invoice
    Always fetches USD rate for reporting purposes
    
    Returns dict with:
    - usd_exchange_rate: USD to invoice currency rate (always calculated)
    - po_to_invoice_rate: PO currency to invoice currency rate
    """
    rates = {}
    
    # ALWAYS get USD to invoice currency rate (important for financial reporting)
    if invoice_currency_code == 'USD':
        rates['usd_exchange_rate'] = 1.0
        logger.info("Invoice currency is USD, rate = 1.0")
    else:
        # Always fetch USD rate, even if not needed for conversion
        rates['usd_exchange_rate'] = get_latest_exchange_rate('USD', invoice_currency_code)
        
        # Log warning if USD rate couldn't be fetched
        if rates['usd_exchange_rate'] is None:
            logger.warning(f"Could not fetch USD to {invoice_currency_code} exchange rate")
        else:
            logger.info(f"USD to {invoice_currency_code} rate: {rates['usd_exchange_rate']}")
    
    # PO to invoice currency rate
    if po_currency_code == invoice_currency_code:
        rates['po_to_invoice_rate'] = 1.0
        logger.info(f"Same currency {po_currency_code}, conversion rate = 1.0")
    else:
        rates['po_to_invoice_rate'] = get_latest_exchange_rate(po_currency_code, invoice_currency_code)
        
        # Log warning if conversion rate couldn't be fetched
        if rates['po_to_invoice_rate'] is None:
            logger.warning(f"Could not fetch {po_currency_code} to {invoice_currency_code} exchange rate")
        else:
            logger.info(f"{po_currency_code} to {invoice_currency_code} rate: {rates['po_to_invoice_rate']}")
    
    return rates

def validate_exchange_rates(rates: Dict[str, Optional[float]], 
                          po_currency: str, 
                          invoice_currency: str) -> Tuple[bool, List[str]]:
    """
    Validate that all required exchange rates are available
    
    Returns:
        (is_valid, list_of_warnings)
    """
    warnings = []
    is_valid = True
    
    # Check PO to invoice rate if currencies are different
    if po_currency != invoice_currency:
        if rates.get('po_to_invoice_rate') is None:
            warnings.append(f"Cannot convert {po_currency} to {invoice_currency}")
            is_valid = False
    
    # USD rate is important but not critical for invoice creation
    if invoice_currency != 'USD' and rates.get('usd_exchange_rate') is None:
        warnings.append(f"USD to {invoice_currency} rate not available (needed for reporting)")
        # Don't set is_valid to False - this is just a warning
    
    return is_valid, warnings

def format_exchange_rate(rate: Optional[float]) -> str:
    """Format exchange rate for display"""
    if rate is None:
        return "N/A"
        
    if rate >= 1000:
        return f"{rate:,.2f}"
    elif rate >= 10:
        return f"{rate:,.4f}"
    elif rate >= 1:
        return f"{rate:,.6f}"
    else:
        # For small rates, show more decimal places
        zeros = 0
        temp_rate = rate
        while temp_rate < 0.1:
            temp_rate *= 10
            zeros += 1
        return f"{rate:,.{zeros + 3}f}"

def get_invoice_amounts_in_currency(
    selected_df: pd.DataFrame, 
    po_currency: str, 
    invoice_currency: str
) -> Optional[Dict[str, float]]:
    """
    Calculate invoice amounts in the selected invoice currency
    
    Args:
        selected_df: DataFrame with selected invoice lines
        po_currency: Original PO currency code
        invoice_currency: Selected invoice currency code
        
    Returns:
        Dictionary with converted amounts or None if exchange rate unavailable
    """
    if po_currency == invoice_currency:
        # No conversion needed
        exchange_rate = 1.0
    else:
        exchange_rate = get_latest_exchange_rate(po_currency, invoice_currency)
        
        # Return None if exchange rate is not available
        if exchange_rate is None:
            logger.error(f"Cannot calculate amounts: No exchange rate for {po_currency}/{invoice_currency}")
            return None
    
    total_amount = 0
    total_vat = 0
    
    for _, row in selected_df.iterrows():
        # Extract unit cost (assuming format "123.45 USD")
        cost_str = str(row.get('buying_unit_cost', '0'))
        if ' ' in cost_str:
            unit_cost = float(cost_str.split()[0])
        else:
            unit_cost = float(cost_str)
        
        quantity = row.get('uninvoiced_quantity', 0)
        vat_percent = row.get('vat_percent', 0)
        
        # Calculate line amount in original currency
        line_amount = unit_cost * quantity
        
        # Convert to invoice currency
        line_amount_converted = line_amount * exchange_rate
        
        # Calculate VAT
        vat_amount = line_amount_converted * vat_percent / 100
        
        total_amount += line_amount_converted
        total_vat += vat_amount
    
    return {
        'exchange_rate': exchange_rate,
        'subtotal': round(total_amount, 2),
        'total_vat': round(total_vat, 2),
        'total_with_vat': round(total_amount + total_vat, 2),
        'currency': invoice_currency
    }