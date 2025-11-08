# utils/invoice_service.py

from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List, Optional, Tuple
import logging
from .invoice_data import get_payment_terms, get_po_line_summary

logger = logging.getLogger(__name__)

class InvoiceService:
    """Service class for invoice business logic with enhanced PO level validation"""
    
    @staticmethod
    def calculate_due_date(invoice_date: datetime, payment_term_days: int = 30) -> datetime:
        """Calculate due date based on payment terms"""
        return invoice_date + timedelta(days=payment_term_days)
    
    @staticmethod
    def calculate_invoice_totals(df: pd.DataFrame) -> Dict:
        """Calculate invoice totals from selected ANs (used at line 496)"""
        totals = {
            'total_quantity': df['uninvoiced_quantity'].sum(),
            'total_lines': len(df),
            'po_count': df['po_number'].nunique(),
            'an_count': df['arrival_note_number'].nunique()
        }
        
        # Calculate total value
        total_value = 0
        currency = None
        
        for _, row in df.iterrows():
            cost_parts = str(row['buying_unit_cost']).split()
            if len(cost_parts) >= 2:
                unit_cost = float(cost_parts[0])
                if not currency:
                    currency = cost_parts[1]
                
                # Use true_remaining_qty if available
                qty = row.get('true_remaining_qty', row['uninvoiced_quantity'])
                total_value += unit_cost * qty
        
        totals['total_value'] = round(total_value, 2)
        totals['currency'] = currency or 'USD'
        
        return totals

    @staticmethod
    def calculate_invoice_totals_with_vat(df: pd.DataFrame) -> Dict:
        """Calculate invoice totals including VAT breakdown (used at line 850)"""
        totals = {
            'total_quantity': df['uninvoiced_quantity'].sum(),
            'total_lines': len(df),
            'po_count': df['po_number'].nunique(),
            'an_count': df['arrival_note_number'].nunique()
        }
        
        # Calculate subtotal and VAT
        subtotal = 0
        total_vat = 0
        currency = None
        
        for _, row in df.iterrows():
            cost_parts = str(row['buying_unit_cost']).split()
            if len(cost_parts) >= 2:
                unit_cost = float(cost_parts[0])
                if not currency:
                    currency = cost_parts[1]
                
                qty = row.get('true_remaining_qty', row['uninvoiced_quantity'])
                line_amount = unit_cost * qty
                subtotal += line_amount
                
                # Calculate VAT
                vat_percent = row.get('vat_percent', 0)
                vat_amount = line_amount * vat_percent / 100
                total_vat += vat_amount
        
        totals['subtotal'] = round(subtotal, 2)
        totals['total_vat'] = round(total_vat, 2)
        totals['total_with_vat'] = round(subtotal + total_vat, 2)
        totals['currency'] = currency or 'USD'
        
        return totals

    @staticmethod
    def prepare_invoice_summary(df: pd.DataFrame) -> pd.DataFrame:
        """Prepare summary for invoice preview with ID column (used at lines 844, 847)"""
        # Group by PO, product, and VAT rate
        summary = df.groupby(['po_number', 'pt_code', 'product_name', 'buying_unit_cost', 'vat_percent']).agg({
            'uninvoiced_quantity': 'sum',
            'true_remaining_qty': 'sum' if 'true_remaining_qty' in df.columns else lambda x: None,
            'arrival_note_number': lambda x: ', '.join(x.unique())
        }).reset_index()
        
        # Add row ID column (starting from 1)
        summary.insert(0, 'id', range(1, len(summary) + 1))
        
        # Use true_remaining_qty if available
        if 'true_remaining_qty' in summary.columns and summary['true_remaining_qty'].notna().any():
            qty_col = 'true_remaining_qty'
        else:
            qty_col = 'uninvoiced_quantity'
        
        # Calculate amounts
        summary['line_amount'] = summary.apply(
            lambda row: float(str(row['buying_unit_cost']).split()[0]) * row[qty_col], 
            axis=1
        )
        
        summary['vat_amount'] = summary['line_amount'] * summary['vat_percent'] / 100
        summary['total_amount'] = summary['line_amount'] + summary['vat_amount']
        
        # Format for display
        summary['vat_display'] = summary['vat_percent'].apply(lambda x: f"{x:.0f}%")
        
        # Add warning if quantity was adjusted
        if 'true_remaining_qty' in summary.columns:
            summary['adjusted'] = summary.apply(
                lambda row: '⚠️' if row.get('true_remaining_qty', 0) < row['uninvoiced_quantity'] else '',
                axis=1
            )
        
        # Format monetary values
        summary['line_amount'] = summary['line_amount'].apply(lambda x: f"{x:,.2f}")
        summary['vat_amount'] = summary['vat_amount'].apply(lambda x: f"{x:,.2f}")
        summary['total_amount'] = summary['total_amount'].apply(lambda x: f"{x:,.2f}")
        
        # Format quantity with 2 decimal places
        summary[qty_col] = summary[qty_col].apply(lambda x: f"{x:,.2f}")
        
        # Rename columns
        columns_rename = {
            'id': 'ID',
            'po_number': 'PO Number',
            'pt_code': 'PT Code',
            'product_name': 'Product',
            'buying_unit_cost': 'Unit Cost',
            qty_col: 'Quantity',
            'arrival_note_number': 'AN Numbers',
            'line_amount': 'Subtotal',
            'vat_display': 'VAT',
            'vat_amount': 'VAT Amount',
            'total_amount': 'Total'
        }
        
        if 'adjusted' in summary.columns:
            columns_rename['adjusted'] = 'Adj'
        
        summary.rename(columns=columns_rename, inplace=True)
        
        # Reorder columns with ID first
        display_cols = ['ID', 'PO Number', 'PT Code', 'Product', 'Unit Cost', 
                       'Quantity', 'AN Numbers', 'Subtotal', 'VAT', 
                       'VAT Amount', 'Total']
        
        if 'Adj' in summary.columns:
            display_cols.append('Adj')
        
        return summary[display_cols]
    
    @staticmethod
    def validate_invoice_with_po_level(df: pd.DataFrame) -> Tuple[Dict, Dict]:
        """
        Enhanced validation with PO level checks (used at line 572)
        
        Returns:
            (validation_results, messages)
        """
        validation_results = {
            'can_invoice': True,
            'has_warnings': False,
            'has_risks': False
        }
        
        messages = {
            'error': None,
            'warnings': [],
            'risks': []
        }
        
        # Basic validation
        if df.empty:
            validation_results['can_invoice'] = False
            messages['error'] = "No items selected"
            return validation_results, messages
        
        # Check single vendor
        vendors = df['vendor_code'].unique()
        if len(vendors) > 1:
            validation_results['can_invoice'] = False
            messages['error'] = f"Multiple vendors selected: {', '.join(vendors)}"
            return validation_results, messages
        
        # Check single entity
        entities = df['legal_entity_code'].unique()
        if len(entities) > 1:
            validation_results['can_invoice'] = False
            messages['error'] = f"Multiple legal entities selected: {', '.join(entities)}"
            return validation_results, messages
        
        # Check vendor type consistency
        vendor_types = df['vendor_type'].unique()
        if len(vendor_types) > 1:
            validation_results['can_invoice'] = False
            messages['error'] = "Cannot mix Internal and External vendors"
            return validation_results, messages
        
        # PO Level Validation
        if 'product_purchase_order_id' in df.columns:
            po_line_ids = df['product_purchase_order_id'].unique().tolist()
            try:
                po_summary = get_po_line_summary(po_line_ids)
                
                if not po_summary.empty:
                    for po_id in po_line_ids:
                        po_data = po_summary[po_summary['product_purchase_order_id'] == po_id]
                        if po_data.empty:
                            continue
                        
                        po_row = po_data.iloc[0]
                        selected_for_po = df[df['product_purchase_order_id'] == po_id]
                        total_selected = selected_for_po['uninvoiced_quantity'].sum()
                        
                        # Check if selection would exceed PO quantity
                        remaining_qty = po_row.get('po_remaining_qty', float('inf'))
                        if total_selected > remaining_qty * 1.1:  # Allow 10% tolerance
                            validation_results['can_invoice'] = False
                            messages['error'] = f"Selection exceeds PO remaining quantity for {po_row['po_number']}-{po_row['pt_code']}"
                            return validation_results, messages
                        
                        # Warnings for legacy invoices
                        if po_row.get('legacy_invoice_qty', 0) > 0:
                            validation_results['has_warnings'] = True
                            messages['warnings'].append(
                                f"PO {po_row['po_number']}-{po_row['pt_code']} has {po_row['legacy_invoice_qty']:.0f} units from legacy invoices"
                            )
                        
                        # Warning if close to limit
                        if total_selected > remaining_qty * 0.9:
                            validation_results['has_warnings'] = True
                            messages['warnings'].append(
                                f"PO {po_row['po_number']}-{po_row['pt_code']} will be >90% invoiced"
                            )
            except Exception as e:
                logger.error(f"Error validating PO levels: {e}")
                validation_results['has_warnings'] = True
                messages['warnings'].append("Could not validate PO level constraints")
        
        # Check payment terms
        payment_terms = df['payment_term'].dropna().unique()
        if len(payment_terms) > 1:
            validation_results['has_warnings'] = True
            messages['warnings'].append(
                f"Multiple payment terms: {', '.join(payment_terms)}. Most common will be used."
            )
        
        # Check VAT rates
        if 'vat_percent' in df.columns:
            vat_rates = df['vat_percent'].unique()
            if len(vat_rates) > 1:
                validation_results['has_warnings'] = True
                messages['warnings'].append(
                    f"Multiple VAT rates: {', '.join([f'{v:.0f}%' for v in vat_rates])}"
                )
        
        # Check for problematic flags
        if 'po_line_is_over_delivered' in df.columns:
            over_delivered = df[df['po_line_is_over_delivered'] == 'Y']
            if not over_delivered.empty:
                validation_results['has_warnings'] = True
                messages['warnings'].append(
                    f"{len(over_delivered)} PO line(s) have over-delivery"
                )
        
        if 'po_line_is_over_invoiced' in df.columns:
            over_invoiced = df[df['po_line_is_over_invoiced'] == 'Y']
            if not over_invoiced.empty:
                validation_results['has_warnings'] = True
                messages['warnings'].append(
                    f"{len(over_invoiced)} PO line(s) have over-invoicing"
                )
        
        if 'has_legacy_invoices' in df.columns:
            has_legacy = df[df['has_legacy_invoices'] == 'Y']
            if not has_legacy.empty:
                validation_results['has_warnings'] = True
                messages['warnings'].append(
                    f"{len(has_legacy)} PO line(s) have legacy invoices"
                )
        
        return validation_results, messages
    
    @staticmethod
    def get_payment_terms_dict() -> Dict:
        """Get available payment terms as dictionary (used at line 950)"""
        try:
            df = get_payment_terms()
            # Convert to dictionary with ID as key
            return {
                row['id']: {
                    'name': row['name'],
                    'days': row['days'],
                    'description': row.get('description', '')
                }
                for _, row in df.iterrows()
            }
        except Exception as e:
            logger.error(f"Error getting payment terms dict: {e}")
            # Return default if error
            return {
                1: {'name': 'Net 30', 'days': 30, 'description': 'Payment due in 30 days'},
                2: {'name': 'Net 60', 'days': 60, 'description': 'Payment due in 60 days'},
                3: {'name': 'Net 90', 'days': 90, 'description': 'Payment due in 90 days'},
                4: {'name': 'COD', 'days': 0, 'description': 'Cash on delivery'}
            }