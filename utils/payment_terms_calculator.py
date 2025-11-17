"""
Enhanced Payment Terms Calculator
Handles all payment term types from database with proper categorization
"""
import re
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Tuple, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class PaymentTermCategory(Enum):
    """Categories of payment terms"""
    NET_DAYS = "net_days"              # NET X DAYS BY TT
    AMS_DAYS = "ams_days"              # After Month Start - X DAYS
    ADVANCE = "advance"                # TT IN ADVANCE, COD
    AFTER_EVENT = "after_event"        # AFTER DELIVERY, AFTER INVOICE
    SPLIT_PAYMENT = "split_payment"    # 50% DP, 50% NET 30
    SPECIAL_DATE = "special_date"      # 25th of month, EOM
    OTHER = "other"                    # Custom terms


class PaymentTermParser:
    """Parse and calculate due dates for all payment term types"""
    
    @staticmethod
    def categorize_payment_term(term_name: str, description: str = "") -> PaymentTermCategory:
        """
        Categorize payment term based on name and description
        
        Args:
            term_name: Payment term name from database
            description: Payment term description
            
        Returns:
            PaymentTermCategory enum
        """
        if pd.isna(term_name):
            return PaymentTermCategory.OTHER
        
        term_upper = str(term_name).upper()
        
        # Priority order matters!
        
        # 1. Split payments (has percentage or colon)
        if '%' in term_name or (':' in term_name and term_name.count(':') >= 1):
            return PaymentTermCategory.SPLIT_PAYMENT
        
        # 2. NET DAYS (most common)
        if 'NET' in term_upper and 'DAYS' in term_upper:
            return PaymentTermCategory.NET_DAYS
        
        # 3. AMS (After Month Start)
        if 'AMS' in term_upper:
            return PaymentTermCategory.AMS_DAYS
        
        # 4. Advance payment
        if any(kw in term_upper for kw in ['ADVANCE', 'COD', 'CIA', 'PREPAID']):
            return PaymentTermCategory.ADVANCE
        
        # 5. Special dates
        if any(kw in term_upper for kw in ['25TH', 'EOM', 'MOA', 'END OF MONTH']):
            return PaymentTermCategory.SPECIAL_DATE
        
        # 6. Event-based
        if any(kw in term_upper for kw in ['AFTER', 'BEFORE', 'UPON']):
            return PaymentTermCategory.AFTER_EVENT
        
        # 7. Other
        return PaymentTermCategory.OTHER
    
    @staticmethod
    def extract_days_from_net_term(term_name: str) -> Optional[int]:
        """
        Extract number of days from NET X DAYS terms
        
        Examples:
            "NET 60 DAYS BY TT" -> 60
            "NET 30 DAYS" -> 30
            "Net 5 days by TT" -> 5
        """
        if pd.isna(term_name):
            return None
        
        term_upper = str(term_name).upper()
        
        # Pattern: NET followed by number and DAYS
        pattern = r'NET\s+(\d+)\s*DAYS?'
        match = re.search(pattern, term_upper)
        
        if match:
            return int(match.group(1))
        
        return None
    
    @staticmethod
    def extract_days_from_ams_term(term_name: str) -> Optional[int]:
        """
        Extract number of days from AMS X DAYS terms
        
        Examples:
            "AMS 60 DAYS BY TT" -> 60
            "AMS 90 DAYS" -> 90
        """
        if pd.isna(term_name):
            return None
        
        term_upper = str(term_name).upper()
        
        # Pattern: AMS followed by number and optional DAYS
        pattern = r'AMS\s+(\d+)\s*DAYS?'
        match = re.search(pattern, term_upper)
        
        if match:
            return int(match.group(1))
        
        return None
    
    @staticmethod
    def calculate_ams_due_date(invoice_date: date, days: int) -> date:
        """
        Calculate AMS (After Month Start) due date
        
        AMS means: X days from the FIRST day of the NEXT month
        
        Example:
            Invoice date: 2025-01-17
            AMS 60 DAYS:
            - First day of next month: 2025-02-01
            - Due date: 2025-02-01 + 60 days = 2025-04-02
        """
        # Get first day of next month
        if invoice_date.month == 12:
            first_of_next_month = date(invoice_date.year + 1, 1, 1)
        else:
            first_of_next_month = date(invoice_date.year, invoice_date.month + 1, 1)
        
        # Add days
        due_date = first_of_next_month + timedelta(days=days)
        return due_date
    
    @staticmethod
    def extract_final_payment_days(term_name: str, description: str = "") -> Optional[int]:
        """
        For split payment terms, extract the FINAL payment days if specified
        
        Examples:
            "50% DP, 50% NET 30 DAYS" -> 30
            "50% IN ADVANCE, 50% NET 15 DAYS" -> 15
            "30:40:30 Net 30" -> 30
        """
        text = f"{term_name} {description}".upper()
        
        # Look for NET X pattern in split payment terms
        pattern = r'NET\s+(\d+)\s*DAYS?'
        matches = re.findall(pattern, text)
        
        if matches:
            # Return the last (final) NET days found
            return int(matches[-1])
        
        return None
    
    @staticmethod
    def calculate_due_date(
        term_name: str,
        invoice_date: date,
        description: str = ""
    ) -> Tuple[Optional[date], str, bool]:
        """
        Calculate due date for any payment term
        
        Args:
            term_name: Payment term name
            invoice_date: Invoice date
            description: Payment term description (optional)
            
        Returns:
            Tuple of (due_date, explanation, needs_manual_review)
            - due_date: Calculated due date or None
            - explanation: Human-readable explanation
            - needs_manual_review: True if user should review/edit
        """
        if pd.isna(term_name):
            return None, "Payment term not specified", True
        
        # Categorize the term
        category = PaymentTermParser.categorize_payment_term(term_name, description)
        
        # Calculate based on category
        if category == PaymentTermCategory.NET_DAYS:
            days = PaymentTermParser.extract_days_from_net_term(term_name)
            if days is not None:
                due_date = invoice_date + timedelta(days=days)
                return due_date, f"Invoice date + {days} days", False
            else:
                return None, f"Could not parse NET days from: {term_name}", True
        
        elif category == PaymentTermCategory.AMS_DAYS:
            days = PaymentTermParser.extract_days_from_ams_term(term_name)
            if days is not None:
                due_date = PaymentTermParser.calculate_ams_due_date(invoice_date, days)
                return due_date, f"First day of next month + {days} days", False
            else:
                return None, f"Could not parse AMS days from: {term_name}", True
        
        elif category == PaymentTermCategory.ADVANCE:
            # Advance payment = due immediately (0 days)
            return invoice_date, "Payment in advance (due immediately)", False
        
        elif category == PaymentTermCategory.SPLIT_PAYMENT:
            # Try to extract final payment days
            final_days = PaymentTermParser.extract_final_payment_days(term_name, description)
            if final_days is not None:
                due_date = invoice_date + timedelta(days=final_days)
                return (
                    due_date,
                    f"⚠️ Split payment term - Final payment: Invoice date + {final_days} days",
                    True  # Always needs review for split payments
                )
            else:
                return (
                    invoice_date + timedelta(days=30),
                    f"⚠️ Split payment term - Please review payment milestones",
                    True
                )
        
        elif category == PaymentTermCategory.SPECIAL_DATE:
            # Special date logic (25th, EOM, etc.)
            if '25TH' in term_name.upper():
                # Payment on 25th of current or next month
                if invoice_date.day <= 25:
                    due_date = date(invoice_date.year, invoice_date.month, 25)
                else:
                    # Next month 25th
                    if invoice_date.month == 12:
                        due_date = date(invoice_date.year + 1, 1, 25)
                    else:
                        due_date = date(invoice_date.year, invoice_date.month + 1, 25)
                return due_date, f"Payment due on 25th of month", True
            
            elif 'EOM' in term_name.upper():
                # Extract days after EOM
                match = re.search(r'EOM\s*(\d+)', term_name.upper())
                if match:
                    days = int(match.group(1))
                    # Last day of current month + days
                    if invoice_date.month == 12:
                        last_day = date(invoice_date.year, 12, 31)
                    else:
                        next_month = date(invoice_date.year, invoice_date.month + 1, 1)
                        last_day = next_month - timedelta(days=1)
                    due_date = last_day + timedelta(days=days)
                    return due_date, f"End of month + {days} days", True
            
            elif 'MOA' in term_name.upper():
                # MOA terms
                match = re.search(r'(\d+)', term_name)
                if match:
                    days = int(match.group(1))
                    due_date = invoice_date + timedelta(days=days)
                    return due_date, f"MOA: Invoice date + {days} days", True
            
            return None, f"Special date term - Please specify due date", True
        
        elif category == PaymentTermCategory.AFTER_EVENT:
            # Event-based (after delivery, etc.) - cannot auto-calculate
            return (
                invoice_date + timedelta(days=30),
                f"⚠️ Event-based payment ({term_name}) - Please specify due date",
                True
            )
        
        else:
            # OTHER - default to 30 days
            return (
                invoice_date + timedelta(days=30),
                f"⚠️ Custom payment term - Please review and adjust",
                True
            )


# Backward compatibility function
def calculate_days_from_term_name(term_name: str) -> int:
    """
    Legacy function for backward compatibility
    Returns number of days (default: 30)
    """
    if pd.isna(term_name):
        return 30
    
    category = PaymentTermParser.categorize_payment_term(term_name)
    
    if category == PaymentTermCategory.NET_DAYS:
        days = PaymentTermParser.extract_days_from_net_term(term_name)
        return days if days is not None else 30
    
    elif category == PaymentTermCategory.AMS_DAYS:
        days = PaymentTermParser.extract_days_from_ams_term(term_name)
        # AMS adds extra days, approximate as base days + 15 (half month)
        return (days + 15) if days is not None else 30
    
    elif category == PaymentTermCategory.ADVANCE:
        return 0
    
    elif category == PaymentTermCategory.SPLIT_PAYMENT:
        final_days = PaymentTermParser.extract_final_payment_days(term_name)
        return final_days if final_days is not None else 30
    
    else:
        return 30


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    test_cases = [
        # NET DAYS
        ("NET 60 DAYS BY TT", "2025-01-17"),
        ("NET 30 DAYS", "2025-01-17"),
        ("Net 5 days by TT", "2025-01-17"),
        
        # AMS DAYS
        ("AMS 60 DAYS BY TT", "2025-01-17"),
        ("AMS 90 DAYS", "2025-01-31"),
        
        # ADVANCE
        ("TT IN ADVANCE", "2025-01-17"),
        ("COD", "2025-01-17"),
        
        # SPLIT PAYMENT
        ("50% IN ADVANCE, 50% NET 30 DAYS", "2025-01-17"),
        ("50% DP, 50% Net 10", "2025-01-17"),
        
        # SPECIAL DATE
        ("TT on the 25th of every month", "2025-01-17"),
        ("TT on the 25th of every month", "2025-01-26"),
        ("EOM 90", "2025-01-17"),
        
        # AFTER EVENT
        ("TT AFTER DELIVERY", "2025-01-17"),
    ]
    
    print("=" * 100)
    print("PAYMENT TERMS CALCULATOR - TEST RESULTS")
    print("=" * 100)
    
    parser = PaymentTermParser()
    
    for term_name, inv_date_str in test_cases:
        inv_date = datetime.strptime(inv_date_str, "%Y-%m-%d").date()
        
        category = parser.categorize_payment_term(term_name)
        due_date, explanation, needs_review = parser.calculate_due_date(term_name, inv_date)
        
        print(f"\nTerm: {term_name}")
        print(f"  Category: {category.value}")
        print(f"  Invoice Date: {inv_date}")
        print(f"  Due Date: {due_date}")
        print(f"  Explanation: {explanation}")
        print(f"  Needs Manual Review: {needs_review}")
        print(f"  Days (legacy): {calculate_days_from_term_name(term_name)}")