from sqlalchemy import Column, Integer, String, Numeric, Date, Text, Index, Enum as SQLEnum
from .base import Base, TimestampMixin, TallyCDCMixin, CompanyMixin
import enum


class VoucherType(enum.Enum):
    """Voucher type enumeration"""
    SALES = "Sales"
    PURCHASE = "Purchase"
    RECEIPT = "Receipt"
    PAYMENT = "Payment"
    JOURNAL = "Journal"
    CONTRA = "Contra"
    CREDIT_NOTE = "Credit Note"
    DEBIT_NOTE = "Debit Note"


class SalesVoucher(Base, TimestampMixin, TallyCDCMixin, CompanyMixin):
    """Sales Voucher with line items"""
    __tablename__ = 'sales_vouchers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Voucher Header
    voucher_type = Column(String(50), default="Sales", nullable=False)
    voucher_number = Column(String(100), nullable=False, index=True)
    voucher_date = Column(Date, nullable=False, index=True)
    reference = Column(String(100), nullable=True, comment="Reference number")
    
    # Party Information
    party_name = Column(String(255), nullable=True, index=True)
    party_gstin = Column(String(50), nullable=True)
    place_of_supply = Column(String(100), nullable=True)
    
    # Item Details (denormalized for each line item)
    item_name = Column(String(255), nullable=True)
    quantity = Column(Numeric(18, 4), default=0.0000)
    rate = Column(Numeric(18, 4), default=0.0000)
    amount = Column(Numeric(18, 2), default=0.00)
    discount = Column(Numeric(18, 2), default=0.00)
    
    # Tax Breakdown
    cgst_amt = Column(Numeric(18, 2), default=0.00, comment="CGST amount")
    sgst_amt = Column(Numeric(18, 2), default=0.00, comment="SGST amount")
    igst_amt = Column(Numeric(18, 2), default=0.00, comment="IGST amount")
    
    # Additional Charges
    freight_amt = Column(Numeric(18, 2), default=0.00)
    dca_amt = Column(Numeric(18, 2), default=0.00, comment="Delivery charges")
    cf_amt = Column(Numeric(18, 2), default=0.00, comment="Clearing & Forwarding")
    other_amt = Column(Numeric(18, 2), default=0.00)
    
    # Currency
    currency = Column(String(10), default='INR', nullable=False)
    currency_name = Column(String(100), default='Indian Rupee')
    
    # Additional Fields
    narration = Column(Text, nullable=True)
    entered_by = Column(String(100), nullable=True)
    basic_ship_doc_no = Column(String(100), nullable=True, comment="Shipping document number")
    
    # CDC Status
    change_status = Column(String(20), nullable=True, comment="New/Modified/Unknown")
    
    __table_args__ = (
        Index('idx_sales_company_date', 'company_name', 'voucher_date'),
        Index('idx_sales_party', 'party_name'),
        Index('idx_sales_voucher_no', 'voucher_number'),
        {'comment': 'Sales vouchers with line items (denormalized)'},
    )

    def __repr__(self):
        return f"<SalesVoucher(no={self.voucher_number}, date={self.voucher_date}, party={self.party_name})>"


class PurchaseVoucher(Base, TimestampMixin, TallyCDCMixin, CompanyMixin):
    """Purchase Voucher with line items"""
    __tablename__ = 'purchase_vouchers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Voucher Header
    voucher_type = Column(String(50), default="Purchase", nullable=False)
    voucher_number = Column(String(100), nullable=False, index=True)
    voucher_date = Column(Date, nullable=False, index=True)
    reference = Column(String(100), nullable=True)
    
    # Party Information
    party_name = Column(String(255), nullable=True, index=True)
    party_gstin = Column(String(50), nullable=True)
    place_of_supply = Column(String(100), nullable=True)
    
    # Item Details (denormalized)
    item_name = Column(String(255), nullable=True)
    quantity = Column(Numeric(18, 4), default=0.0000)
    rate = Column(Numeric(18, 4), default=0.0000)
    amount = Column(Numeric(18, 2), default=0.00)
    discount = Column(Numeric(18, 2), default=0.00)
    
    # Tax Breakdown
    cgst_amt = Column(Numeric(18, 2), default=0.00)
    sgst_amt = Column(Numeric(18, 2), default=0.00)
    igst_amt = Column(Numeric(18, 2), default=0.00)
    
    # Additional Charges
    freight_amt = Column(Numeric(18, 2), default=0.00)
    dca_amt = Column(Numeric(18, 2), default=0.00)
    cf_amt = Column(Numeric(18, 2), default=0.00)
    other_amt = Column(Numeric(18, 2), default=0.00)
    
    # Currency
    currency = Column(String(10), default='INR', nullable=False)
    currency_name = Column(String(100), default='Indian Rupee')
    
    # Additional Fields
    narration = Column(Text, nullable=True)
    entered_by = Column(String(100), nullable=True)
    basic_ship_doc_no = Column(String(100), nullable=True)
    
    # CDC Status
    change_status = Column(String(20), nullable=True)
    
    __table_args__ = (
        Index('idx_purchase_company_date', 'company_name', 'voucher_date'),
        Index('idx_purchase_party', 'party_name'),
        Index('idx_purchase_voucher_no', 'voucher_number'),
        {'comment': 'Purchase vouchers with line items (denormalized)'},
    )

    def __repr__(self):
        return f"<PurchaseVoucher(no={self.voucher_number}, date={self.voucher_date}, party={self.party_name})>"


class ReceiptVoucher(Base, TimestampMixin, TallyCDCMixin, CompanyMixin):
    """Receipt Voucher"""
    __tablename__ = 'receipt_vouchers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    voucher_type = Column(String(50), default="Receipt", nullable=False)
    voucher_number = Column(String(100), nullable=False, index=True)
    voucher_date = Column(Date, nullable=False, index=True)
    reference = Column(String(100), nullable=True)
    
    party_name = Column(String(255), nullable=True, index=True)
    ledger_name = Column(String(255), nullable=True)
    amount = Column(Numeric(18, 2), default=0.00)
    
    narration = Column(Text, nullable=True)
    instrument_number = Column(String(100), nullable=True, comment="Cheque/instrument number")
    instrument_date = Column(Date, nullable=True)
    bank_name = Column(String(255), nullable=True)
    
    change_status = Column(String(20), nullable=True)
    
    __table_args__ = (
        Index('idx_receipt_company_date', 'company_name', 'voucher_date'),
        {'comment': 'Receipt vouchers'},
    )


class PaymentVoucher(Base, TimestampMixin, TallyCDCMixin, CompanyMixin):
    """Payment Voucher"""
    __tablename__ = 'payment_vouchers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    voucher_type = Column(String(50), default="Payment", nullable=False)
    voucher_number = Column(String(100), nullable=False, index=True)
    voucher_date = Column(Date, nullable=False, index=True)
    reference = Column(String(100), nullable=True)
    
    party_name = Column(String(255), nullable=True, index=True)
    ledger_name = Column(String(255), nullable=True)
    amount = Column(Numeric(18, 2), default=0.00)
    
    narration = Column(Text, nullable=True)
    instrument_number = Column(String(100), nullable=True)
    instrument_date = Column(Date, nullable=True)
    bank_name = Column(String(255), nullable=True)
    
    change_status = Column(String(20), nullable=True)
    
    __table_args__ = (
        Index('idx_payment_company_date', 'company_name', 'voucher_date'),
        {'comment': 'Payment vouchers'},
    )


class JournalVoucher(Base, TimestampMixin, TallyCDCMixin, CompanyMixin):
    """Journal Voucher"""
    __tablename__ = 'journal_vouchers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    voucher_type = Column(String(50), default="Journal", nullable=False)
    voucher_number = Column(String(100), nullable=False, index=True)
    voucher_date = Column(Date, nullable=False, index=True)
    reference = Column(String(100), nullable=True)
    
    ledger_name = Column(String(255), nullable=True)
    amount = Column(Numeric(18, 2), default=0.00)
    
    narration = Column(Text, nullable=True)
    change_status = Column(String(20), nullable=True)
    
    __table_args__ = (
        Index('idx_journal_company_date', 'company_name', 'voucher_date'),
        {'comment': 'Journal vouchers'},
    )


class ContraVoucher(Base, TimestampMixin, TallyCDCMixin, CompanyMixin):
    """Contra Voucher"""
    __tablename__ = 'contra_vouchers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    voucher_type = Column(String(50), default="Contra", nullable=False)
    voucher_number = Column(String(100), nullable=False, index=True)
    voucher_date = Column(Date, nullable=False, index=True)
    reference = Column(String(100), nullable=True)
    
    ledger_name = Column(String(255), nullable=True)
    amount = Column(Numeric(18, 2), default=0.00)
    
    narration = Column(Text, nullable=True)
    change_status = Column(String(20), nullable=True)
    
    __table_args__ = (
        Index('idx_contra_company_date', 'company_name', 'voucher_date'),
        {'comment': 'Contra vouchers'},
    )