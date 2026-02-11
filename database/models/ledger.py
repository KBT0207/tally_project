from sqlalchemy import Column, Integer, String, Numeric, Boolean, Text, Index
from .base import Base, TimestampMixin, TallyCDCMixin, CompanyMixin


class Ledger(Base, TimestampMixin, TallyCDCMixin, CompanyMixin):
    """Tally Ledger master data"""
    __tablename__ = 'ledgers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Basic Information
    name = Column(String(255), nullable=False, index=True, comment="Ledger name")
    parent = Column(String(255), nullable=True, comment="Parent group")
    alias = Column(String(255), nullable=True, comment="Ledger alias")
    
    # Balance Information
    opening_balance = Column(Numeric(18, 2), default=0.00, comment="Opening balance")
    closing_balance = Column(Numeric(18, 2), default=0.00, comment="Closing balance")
    
    # Tax & Compliance
    pan = Column(String(20), nullable=True, comment="PAN number")
    gstin = Column(String(50), nullable=True, comment="GST registration number")
    gst_registration_type = Column(String(100), nullable=True)
    
    # Contact Information
    email = Column(String(255), nullable=True)
    phone_number = Column(String(50), nullable=True)
    mobile_number = Column(String(50), nullable=True)
    
    # Address
    address = Column(Text, nullable=True)
    country = Column(String(100), nullable=True)
    state = Column(String(100), nullable=True)
    pincode = Column(String(20), nullable=True)
    
    # Banking Information
    bank_account_number = Column(String(50), nullable=True)
    bank_ifsc = Column(String(20), nullable=True)
    bank_name = Column(String(255), nullable=True)
    bank_branch = Column(String(255), nullable=True)
    
    # Ledger Properties
    is_revenue = Column(Boolean, default=False)
    is_deemed_positive = Column(Boolean, default=False)
    is_cost_centres_on = Column(Boolean, default=False)
    
    # Description
    narration = Column(Text, nullable=True)
    
    __table_args__ = (
        Index('idx_ledger_company_name', 'company_name', 'name'),
        Index('idx_ledger_parent', 'parent'),
        {'comment': 'Tally ledger master data with CDC tracking'},
    )

    def __repr__(self):
        return f"<Ledger(name={self.name}, company={self.company_name}, guid={self.guid})>"