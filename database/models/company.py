from sqlalchemy import Column, Integer, String, Boolean, Date, Text
from .base import Base, TimestampMixin, TallyCDCMixin


class Company(Base, TimestampMixin, TallyCDCMixin):
    """Tally Company master data"""
    __tablename__ = 'companies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Basic Information
    name = Column(String(255), nullable=False, index=True, comment="Company name")
    formal_name = Column(String(255), nullable=True, comment="Formal company name")
    company_number = Column(String(50), nullable=True)
    
    # Date Information
    starting_from = Column(Date, nullable=True, comment="Books starting date")
    books_from = Column(Date, nullable=True, comment="Financial year start")
    audited_upto = Column(Date, nullable=True, comment="Audited up to date")
    
    # Contact Information
    email = Column(String(255), nullable=True)
    phone_number = Column(String(50), nullable=True)
    fax_number = Column(String(50), nullable=True)
    website = Column(String(255), nullable=True)
    contact_person = Column(String(255), nullable=True)
    contact_number = Column(String(50), nullable=True)
    
    # Address Information
    country = Column(String(100), nullable=True)
    state = Column(String(100), nullable=True)
    pincode = Column(String(20), nullable=True)
    address = Column(Text, nullable=True)
    
    # Tax Registration
    gstin = Column(String(50), nullable=True, comment="GST registration number")
    gst_registration_type = Column(String(100), nullable=True)
    pan = Column(String(20), nullable=True, comment="PAN number")
    tan = Column(String(20), nullable=True, comment="TAN number")
    vat_tin = Column(String(50), nullable=True)
    sales_tax_number = Column(String(50), nullable=True)
    service_tax_reg = Column(String(50), nullable=True)
    excise_reg = Column(String(50), nullable=True)
    cin = Column(String(50), nullable=True, comment="Corporate Identification Number")
    export_import_code = Column(String(50), nullable=True)
    pf_code = Column(String(50), nullable=True, comment="PF code")
    esi_code = Column(String(50), nullable=True, comment="ESI code")
    
    # Feature Flags
    is_gst_on = Column(Boolean, default=False)
    is_accounting_on = Column(Boolean, default=False)
    is_inventory_on = Column(Boolean, default=False)
    is_integrated = Column(Boolean, default=False)
    is_billwise_on = Column(Boolean, default=False)
    is_cost_centres_on = Column(Boolean, default=False)
    is_security_on = Column(Boolean, default=False)
    is_payroll_on = Column(Boolean, default=False)
    is_vat_on = Column(Boolean, default=False)
    is_ebanking_on = Column(Boolean, default=False)
    
    # GST Information
    gst_applicable_date = Column(Date, nullable=True)
    type_of_supply = Column(String(50), nullable=True)
    
    # VAT Information
    vat_dealer_type = Column(String(50), nullable=True)
    show_bank_details = Column(String(50), nullable=True)

    __table_args__ = (
        {'comment': 'Tally company master data with CDC tracking'},
    )

    def __repr__(self):
        return f"<Company(name={self.name}, guid={self.guid})>"