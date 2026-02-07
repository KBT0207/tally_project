from sqlalchemy import Column, String, Integer, Float, DateTime, Boolean, Text, Index
from datetime import datetime
from database.models.base import tally_base


class Ledger(tally_base):
    __tablename__ = 'ledgers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    company_name = Column(String(200), nullable=False, index=True)
    
    # Ledger identification
    guid = Column(String(50), nullable=False, index=True)
    name = Column(String(200), nullable=False, index=True)
    
    # Ledger details
    parent = Column(String(200))
    alias = Column(String(200))

    # Contact information
    mailing_name = Column(String(200))
    address = Column(Text)
    state = Column(String(100))
    country = Column(String(100))
    pincode = Column(String(10))

    # Tax information
    gstin = Column(String(15))
    pan = Column(String(10))

    # Contact details
    mobile = Column(String(20))
    email = Column(String(100))

    # Financial information
    opening_balance = Column(Float, default=0.0)

    # Status and timestamps
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_synced_at = Column(DateTime, default=datetime.utcnow)

    # Raw data backup
    raw_data = Column(Text)

    # Composite unique constraint: GUID is unique within a company
    # This allows same GUID in different companies (if needed)
    __table_args__ = (
        Index('idx_company_guid', 'company_name', 'guid', unique=True),
    )

    def __repr__(self):
        return f"<Ledger(company='{self.company_name}', name='{self.name}', guid='{self.guid}')>"


class SyncLog(tally_base):
    __tablename__ = 'sync_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Company identification
    company_name = Column(String(200), nullable=False, index=True)
    
    # Sync timing
    sync_started_at = Column(DateTime, default=datetime.utcnow)
    sync_completed_at = Column(DateTime)

    # Statistics
    total_in_tally = Column(Integer, default=0)
    total_in_db = Column(Integer, default=0)
    new_added = Column(Integer, default=0)
    modified = Column(Integer, default=0)
    deleted = Column(Integer, default=0)

    # Status
    status = Column(String(20))  # 'in_progress', 'completed', 'failed'
    error_message = Column(Text)
    details = Column(Text)

    def __repr__(self):
        return f"<SyncLog(company='{self.company_name}', status='{self.status}')>"


class ChangeLog(tally_base):
    __tablename__ = 'change_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Company and ledger identification
    company_name = Column(String(200), nullable=False, index=True)
    ledger_guid = Column(String(50), nullable=False, index=True)
    ledger_name = Column(String(200))

    # Change information
    change_type = Column(String(20))  # 'created', 'modified', 'deleted'
    change_timestamp = Column(DateTime, default=datetime.utcnow)

    # Change details
    old_values = Column(Text)
    new_values = Column(Text)
    changed_fields = Column(Text)

    # Reference to sync log
    sync_log_id = Column(Integer)

    def __repr__(self):
        return f"<ChangeLog(company='{self.company_name}', ledger='{self.ledger_name}', type='{self.change_type}')>"