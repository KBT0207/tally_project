from sqlalchemy import Column, Integer, String, DateTime, Date, Enum as SQLEnum
from sqlalchemy.sql import func
import enum
from .base import Base


class SyncMode(enum.Enum):
    """Enumeration for sync modes"""
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"


class SyncStatus(enum.Enum):
    """Enumeration for sync status"""
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    IN_PROGRESS = "IN_PROGRESS"


class SyncMetadata(Base):
    """
    Tracks sync state for each entity type and company
    This is the heart of the CDC system
    """
    __tablename__ = 'sync_metadata'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Entity identification
    entity_type = Column(String(50), nullable=False, index=True, 
                         comment="Type: sales_voucher, purchase_voucher, ledger, etc.")
    company_name = Column(String(255), nullable=False, index=True,
                          comment="Tally company name")
    
    # CDC tracking fields
    last_max_alterid = Column(Integer, default=0, nullable=False,
                             comment="Maximum ALTER_ID from last sync")
    last_sync_date = Column(Date, nullable=True,
                           comment="Last modification date synced")
    last_sync_timestamp = Column(DateTime, nullable=True,
                                comment="When last sync completed")
    
    # Sync metadata
    sync_mode = Column(SQLEnum(SyncMode), nullable=True,
                      comment="FULL or INCREMENTAL")
    sync_status = Column(SQLEnum(SyncStatus), default=SyncStatus.SUCCESS,
                        comment="Last sync status")
    
    # Statistics
    records_synced = Column(Integer, default=0,
                           comment="Records synced in last run")
    total_records = Column(Integer, default=0,
                          comment="Total records in table")
    records_inserted = Column(Integer, default=0,
                             comment="New records inserted")
    records_updated = Column(Integer, default=0,
                            comment="Existing records updated")
    records_deleted = Column(Integer, default=0,
                            comment="Records marked as deleted")
    
    # Error handling
    error_message = Column(String(1000), nullable=True,
                          comment="Error message if sync failed")
    retry_count = Column(Integer, default=0,
                        comment="Number of retry attempts")
    
    # Timestamps
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Unique constraint: one entry per entity type per company
    __table_args__ = (
        {'comment': 'Tracks CDC sync state for all Tally entities'},
    )

    def __repr__(self):
        return f"<SyncMetadata(entity={self.entity_type}, company={self.company_name}, last_alterid={self.last_max_alterid})>"