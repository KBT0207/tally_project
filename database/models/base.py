from sqlalchemy import Column, Integer, String, DateTime, Boolean, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime

Base = declarative_base()


class TimestampMixin:
    """Mixin for adding timestamp fields to models"""
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    synced_at = Column(DateTime, default=func.now(), nullable=False)


class TallyCDCMixin:
    """Mixin for Tally CDC tracking fields"""
    guid = Column(String(255), unique=True, nullable=False, index=True, comment="Tally GUID - Unique identifier")
    alter_id = Column(Integer, nullable=True, index=True, comment="Tally ALTER ID - Version number")
    master_id = Column(Integer, nullable=True, comment="Tally MASTER ID - Creation version")
    last_modified = Column(Date, nullable=True, index=True, comment="Last modification date in Tally")
    is_deleted = Column(Boolean, default=False, nullable=False, index=True, comment="Soft delete flag")
    deleted_at = Column(DateTime, nullable=True, comment="When record was marked as deleted")


class CompanyMixin:
    """Mixin for company association"""
    company_name = Column(String(255), nullable=False, index=True, comment="Tally company name")