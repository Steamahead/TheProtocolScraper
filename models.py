from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

@dataclass
class JobListing:
    job_id: str                   # Unique ID or URL of the job
    source: str                   # e.g., 'theprotocol.it'
    title: str
    company: str
    link: str                     # Direct URL to the job offer
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    location: Optional[str] = None
    operating_mode: Optional[str] = None
    work_type: Optional[str] = None
    experience_level: Optional[str] = None
    employment_type: Optional[str] = None
    years_of_experience: Optional[int] = None
    scrape_date: datetime = field(default_factory=datetime.utcnow)
    listing_status: str = 'Active'  # e.g., 'Active', 'Closed'
    short_id: Optional[int] = None  # Populated after DB insert

@dataclass
class Skill:
    job_id: str
    source: str
    skill_name: str
    skill_category: Optional[str] = None
    short_id: Optional[int] = None
