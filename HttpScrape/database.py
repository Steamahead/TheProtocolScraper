import pymssql
from typing import Optional, List
import os
import logging
from datetime import datetime
# --- CORRECTED RELATIVE IMPORT ---
from .models import JobListing, Skill

def _truncate(value: Optional[str], length: int) -> Optional[str]:
    """Helper to ensure string does not exceed the given length."""
    if isinstance(value, str) and len(value) > length:
        return value[:length]
    return value

def get_sql_connection():
    """Get SQL connection using SQL authentication"""
    try:
        server = os.environ.get('DB_SERVER')
        database = os.environ.get('DB_NAME')
        username = os.environ.get('DB_UID')
        password = os.environ.get('DB_PWD')
        connection = pymssql.connect(
            server=server, user=username, password=password, database=database,
            timeout=30, appname="JobMinerApp"
        )
        logging.info("SQL connection successful")
        return connection
    except Exception as e:
        logging.error(f"SQL connection error: {str(e)}", exc_info=True)
        raise

def create_tables_if_not_exist():
    """Create the database tables if they don't exist"""
    with get_sql_connection() as connection:
        with connection.cursor() as cursor:
            jobs_table_sql = """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'JobListings')
            BEGIN
                CREATE TABLE JobListings (
                    ID INT IDENTITY(1,1) PRIMARY KEY, JobID NVARCHAR(100) NOT NULL, Source NVARCHAR(50) NOT NULL,
                    Title NVARCHAR(255) NOT NULL, Company NVARCHAR(255) NOT NULL, Link NVARCHAR(500) NOT NULL,
                    SalaryMin INT NULL, SalaryMax INT NULL, Location NVARCHAR(255) NULL,
                    OperatingMode NVARCHAR(255) NULL, WorkType NVARCHAR(50) NULL, ExperienceLevel NVARCHAR(MAX) NULL,
                    EmploymentType NVARCHAR(50) NULL, YearsOfExperience INT NULL, ScrapeDate DATETIME NOT NULL,
                    ListingStatus NVARCHAR(20) NOT NULL, CONSTRAINT UC_JobListing UNIQUE (JobID, Source)
                )
            END
            """
            cursor.execute(jobs_table_sql)
            
            # --- Added Skills Table ---
            skills_table_sql = """
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Skills')
            BEGIN
                CREATE TABLE Skills (
                    ID INT IDENTITY(1,1) PRIMARY KEY,
                    JobID NVARCHAR(100) NOT NULL,
                    ShortID INT NOT NULL,
                    Source NVARCHAR(50) NOT NULL,
                    SkillName NVARCHAR(150) NOT NULL,
                    SkillCategory NVARCHAR(50) NULL,
                    CONSTRAINT UC_JobSkill UNIQUE (JobID, Source, SkillName)
                )
            END
            """
            cursor.execute(skills_table_sql)
            connection.commit()
    logging.info("Database tables created or already exist")

def insert_job_listing(job: JobListing) -> Optional[int]:
    """Insert a job listing into the database and return its ID"""
    try:
        with get_sql_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT ID FROM JobListings WHERE JobID=%s AND Source=%s", (job.job_id, job.source))
                row = cursor.fetchone()
                if row:
                    logging.info(f"Job already exists with ID: {row[0]}")
                    job.short_id = row[0] # Assign existing ID
                    return row[0]
                
                insert_sql = """
                INSERT INTO JobListings (
                    JobID, Source, Title, Company, Link, SalaryMin, SalaryMax, Location,
                    OperatingMode, WorkType, ExperienceLevel, EmploymentType, YearsOfExperience,
                    ScrapeDate, ListingStatus
                ) OUTPUT INSERTED.ID VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                params = (
                    _truncate(job.job_id, 100), _truncate(job.source, 50), _truncate(job.title, 255),
                    _truncate(job.company, 255), _truncate(job.link, 500), job.salary_min, job.salary_max,
                    _truncate(job.location, 255), _truncate(job.operating_mode, 255), _truncate(job.work_type, 50),
                    job.experience_level, job.employment_type, job.years_of_experience,
                    job.scrape_date, _truncate(job.listing_status, 20),
                )
                cursor.execute(insert_sql, params)
                new_id = cursor.fetchone()[0]
                connection.commit()
                logging.info(f"Inserted new job with ID: {new_id}")
                job.short_id = new_id # Assign new ID
                return new_id
    except Exception as e:
        logging.error(f"Error inserting job listing '{job.title}': {e}", exc_info=True)
        return None

def insert_skill(skill: Skill) -> bool:
    """Insert a skill into the database"""
    try:
        with get_sql_connection() as connection:
            with connection.cursor() as cursor:
                # Check if skill already exists for this job
                check_query = """
                SELECT ID FROM Skills
                WHERE JobID = %s AND Source = %s AND SkillName = %s
                """
                cursor.execute(check_query, (skill.job_id, skill.source, skill.skill_name))
                if cursor.fetchone():
                    logging.info(f"Skill '{skill.skill_name}' already exists for job {skill.job_id}")
                    return True # Skill already exists, no need to insert
                
                # Insert new skill
                insert_query = """
                INSERT INTO Skills (JobID, ShortID, Source, SkillName, SkillCategory)
                VALUES (%s, %s, %s, %s, %s)
                """
                params = (
                    _truncate(skill.job_id, 100),
                    skill.short_id,
                    _truncate(skill.source, 50),
                    _truncate(skill.skill_name, 150),
                    _truncate(skill.skill_category, 50)
                )
                cursor.execute(insert_query, params)
                connection.commit()
                logging.info(f"Inserted skill: {skill.skill_name} for job {skill.job_id}")
                return True
    except Exception as e:
        logging.error(f"Error inserting skill '{skill.skill_name}' for job '{skill.job_id}': {e}", exc_info=True)
        return False
