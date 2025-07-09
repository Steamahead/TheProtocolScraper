import pymssql
import os
import sqlite3
import logging
from typing import List, Optional
from datetime import datetime
from models import JobListing, Skill

def _truncate(value: Optional[str], length: int) -> Optional[str]:
    """Helper to ensure string does not exceed the given length."""
    if isinstance(value, str) and len(value) > length:
        return value[:length]
    return value

def get_sql_connection():
    """Get SQL connection using SQL authentication"""
    try:
        # Get connection details from environment variables
        server = os.environ.get('DB_SERVER')
        database = os.environ.get('DB_NAME')
        username = os.environ.get('DB_UID')
        password = os.environ.get('DB_PWD')

        if not all([server, database, username, password]):
            missing = [k for k, v in {
                'DB_SERVER': server, 'DB_NAME': database, 
                'DB_UID': username, 'DB_PWD': password
            }.items() if not v]
            raise Exception(f"Missing environment variables: {missing}")

        logging.info(f"Connecting to SQL server: {server}/{database} as {username}")

        # Connect using pymssql with SQL authentication
        connection = pymssql.connect(
            server=server,
            user=username,
            password=password,
            database=database,
            timeout=30,
            appname="JobMinerApp"
        )

        logging.info("SQL connection successful")
        return connection
    except Exception as e:
        logging.error(f"SQL connection error: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        raise

def create_tables_if_not_exist():
    """Create the database tables if they don't exist"""
    connection = None
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        # Create Jobs table
        jobs_table_sql = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'JobListings')
        BEGIN
            CREATE TABLE JobListings (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                JobID NVARCHAR(100) NOT NULL,
                Source NVARCHAR(50) NOT NULL,
                Title NVARCHAR(255) NOT NULL,
                Company NVARCHAR(255) NOT NULL,
                Link NVARCHAR(500) NOT NULL,
                SalaryMin INT NULL,
                SalaryMax INT NULL,
                Location NVARCHAR(255) NULL,
                OperatingMode NVARCHAR(255) NULL,
                WorkType NVARCHAR(50) NULL,
                ExperienceLevel NVARCHAR(MAX) NULL,
                EmploymentType NVARCHAR(50) NULL,
                YearsOfExperience INT NULL,
                ScrapeDate DATETIME NOT NULL,
                ListingStatus NVARCHAR(20) NOT NULL,
                CONSTRAINT UC_JobListing UNIQUE (JobID, Source)
            )
        END
        """
        
        # Create Skills table
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
        
        cursor.execute(jobs_table_sql)
        cursor.execute(skills_table_sql)
        connection.commit()
        logging.info("Database tables created or already exist")
        
    except Exception as e:
        logging.error(f"Error creating tables: {str(e)}")
        raise
    finally:
        if connection:
            connection.close()


def insert_job_listing(job: JobListing) -> Optional[int]:
    """Insert a job listing into the database and return its ID"""
    connection = None
    cursor = None
    
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()

        # Check for existing
        cursor.execute(
            "SELECT ID FROM JobListings WHERE JobID=%s AND Source=%s",
            (job.job_id, job.source)
        )
        row = cursor.fetchone()
        if row:
            job.short_id = row[0]
            logging.info(f"Job already exists with ID: {row[0]}")
            return row[0]

        # Insert new
        insert_sql = """
        INSERT INTO JobListings (
            JobID, Source, Title, Company, Link,
            SalaryMin, SalaryMax, Location,
            OperatingMode, WorkType, ExperienceLevel, EmploymentType,
            YearsOfExperience, ScrapeDate, ListingStatus
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        
        params = (
            _truncate(job.job_id, 100),
            _truncate(job.source, 50),
            _truncate(job.title, 255),
            _truncate(job.company, 255),
            _truncate(job.link, 500),
            job.salary_min,
            job.salary_max,
            _truncate(job.location, 255) if job.location else None,
            _truncate(job.operating_mode, 255) if job.operating_mode else None,
            _truncate(job.work_type, 50) if job.work_type else None,
            _truncate(job.experience_level, 100) if job.experience_level else None,
            _truncate(job.employment_type, 50) if job.employment_type else None,
            job.years_of_experience,
            job.scrape_date,
            _truncate(job.listing_status, 20),
        )

        cursor.execute(insert_sql, params)
        
        # Get the inserted ID
        cursor.execute("SELECT @@IDENTITY")
        new_id = int(cursor.fetchone()[0])
        
        connection.commit()
        job.short_id = new_id
        logging.info(f"Inserted new job with ID: {new_id}")
        return new_id
        
    except Exception as e:
        if connection:
            connection.rollback()
        logging.error(f"Error inserting job listing '{job.title}': {e}")
        import traceback
        logging.error(traceback.format_exc())
        raise
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def insert_skill(skill: Skill) -> bool:
    """Insert a skill into the database"""
    connection = None
    cursor = None
    
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        # Check if skill already exists for this job
        check_query = """
        SELECT ID, ShortID
        FROM Skills
        WHERE JobID = %s
          AND Source = %s
          AND SkillName = %s
        """
        cursor.execute(check_query, (skill.job_id, skill.source, skill.skill_name))
        existing = cursor.fetchone()
        
        if existing:
            # write back the existing ShortID
            skill.short_id = existing[1]
            logging.info(f"Skill already exists: {skill.skill_name} for job {skill.job_id}")
            return True
        
        # Insert new skill
        insert_query = """
        INSERT INTO Skills (
            JobID,
            ShortID,
            Source,
            SkillName,
            SkillCategory
        ) VALUES (%s, %s, %s, %s, %s)
        """
        
        params = (
            _truncate(skill.job_id, 100),
            skill.short_id,
            _truncate(skill.source, 50),
            _truncate(skill.skill_name, 150),
            _truncate(skill.skill_category, 50) if skill.skill_category else None
        )
        
        cursor.execute(insert_query, params)
        connection.commit()
        
        logging.info(f"Inserted skill: {skill.skill_name} for job {skill.job_id}")
        return True
        
    except Exception as e:
        if connection:
            connection.rollback()
        import traceback
        logging.error(f"Error inserting skill '{skill.skill_name}' for job {skill.job_id}: {e}")
        logging.error(traceback.format_exc())
        return False
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def get_job_by_id(job_id: str, source: str) -> Optional[JobListing]:
    """Retrieve a job listing by job_id and source"""
    connection = None
    cursor = None
    
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        query = """
        SELECT ID, JobID, Source, Title, Company, Link,
               SalaryMin, SalaryMax, Location, OperatingMode, WorkType,
               ExperienceLevel, EmploymentType, YearsOfExperience,
               ScrapeDate, ListingStatus
        FROM JobListings
        WHERE JobID = %s AND Source = %s
        """
        
        cursor.execute(query, (job_id, source))
        row = cursor.fetchone()
        
        if not row:
            return None
            
        job = JobListing(
            job_id=row[1],
            source=row[2],
            title=row[3],
            company=row[4],
            link=row[5],
            salary_min=row[6],
            salary_max=row[7],
            location=row[8],
            operating_mode=row[9],
            work_type=row[10],
            experience_level=row[11],
            employment_type=row[12],
            years_of_experience=row[13],
            scrape_date=row[14],
            listing_status=row[15]
        )
        job.short_id = row[0]
        
        return job
        
    except Exception as e:
        logging.error(f"Error retrieving job {job_id}: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def get_skills_for_job(job_id: str, source: str) -> List[Skill]:
    """Retrieve all skills for a specific job"""
    connection = None
    cursor = None
    skills = []
    
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        query = """
        SELECT JobID, ShortID, Source, SkillName, SkillCategory
        FROM Skills
        WHERE JobID = %s AND Source = %s
        """
        
        cursor.execute(query, (job_id, source))
        rows = cursor.fetchall()
        
        for row in rows:
            skill = Skill(
                job_id=row[0],
                source=row[2],
                skill_name=row[3],
                skill_category=row[4],
                short_id=row[1]
            )
            skills.append(skill)
            
        return skills
        
    except Exception as e:
        logging.error(f"Error retrieving skills for job {job_id}: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def update_job_status(job_id: str, source: str, status: str) -> bool:
    """Update the status of a job listing"""
    connection = None
    cursor = None
    
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        query = """
        UPDATE JobListings
        SET ListingStatus = %s
        WHERE JobID = %s AND Source = %s
        """
        
        cursor.execute(query, (_truncate(status, 20), job_id, source))
        rows_affected = cursor.rowcount
        connection.commit()
        
        if rows_affected > 0:
            logging.info(f"Updated job {job_id} status to {status}")
            return True
        else:
            logging.warning(f"No job found to update: {job_id}")
            return False
            
    except Exception as e:
        if connection:
            connection.rollback()
        logging.error(f"Error updating job status for {job_id}: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def get_all_active_jobs(source: Optional[str] = None) -> List[JobListing]:
    """Retrieve all active job listings, optionally filtered by source"""
    connection = None
    cursor = None
    jobs = []
    
    try:
        connection = get_sql_connection()
        cursor = connection.cursor()
        
        if source:
            query = """
            SELECT ID, JobID, Source, Title, Company, Link,
                   SalaryMin, SalaryMax, Location, OperatingMode, WorkType,
                   ExperienceLevel, EmploymentType, YearsOfExperience,
                   ScrapeDate, ListingStatus
            FROM JobListings
            WHERE ListingStatus = 'Active' AND Source = %s
            ORDER BY ScrapeDate DESC
            """
            cursor.execute(query, (source,))
        else:
            query = """
            SELECT ID, JobID, Source, Title, Company, Link,
                   SalaryMin, SalaryMax, Location, OperatingMode, WorkType,
                   ExperienceLevel, EmploymentType, YearsOfExperience,
                   ScrapeDate, ListingStatus
            FROM JobListings
            WHERE ListingStatus = 'Active'
            ORDER BY ScrapeDate DESC
            """
            cursor.execute(query)
        
        rows = cursor.fetchall()
        
        for row in rows:
            job = JobListing(
                job_id=row[1],
                source=row[2],
                title=row[3],
                company=row[4],
                link=row[5],
                salary_min=row[6],
                salary_max=row[7],
                location=row[8],
                operating_mode=row[9],
                work_type=row[10],
                experience_level=row[11],
                employment_type=row[12],
                years_of_experience=row[13],
                scrape_date=row[14],
                listing_status=row[15]
            )
            job.short_id = row[0]
            jobs.append(job)
            
        return jobs
        
    except Exception as e:
        logging.error(f"Error retrieving active jobs: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
