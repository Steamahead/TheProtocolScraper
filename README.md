# TheProtocol.it Job Scraper

This project is an automated Azure Function that scrapes Big Data and Data Science job listings from theprotocol.it. It extracts key details, identifies required technical skills, and stores the structured data in a centralized Azure SQL Database.

This scraper is a key component of a larger data analysis portfolio. The database it populates is shared with another scraper (JobMiner for pracuj.pl), and the combined dataset is used to power interactive Power BI dashboards that visualize trends in the job market.

Features

Automated Daily Scraping: Runs on a daily schedule using an Azure Timer Trigger to collect the latest job offers.
Source-Specific: Tailored to scrape job offers from theprotocol.it.
Rich Data Extraction: Captures job title, company, salary, location, operating mode, and required years of experience.
Advanced Skill Parsing: Identifies and categorizes dozens of technical skills (e.g., Python, AWS, Power BI) from job descriptions.
Efficient & Resilient: Uses concurrent requests to speed up scraping and a single database connection for efficient data insertion.

Technical Stack

Language: Python 3.11
Web Scraping: BeautifulSoup4, requests
Database: Azure SQL Database (pymssql)
Hosting: Azure Functions
CI/CD: GitHub Actions
Visualization: Power BI
