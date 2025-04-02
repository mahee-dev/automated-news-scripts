# RSS Feed Fetcher and Analyzer

This repository contains two Python scripts designed to fetch RSS feeds, store them in a database, and analyze their content using Google's Gemini API. These scripts are intended to be run as cron jobs on GitHub Actions for automated, scheduled execution.

- **`rss_fetcher.py`**: Fetches RSS feeds from specified sources and stores them in a PostgreSQL database.
- **`rss_analyzer.py`**: Analyzes unprocessed RSS feed entries using the Gemini API and stores the results in the database.

The scripts are feeding the Global News Analysis service: https://globalnewssite.netlify.app

## Features

- **RSS Fetcher**:
  - Retrieves RSS feeds from URLs stored in a database.
  - Stores feed metadata (title, link, published date, description) in a PostgreSQL database.
  - Limits storage to the latest 10 entries per feed to avoid redundancy.
  - Logs errors and processing details for debugging.

- **RSS Analyzer**:
  - Processes unprocessed RSS entries in batches using Google's Gemini API.
  - Extracts translated titles, descriptions, keywords, sentiment, and categories.
  - Updates the database with analysis results and marks entries as processed.
  - Implements rate limiting to comply with API constraints.

## Prerequisites

- Python 3.8+
- PostgreSQL database
- Google Gemini API key
- GitHub account (for running as cron jobs via GitHub Actions)

### Required Python Packages
Install the dependencies using:
```bash
pip install requests feedparser sqlalchemy psycopg2-binary beautifulsoup4 python-dotenv tqdm google-generativeai
```

#### Database Schema
rss_feed_sources:
- id (Integer, Primary Key)
- url (String, Unique)

rss_feed_entries:
- id (Integer, Primary Key)
- title (String)
- link (String)
- published (DateTime)
- description (Text)
- feed_id (Integer, Foreign Key to rss_feed_sources)
- processed (Boolean, Default: False)

rss_feed_analysed:
- entry_id (Integer, Foreign Key to rss_feed_entries)
- translated_title (String)
- translated_description (Text)
- keywords (JSON)
- sentiment (String)
- category (String)



