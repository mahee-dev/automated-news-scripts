# RSS Feed Fetcher and Analyzer

This repository contains two Python scripts designed to fetch RSS feeds, store them in a database, and analyze their content using Google's Gemini API. These scripts are intended to be run as cron jobs on GitHub Actions for automated, scheduled execution.

- **`rss-fetcher-v10.py`**: Fetches RSS feeds from specified sources and stores them in a PostgreSQL database.
- **`rss-analyser-v10.py`**: Analyzes unprocessed RSS feed entries using the Gemini API and stores the results in the database.

The scripts are feeding the Global News Analysis service: https://globalnewssite.netlify.app

## Processing Flow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  RSS Sources    │────▶│  RSS Fetcher    │────▶│  rss_feed_      │
│  (Database)     │     │  (v10)          │     │  entries        │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  rss_feed_      │◀────│  RSS Analyser   │◀────│  Unprocessed    │
│  analysed       │     │  (v10)          │     │  Entries        │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │
                               ▼
                        ┌─────────────────┐
                        │  Gemini 2.5     │
                        │  Flash API      │
                        └─────────────────┘
```

## Features

### RSS Fetcher
- Retrieves RSS feeds from URLs stored in a database
- Stores feed metadata (title, link, published date, description) in a PostgreSQL database
- Limits storage to the **latest 20 entries** per feed to avoid redundancy
- Cleans HTML content from descriptions using BeautifulSoup
- Handles SSL certificate errors gracefully
- Uses SQLAlchemy with connection pooling
- Logs errors and processing details for debugging

### RSS Analyzer
- Uses **Gemini 2.5-Flash** model for content analysis
- Processes unprocessed RSS entries in batches (batch size: **10**)
- Extracts translated titles, descriptions, keywords, sentiment, and categories
- Updates the database with analysis results and marks entries as processed
- Implements rate limiting (**15 requests/minute**, ~4 second delay between requests)
- Uses Pydantic for response validation
- **1-hour timeout** protection to prevent runaway execution
- Retry logic with exponential backoff for API failures
- Database statement timeouts (**5 seconds**) to prevent long-running queries

## Categories

Content is classified into one of 14 categories:
- Politics, Business, World, Local, Sports, Entertainment, Technology
- Health, Science, Opinion, Lifestyle, Education, Crime, Environment

## Sentiment Values

- `positive`
- `neutral`
- `negative`

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

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `GEMINI_API_KEY` | Google Gemini API key |
| `PROMPT_FILE` | Path to prompt template (default: `prompt-google.txt`) |

## GitHub Actions

The workflow runs automatically via `.github/workflows/daily-rss.yml`:

- **Schedule**: Daily at 12:00 UTC
- **Manual trigger**: Supports `workflow_dispatch` for on-demand execution
- **Steps**:
  1. Fetches RSS feeds (`rss-fetcher-v10.py`)
  2. Analyzes new entries (`rss-analyser-v10.py`)

## Database Schema

**rss_feed_sources:**
- id (Integer, Primary Key)
- url (String, Unique)

**rss_feed_entries:**
- id (Integer, Primary Key)
- title (String)
- link (String)
- published (DateTime)
- description (Text)
- feed_id (Integer, Foreign Key to rss_feed_sources)
- processed (Boolean, Default: False)

**rss_feed_analysed:**
- entry_id (Integer, Foreign Key to rss_feed_entries)
- translated_title (String)
- translated_description (Text)
- keywords (JSON)
- sentiment (String)
- category (String)
