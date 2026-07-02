import requests
import feedparser
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, ForeignKey, Boolean, text
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from datetime import datetime
from bs4 import BeautifulSoup
import logging
from dotenv import load_dotenv
import os
import sys

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    #filename='rss_fetcher.log',  # Log file name
    stream=sys.stdout,
    level=logging.INFO,           # Log level
    format='%(asctime)s - %(levelname)s - %(message)s'  # Log message format
)

# Database setup
DATABASE_URL = os.getenv('DATABASE_URL')

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    isolation_level="AUTOCOMMIT",  # <- IMPORTANT
)

Base = declarative_base()

# Define the RSS feed source model
class RSSFeedSource(Base):
    __tablename__ = 'rss_feed_sources'
    
    id = Column(Integer, primary_key=True)
    url = Column(String, unique=True, nullable=False)

    entries = relationship("RSSFeedEntry", back_populates="feed")

# Define the RSS feed entry model
class RSSFeedEntry(Base):
    __tablename__ = 'rss_feed_entries'
    
    id = Column(Integer, primary_key=True)
    title = Column(String)
    link = Column(String)
    published = Column(DateTime)
    description = Column(Text)
    feed_id = Column(Integer, ForeignKey('rss_feed_sources.id'), nullable=False)
    processed = Column(Boolean, nullable=False, default=False, server_default=text("false"))

    feed = relationship("RSSFeedSource", back_populates="entries")

# Create the tables
Base.metadata.create_all(engine)

def ensure_schema():
    """Keep fresh databases aligned with the analyzer's expected contract."""
    with engine.begin() as connection:
        connection.execute(text("""
            ALTER TABLE rss_feed_entries
            ADD COLUMN IF NOT EXISTS processed BOOLEAN NOT NULL DEFAULT FALSE;
        """))

def parse_entry_datetime(entry):
    parsed = entry.get('published_parsed') or entry.get('updated_parsed')
    if not parsed:
        return None

    try:
        return datetime(*parsed[:6])
    except (TypeError, ValueError):
        return None

def prepare_feed_entry(entry):
    link = str(entry.get('link', '')).strip()
    if not link:
        logging.warning("Skipping RSS entry without a link: %s", entry.get('title', '<untitled>'))
        return None

    published_at = parse_entry_datetime(entry)
    if published_at is None:
        logging.warning("Skipping RSS entry without a usable date: %s", link)
        return None

    return entry, published_at, link

# Function to add new entries
def add_new_entries(session, feed_entries, source_id, limit=15):
    valid_entries = []
    for entry in feed_entries:
        prepared = prepare_feed_entry(entry)
        if prepared is not None:
            valid_entries.append(prepared)

    latest_entries = sorted(valid_entries, key=lambda e: e[1], reverse=True)[:limit]
    
    for entry, published_at, link in latest_entries:
        latest_entry = session.query(RSSFeedEntry).filter_by(link=link).order_by(RSSFeedEntry.published.desc()).first()
        
        if latest_entry is None or published_at > latest_entry.published:
            clean_description = BeautifulSoup(entry.get('description', ''), 'html.parser').get_text()
            
            rss_entry = RSSFeedEntry(
                title=entry.get('title') or link,
                link=link,
                published=published_at,
                description=clean_description,
                feed_id=source_id,
                processed=False
            )
            session.add(rss_entry)
            print(f"Added new entry: {rss_entry.title}")

# Function to fetch and save RSS feeds
def fetch_and_save_rss_feeds():
    ensure_schema()

    Session = sessionmaker(bind=engine)
    session = Session()

    feed_sources = session.query(RSSFeedSource).all()

    for source in feed_sources:
        try:
            response = requests.get(source.url, timeout=30)
            response.raise_for_status()
            feed = feedparser.parse(response.content)

            add_new_entries(session, feed.entries, source.id, limit=20)

            # COMMIT after each feed
            try:
                session.commit()
            except Exception as commit_error:
                logging.error(f"Commit failed for {source.url}: {commit_error}")
                session.rollback()

            logging.info(f"Processed {len(feed.entries)} entries from {source.url}.")
        
        except requests.exceptions.SSLError as ssl_error:
            logging.error(f"SSL error fetching the RSS feed from {source.url}: {ssl_error}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching the RSS feed from {source.url}: {e}")
        except Exception as e:
            logging.error(f"An error occurred while processing {source.url}: {e}")

    session.close()

if __name__ == '__main__':
    fetch_and_save_rss_feeds()
