import requests
import feedparser
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, ForeignKey
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

    feed = relationship("RSSFeedSource", back_populates="entries")

# Create the tables
Base.metadata.create_all(engine)

# Function to add new entries
def add_new_entries(session, feed_entries, source_id, limit=15):
    latest_entries = sorted(feed_entries, key=lambda e: datetime(*e.published_parsed[:6]), reverse=True)[:limit]
    
    for entry in latest_entries:
        latest_entry = session.query(RSSFeedEntry).filter_by(link=entry.link).order_by(RSSFeedEntry.published.desc()).first()
        
        if latest_entry is None or datetime(*entry.published_parsed[:6]) > latest_entry.published:
            clean_description = BeautifulSoup(entry.get('description', ''), 'html.parser').get_text()
            
            rss_entry = RSSFeedEntry(
                title=entry.title,
                link=entry.link,
                published=datetime(*entry.published_parsed[:6]),
                description=clean_description,
                feed_id=source_id
            )
            session.add(rss_entry)
            print(f"Added new entry: {entry.title}")

# Function to fetch and save RSS feeds
def fetch_and_save_rss_feeds():
    Session = sessionmaker(bind=engine)
    session = Session()

    feed_sources = session.query(RSSFeedSource).all()

    for source in feed_sources:
        try:
            response = requests.get(source.url)
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
