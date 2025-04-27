import requests
import feedparser
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from bs4 import BeautifulSoup
import logging
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    filename='rss_fetcher.log',  # Log file name
    level=logging.INFO,           # Log level
    format='%(asctime)s - %(levelname)s - %(message)s'  # Log message format
)

# Database setup
DATABASE_URL = os.getenv('DATABASE_URL')

engine = create_engine(DATABASE_URL)
Base = declarative_base()

# Define the RSS feed source model
class RSSFeedSource(Base):
    __tablename__ = 'rss_feed_sources'
    
    id = Column(Integer, primary_key=True)
    url = Column(String, unique=True, nullable=False)

    # Relationship to RSS feed entries
    entries = relationship("RSSFeedEntry", back_populates="feed")

# Define the RSS feed entry model
class RSSFeedEntry(Base):
    __tablename__ = 'rss_feed_entries'
    
    id = Column(Integer, primary_key=True)
    title = Column(String)
    link = Column(String)
    published = Column(DateTime)
    description = Column(Text)  # Added description field
    feed_id = Column(Integer, ForeignKey('rss_feed_sources.id'), nullable=False)  # Foreign key to RSSFeedSource

    # Relationship to RSS feed source
    feed = relationship("RSSFeedSource", back_populates="entries")

# Create the tables
Base.metadata.create_all(engine)

# Function to check for duplicates and add new entries
# def add_new_entries(session, feed_entries, source_id):
#     for entry in feed_entries:
def add_new_entries(session, feed_entries, source_id, limit=15):
    # Sort entries by published date and take the latest 'limit' entries
    latest_entries = sorted(feed_entries, key=lambda e: datetime(*e.published_parsed[:6]), reverse=True)[:limit]
    
    for entry in latest_entries:
        # Check for the latest entry for this source
        latest_entry = session.query(RSSFeedEntry).filter_by(link=entry.link).order_by(RSSFeedEntry.published.desc()).first()
        
        # If no entry exists or the new entry is more recent, add it
        if latest_entry is None or datetime(*entry.published_parsed[:6]) > latest_entry.published:
            # Clean the description using BeautifulSoup
            clean_description = BeautifulSoup(entry.get('description', ''), 'html.parser').get_text()  # Strip HTML tags
            
            rss_entry = RSSFeedEntry(
                title=entry.title,
                link=entry.link,
                published=datetime(*entry.published_parsed[:6]),  # Convert to datetime
                description=clean_description,  # Use cleaned description
                feed_id=source_id  # Set the foreign key to the source ID
            )
            session.add(rss_entry)
            print(f"Added new entry: {entry.title}")

# Function to fetch and save RSS feeds from sources in the database
def fetch_and_save_rss_feeds():
    Session = sessionmaker(bind=engine)
    session = Session()

    # Fetch all RSS feed sources from the database
    feed_sources = session.query(RSSFeedSource).all()

    for source in feed_sources:
        try:
            # Remove SSL verification for security
            response = requests.get(source.url)  # SSL verification is now enabled
            response.raise_for_status()  # Raise an error for bad responses
            feed = feedparser.parse(response.content)

            # Add new entries for the current feed, limiting to the latest 20
            # add_new_entries(session, feed.entries, source.id)
            add_new_entries(session, feed.entries, source.id, limit=20)

            # Log the number of processed entries
            logging.info(f"Processed {len(feed.entries)} entries from {source.url}.")
            session.commit()
        except requests.exceptions.SSLError as ssl_error:
            # Log SSL certificate errors
            logging.error(f"SSL error fetching the RSS feed from {source.url}: {ssl_error}")
        except requests.exceptions.RequestException as e:
            # Log the error
            logging.error(f"Error fetching the RSS feed from {source.url}: {e}")
        except Exception as e:
            # Log any other errors
            logging.error(f"An error occurred while processing {source.url}: {e}")

    session.close()

if __name__ == '__main__':
    fetch_and_save_rss_feeds()
