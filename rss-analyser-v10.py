# rss-analyser-v10.py
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.pool import SimpleConnectionPool
import json
from datetime import datetime
import time
from tqdm import tqdm
import google.generativeai as genai
from dotenv import load_dotenv
import os
import logging
from pydantic import BaseModel, ValidationError, Field
from typing import List


# Load environment variables from .env file
load_dotenv()

# Validate required environment variables
required_vars = ['DATABASE_URL', 'GEMINI_API_KEY']
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database setup
DATABASE_URL = os.getenv('DATABASE_URL')
connection_pool = None

def init_connection_pool():
    global connection_pool
    connection_pool = SimpleConnectionPool(
        minconn=1,
        maxconn=5,
        dsn=DATABASE_URL
    )

# Gemini API configuration
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
genai.configure(api_key=GEMINI_API_KEY)
MODEL_NAME = "gemini-2.5-flash"  # Use appropriate model
BATCH_SIZE = 10
REQUESTS_PER_MINUTE = 15  # API rate limit
SECONDS_PER_REQUEST = 60 / REQUESTS_PER_MINUTE  # 4 seconds per request
MAX_RUNTIME_SECONDS = 3600 # 1 hour timeout
PROMPT_FILE = os.getenv('PROMPT_FILE', 'prompt-google.txt')



class ArticleResponse(BaseModel):
    translated_title: str = Field(default="")
    translated_description: str = Field(default="")
    keywords: List[str] = Field(default_factory=list)
    sentiment: str = Field(default="")
    category: str = Field(default="")


def fetch_unprocessed_entries(conn: psycopg2.extensions.connection, batch_size: int) -> list[tuple]:
    """Fetch a batch of unprocessed RSS feed entries.
    
    Args:
        conn: Database connection
        batch_size: Maximum number of entries to fetch
        
    Returns:
        List of tuples containing (id, title, description) for each entry
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            SET statement_timeout = 5000;  -- 5 second timeout
            
            SELECT id, title, description
            FROM rss_feed_entries
            WHERE processed = FALSE
            ORDER BY id ASC
            LIMIT %s;
        """, (batch_size,))
        return cursor.fetchall()


def mark_as_processed(conn: psycopg2.extensions.connection, ids: list[int], success_ids: list[int]) -> None:
    """Mark processed entries in the database by setting processed = TRUE.

    Args:
        conn: Database connection
        ids: List of all entry IDs to mark as processed
        success_ids: List of entry IDs that were successfully analyzed (unused here)
    """
    # Note: Update only sets 'processed = TRUE' as 'processing_success' column is not present.
    with conn.cursor() as cursor:
        cursor.execute("SET statement_timeout = 5000;")  # 5 second timeout
        execute_batch(cursor, """
            UPDATE rss_feed_entries
            SET processed = TRUE
            WHERE id = %s;
        """, [(id_,) for id_ in ids]) # Parameter tuple format changed

def insert_analysed_entries(conn: psycopg2.extensions.connection, analysed_data: list[tuple]) -> None:
    """Insert analysed data into the rss_feed_analysed table.
    
    Args:
        conn: Database connection
        analysed_data: List of tuples containing analysis results
    """
    with conn.cursor() as cursor:
        cursor.execute("SET statement_timeout = 5000;")  # Set 5 second timeout
        execute_batch(cursor, """
            INSERT INTO rss_feed_analysed 
            (entry_id, translated_title, translated_description, keywords, sentiment, category)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, analysed_data)

def count_unprocessed_entries(conn: psycopg2.extensions.connection) -> int:
    """Count total unprocessed entries for progress bar.
    
    Args:
        conn: Database connection
        
    Returns:
        Number of unprocessed entries
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            SET statement_timeout = 5000;  -- 5 second timeout
            SELECT COUNT(*) FROM rss_feed_entries WHERE processed = FALSE
        """)
        count = cursor.fetchone()[0]
    return count

def process_batch(entries: list[tuple]) -> list[tuple]:
    """Process a batch of RSS feed entries using Google's Gemini API, with Pydantic validation."""
    results = []
    raw_response = None

    if not entries:
        logger.warning("No entries to process in batch.")
        return results

    # Load prompt template
    try:
        with open(PROMPT_FILE, 'r') as f:
            prompt_template = f.read()
    except FileNotFoundError:
        logger.error("Prompt file not found: %s", PROMPT_FILE)
        return results

    model = genai.GenerativeModel(MODEL_NAME)

    batch_input = ""
    entry_map = {}
    for idx, entry in enumerate(entries):
        entry_id, title, description = entry
        title = str(title) if title is not None else "Untitled"
        description = str(description) if description is not None else "No description"
        batch_input += f"Entry {idx + 1}:\n- Title: {title}\n- Description: {description}\n\n"
        entry_map[idx + 1] = entry_id

    if not batch_input.strip():
        logger.error("Batch input is empty after construction")
        return results

    try:
        prompt = prompt_template.format(entries=batch_input)
        if not prompt.strip():
            logger.error("Formatted prompt is empty")
            return results

        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                response = model.generate_content(
                    prompt,
                    generation_config={
                        "temperature": 0.7,
                        "max_output_tokens": 2000 * BATCH_SIZE,
                    }
                )
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"API call failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
                time.sleep(retry_delay)
                retry_delay *= 2

        raw_response = response.text

        cleaned_response = raw_response.strip()
        if cleaned_response.startswith("```json"):
            cleaned_response = cleaned_response.split("```json")[1]
        if cleaned_response.endswith("```"):
            cleaned_response = cleaned_response.split("```")[0]
        cleaned_response = cleaned_response.strip()

        # --- Parse and Validate
        try:
            data = json.loads(cleaned_response)
            if not isinstance(data, list):
                raise ValueError("Top-level JSON must be an array")
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Invalid JSON response format: {e}")
            logger.debug(f"Response content: {cleaned_response}")
            return results

        if len(data) != len(entries):
            logger.warning(f"Expected {len(entries)} results, got {len(data)}")

        for idx, raw_item in enumerate(data[:len(entries)]):
            entry_id = entry_map.get(idx + 1)
            if not entry_id:
                logger.warning(f"No mapping for index {idx + 1}")
                continue

            try:
                validated_item = ArticleResponse(**raw_item)
            except ValidationError as ve:
                logger.warning(f"Validation error for entry ID {entry_id}: {ve}")
                continue  # Skip invalid entry

            results.append((
                entry_id,
                validated_item.translated_title,
                validated_item.translated_description,
                json.dumps(validated_item.keywords),
                validated_item.sentiment,
                validated_item.category
            ))

    except Exception as e:
        logger.error(f"Error processing batch: {type(e).__name__}: {e}")
        logger.debug(f"Raw response: {raw_response}")

    return results

def main():
    start_time = time.time() # Record start time for timeout
    processed_count_in_run = 0 # Track processed items in this run
    try:
        # Get connection from pool
        conn = connection_pool.getconn()

        # Get total number of unprocessed entries for progress bar
        total_entries = count_unprocessed_entries(conn)
        logger.info(f"Found {total_entries} unprocessed entries")

        # Initialize progress bar for the total process
        with tqdm(total=total_entries, desc="Processing entries", unit="entry") as pbar:
            last_request_time = 0

            while True:
                # --- Timeout Check ---
                if time.time() - start_time > MAX_RUNTIME_SECONDS:
                    logger.warning(f"Maximum runtime of {MAX_RUNTIME_SECONDS // 60} minutes exceeded. Exiting.")
                    break

                # Fetch a batch of unprocessed entries
                entries = fetch_unprocessed_entries(conn, BATCH_SIZE)
                if not entries:
                    logger.info("No more entries to process")
                    break
                
                entry_ids = [entry[0] for entry in entries]
                batch_size_fetched = len(entry_ids)


                # Rate limiting: wait if necessary
                current_time = time.time()
                time_since_last = current_time - last_request_time
                if time_since_last < SECONDS_PER_REQUEST:
                    time.sleep(SECONDS_PER_REQUEST - time_since_last)

                # Process the batch (catches JSON errors internally)
                analysed_data = process_batch(entries)
                last_request_time = time.time()

                # --- Database Operations & Error Handling ---
                try:
                    success_ids = [d[0] for d in analysed_data]
                    
                    # Insert analysed data if successful
                    if analysed_data:
                        insert_analysed_entries(conn, analysed_data)

                    # Mark batch as processed, with success status per entry
                    mark_as_processed(conn, entry_ids, success_ids)

                    # Commit changes (insertions + marking processed)
                    conn.commit()

                    # Update progress bar for the processed batch size
                    pbar.update(batch_size_fetched)
                    processed_count_in_run += batch_size_fetched

                except psycopg2.Error as db_err: # Catch specific DB errors
                    logger.error(f"Database error during commit for batch: {db_err}. Rolling back transaction and continuing.")
                    try:
                        conn.rollback()
                    except psycopg2.Error as rb_err:
                        logger.error(f"Rollback failed: {rb_err}")
                except Exception as e: # Catch any other unexpected error during DB interaction
                    logger.error(f"Unexpected error during DB interaction: {e}. Rolling back and continuing.")
                    try:
                        conn.rollback() # Attempt rollback
                    except Exception as rb_err:
                        logger.error(f"Rollback failed: {rb_err}")


        # --- Cleanup after loop ---
        pbar.close()
        # Return connection to pool
        connection_pool.putconn(conn)
        logger.info(f"Processing finished. Total entries processed: {processed_count_in_run}")

    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in main: {type(e).__name__}: {e}")

if __name__ == "__main__":
    try:
        init_connection_pool()
        main()
    finally:
        if connection_pool:
            connection_pool.closeall()
