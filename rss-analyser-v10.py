import psycopg2
from psycopg2.extras import execute_batch
import json
from datetime import datetime
import time
from tqdm import tqdm
import google.generativeai as genai
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Database setup
DATABASE_URL = os.getenv('DATABASE_URL')

# Gemini API configuration
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
#client = genai.Client(api_key=GEMINI_API_KEY)
genai.configure(api_key=GEMINI_API_KEY)
MODEL_NAME = "gemini-2.0-flash"  # Use appropriate model
BATCH_SIZE = 10
REQUESTS_PER_MINUTE = 15  # API rate limit
SECONDS_PER_REQUEST = 60 / REQUESTS_PER_MINUTE  # 4 seconds per request
MAX_RUNTIME_SECONDS = 3600 # 1 hour timeout

def fetch_unprocessed_entries(conn, batch_size):
    """Fetch a batch of unprocessed RSS feed entries."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT id, title, description
            FROM rss_feed_entries
            WHERE processed = FALSE
            ORDER BY id ASC
            LIMIT %s;
        """, (batch_size,))
        return cursor.fetchall()

def mark_as_processed(conn, ids):
    """Mark processed entries in the database."""
    with conn.cursor() as cursor:
        execute_batch(cursor, """
            UPDATE rss_feed_entries
            SET processed = TRUE
            WHERE id = %s;
        """, [(id_,) for id_ in ids])

def insert_analysed_entries(conn, analysed_data):
    """Insert analysed data into the rss_feed_analysed table."""
    with conn.cursor() as cursor:
        execute_batch(cursor, """
            INSERT INTO rss_feed_analysed 
            (entry_id, translated_title, translated_description, keywords, sentiment, category)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, analysed_data)

def count_unprocessed_entries(conn):
    """Count total unprocessed entries for progress bar"""
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM rss_feed_entries WHERE processed = FALSE")
        count = cursor.fetchone()[0]
    return count

def process_batch(entries):
    """Process a batch of RSS feed entries using Google's Gemini API in one call."""
    results = []

    # Check entries
    if not entries:
        print("No entries to process in batch.")
        return results

    # Load prompt template
    try:
        with open('prompt-google.txt', 'r') as f:
            prompt_template = f.read()
    except FileNotFoundError:
        print("Error: prompt-google.txt not found.")
        return results
    
    # Initialize the model
    model = genai.GenerativeModel(MODEL_NAME)

    # Prepare batch input
    batch_input = ""
    entry_map = {}
    for idx, entry in enumerate(entries):
        entry_id, title, description = entry
        title = str(title) if title is not None else "Untitled"
        description = str(description) if description is not None else "No description"
        batch_input += f"Entry {idx + 1}:\n- Title: {title}\n- Description: {description}\n\n"
        entry_map[idx + 1] = entry_id
    
    if not batch_input.strip():
        print("Error: Batch input is empty after construction.")
        return results

    try:
        # Format prompt with all entries
        prompt = prompt_template.format(entries=batch_input)
        if not prompt.strip():
            print("Error: Formatted prompt is empty.")
            return results
        
        # Call the Gemini API once for the batch
        response = model.generate_content(
            prompt,
            generation_config={
                "temperature": 0.7,
                "max_output_tokens": 2000 * BATCH_SIZE,
            }
        )
        
        raw_response = response.text
        
        # Clean response
        cleaned_response = raw_response.strip()
        if cleaned_response.startswith("```json"):
            cleaned_response = cleaned_response.split("```json")[1]
        if cleaned_response.endswith("```"):
            cleaned_response = cleaned_response.split("```")[0]
        cleaned_response = cleaned_response.strip()

        # Parse JSON response
        data = json.loads(cleaned_response)
        
        # Verify it's a list and process results
        if not isinstance(data, list):
            print(f"Error: Response is not a list: {type(data)}")
            return results
        if len(data) != len(entries):
            print(f"Warning: Expected {len(entries)} results, got {len(data)}")

        required_fields = ["translated_title", "translated_description", "keywords", "sentiment", "category"]
        for idx, result in enumerate(data[:len(entries)]):  # Limit to input size
            entry_id = entry_map.get(idx + 1)
            if not entry_id:
                print(f"Warning: No mapping for index {idx + 1}")
                continue
            missing_fields = [field for field in required_fields if field not in result]
            if missing_fields:
                print(f"Warning: Missing fields for entry ID {entry_id}: {missing_fields}")
                continue
            results.append((
                entry_id,
                result["translated_title"],
                result["translated_description"],
                json.dumps(result["keywords"]),
                result["sentiment"],
                result["category"]
            ))

    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON for batch. Error: {e}")
        print(f"Raw response: {raw_response}")
    except Exception as e:
        print(f"Error processing batch: {type(e).__name__}: {e}")
    
    return results

def main():
    start_time = time.time() # Record start time for timeout
    processed_count_in_run = 0 # Track processed items in this run
    try:
        # Connect to the database
        conn = psycopg2.connect(DATABASE_URL)

        # Get total number of unprocessed entries for progress bar
        total_entries = count_unprocessed_entries(conn)
        print(f"Found {total_entries} unprocessed entries.")

        # Initialize progress bar for the total process
        with tqdm(total=total_entries, desc="Processing entries", unit="entry") as pbar:
            last_request_time = 0

            while True:
                # --- Timeout Check ---
                if time.time() - start_time > MAX_RUNTIME_SECONDS:
                    print(f"Maximum runtime of {MAX_RUNTIME_SECONDS // 60} minutes exceeded. Exiting.")
                    break

                # Fetch a batch of unprocessed entries
                entries = fetch_unprocessed_entries(conn, BATCH_SIZE)
                if not entries:
                    print("No more entries to process.")
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
                    # Insert analysed data if successful
                    if analysed_data:
                        insert_analysed_entries(conn, analysed_data)

                    # Mark the entire fetched batch as processed (prevents infinite loop on error)
                    mark_as_processed(conn, entry_ids)

                    # Commit changes (insertions + marking processed)
                    conn.commit()

                    # Update progress bar for the processed batch size
                    pbar.update(batch_size_fetched)
                    processed_count_in_run += batch_size_fetched

                except psycopg2.Error as db_err: # Catch specific DB errors
                    print(f"Database error during commit for batch: {db_err}. Rolling back transaction and continuing.")
                    try:
                        conn.rollback()
                    except psycopg2.Error as rb_err:
                        print(f"Rollback failed: {rb_err}")
                except Exception as e: # Catch any other unexpected error during DB interaction
                    print(f"Unexpected error during DB interaction: {e}. Rolling back and continuing.")
                    try:
                        conn.rollback() # Attempt rollback
                    except Exception as rb_err:
                        print(f"Rollback failed: {rb_err}")


        # --- Cleanup after loop ---
        pbar.close()
        conn.close()
        print(f"Processing finished or stopped. Total entries processed/attempted in this run: {processed_count_in_run}")

    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in main: {type(e).__name__}: {e}")

if __name__ == "__main__":
    main()