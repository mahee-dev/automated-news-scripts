import psycopg2
from psycopg2.extras import execute_batch
import json
from datetime import datetime
import time
from tqdm import tqdm
import google.generativeai as genai
from dotenv import load_dotenv

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
        print(f"Error processing batch: {type(e).__name__}: {str(e)}")
    
    return results

def main():
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
                # Fetch a batch of unprocessed entries
                entries = fetch_unprocessed_entries(conn, BATCH_SIZE)
                if not entries:
                    print("No more entries to process.")
                    break

                # Rate limiting: wait if necessary
                current_time = time.time()
                time_since_last = current_time - last_request_time
                if time_since_last < SECONDS_PER_REQUEST:
                    time.sleep(SECONDS_PER_REQUEST - time_since_last)

                # Process the batch
                analysed_data = process_batch(entries)
                last_request_time = time.time()
                
                if analysed_data:
                    # Insert analysed data into the database
                    insert_analysed_entries(conn, analysed_data)

                    # Mark processed entries
                    ids = [entry[0] for entry in entries]
                    mark_as_processed(conn, ids)

                    # Commit the transaction
                    conn.commit()

                    # Update progress bar
                    pbar.update(len(entries))
                    pbar.set_postfix(last_id=ids[-1], batch_size=len(entries))
                    print(f"Processed and committed batch of {len(entries)} entries.")
                
    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()