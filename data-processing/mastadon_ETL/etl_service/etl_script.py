import os
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import psycopg2
import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from dotenv import load_dotenv

load_dotenv()

MASTODON_URL = 'https://mastodon.social' # public 
TOPIC = "ORCL" # Replace with any keywords to search posts on mastadon eg. 'bitcoin'
DISCORD_URL = os.getenv('DISCORD_WEBHOOK_URL')
THRESHOLD = 0.85 # sentiments are rated between -1 to 1 , replace with any number in this range

# Database connection details (from .env)
DB_DETAILS = {
    'host': os.getenv('ANALYTICS_DB_HOST'),
    'user': os.getenv('POSTGRES_USER_ANALYTICS'),
    'password': os.getenv('POSTGRES_PASSWORD_ANALYTICS'),
    'dbname': os.getenv('POSTGRES_DB_ANALYTICS')
}

ANALYZER = SentimentIntensityAnalyzer()
HTML_PATTERN = r"<[^>]+>" # replace with a pattern to remove all HTML tags in the extracted text

# --- E: Extraction ---


def extract_data():
    """Gets the latest page of statuses from Mastodon API."""
    api_url = f"{MASTODON_URL}/api/v1/timelines/tag/{TOPIC}"
    print(f"Extracting data from: {api_url}")

    response = requests.get(api_url)

    if response.status_code != 200:
        print("Failed to fetch Mastodon data:", response.text)
        return []

    return response.json()

# --- T: Transformation ---


def clean_html(raw_html):
    """Strips HTML tags and cleans whitespace."""
    pattern = re.compile(HTML_PATTERN)
    clean_text = re.sub(pattern, '', raw_html ) #
    clean_text = ' '.join(clean_text.split())   #clean whitespaces
    return clean_text

def clean_and_transform(raw_statuses):
    """Cleans data, uses the raw timestamp string, and adds VADER sentiment."""
    transformed_list = []

    print("Transforming data by cleaning HTML and adding sentiment...")
    for status in raw_statuses:
        text_content = clean_html(status.get('content', ''))
        created_at_str = status.get('created_at')
        vs = ANALYZER.polarity_scores(text_content)

        transformed_list.append({
            'id': status.get('id'),
            'created_at': created_at_str, 
            'text_content': text_content,
            'vader_compound_score': vs['compound']
        })
    return transformed_list

# --- L: Load Analytics & Discord Integration ---


def connect_db():
    """
    Connects to the default 'postgres' database and creates the specified
    database using SQLAlchemy.
    """
    try:
        # Create a connection engine to the default 'postgres' database
        # This is necessary to issue the CREATE DATABASE command

        temp_engine = create_engine(f'postgresql+psycopg2://{DB_DETAILS["user"]}:{DB_DETAILS["password"]}@{DB_DETAILS["host"]}:5432/{DB_DETAILS["dbname"]}')

        # Connect to the database and set the isolation level to AUTOCOMMIT
        with temp_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            # Check if the database exists
            query = text(f"SELECT 1 FROM pg_database WHERE datname = '{DB_DETAILS['dbname']}'")
            result = conn.execute(query)
            exists = result.scalar()

            if not exists:
                # Create the database using a raw SQL command
                conn.execute(text(f"CREATE DATABASE {DB_DETAILS['dbname']}"))
                print(f"Database {DB_DETAILS['dbname']} created successfully.")
            else:
                print(f"Database {DB_DETAILS['dbname']} already exists.")

    except Exception as e:
        print(f"Error creating database: {e}")

    return temp_engine


def setup_tables(engine):
    create_table_query = text("""
    CREATE TABLE IF NOT EXISTS mastodon_analytics (
        id BIGINT PRIMARY KEY,
        created_at TIMESTAMP,
        text_content TEXT,
        sentiment_score NUMERIC
    );
    """)

    with engine.connect() as conn:
        conn.execute(create_table_query)
        conn.commit()
        print("Table 'mastodon_analytics' created or exists.")



def load_analytics_and_alert(analytics_conn, transformed_data):
    """
    Inserts transformed data into mastodon_analytics.
    Sends Discord alert ONLY for new positive posts.
    """

    insert_query = text("""
        INSERT INTO mastodon_analytics (id, created_at, text_content, sentiment_score)
        VALUES (:id, :created_at, :text_content, :sentiment_score)
        ON CONFLICT (id) DO NOTHING
        RETURNING id;
    """)

    with analytics_conn.connect() as conn:
        for row in transformed_data:

            # Convert row keys to match SQL fields
            params = {
                "id": row["id"],
                "created_at": row["created_at"],
                "text_content": row["text_content"],
                "sentiment_score": row["vader_compound_score"]
            }

            result = conn.execute(insert_query, params)
            inserted = result.fetchone()  # None if duplicate

            # Only alert on NEW rows
            if inserted and row["vader_compound_score"] > THRESHOLD:
                send_discord_alert({
                    "sentiment_score": row["vader_compound_score"],
                    "text_content": row["text_content"]
                })

        print("Data inserted. Alerts sent where applicable.")


# --- Alerts to discord ---

def send_discord_alert(data):
    """Sends a message to Discord webhook."""
    message = {
        "content": f"**Positive Alert!** Score: {data['sentiment_score']:.2f} for #{TOPIC}",
        "embeds": [{"description": data['text_content']}]
    }
    requests.post(DISCORD_URL, json=message)

def run_etl_pipeline():
    """Executes the full ETL pipeline."""
    print("Starting ETL pipeline...")

    # Connect DB
    engine = connect_db()

    # Create table
    setup_tables(engine)

    # ---- ETL ----
    raw = extract_data()                      # Step 1: Extract
    transformed = clean_and_transform(raw)    # Step 2: Transform
    load_analytics_and_alert(engine, transformed)  # Step 3: Load + Alert


    print("ETL pipeline completed.")









if __name__ == "__main__":
    run_etl_pipeline()
