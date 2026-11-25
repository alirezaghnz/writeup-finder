
import os
import time
import logging
import requests
import psycopg2
import xml.etree.ElementTree as ET

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from psycopg2.extras import execute_values
from config import DB_CONFIG, WEB_HOOK


##Configuration

TIMEOUT = 15
MAX_RETRIES = 3
MAX_RETRY_WAIT = 300  # maximum wait per retry: 5 minutes
REQUEST_DELAY = 2     # delay between Medium feeds

YOUTUBE_FILE = "files/youtube_channels.txt"
MEDIUM_FILE = "files/lists.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

log = logging.getLogger("writeup-finder")


## http session with custom headers to avoid being blocked by some servers
def create_session():
    session = requests.Session()

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
    })

    return session



# connect Database
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def setup_database():
    conn = get_db_connection()

    try:
        with conn.cursor() as cursor:

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS youtube_videos (
                    id SERIAL PRIMARY KEY,
                    link TEXT UNIQUE NOT NULL,
                    title TEXT,
                    pub_date TIMESTAMP
                );
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS medium_writeups (
                    id SERIAL PRIMARY KEY,
                    link TEXT UNIQUE NOT NULL,
                    title TEXT,
                    pub_date TIMESTAMP
                );
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pentesterland_writeups (
                    id SERIAL PRIMARY KEY,
                    link TEXT UNIQUE NOT NULL,
                    title TEXT,
                    pub_date TIMESTAMP,
                    authors TEXT[],
                    programs TEXT[],
                    bugs TEXT[],
                    added_date TIMESTAMP
                );
            """)

        conn.commit()
        log.info("Database setup completed.")

    finally:
        conn.close()


ALLOWED_TABLES = {
    "youtube_videos",
    "medium_writeups",
    "pentesterland_writeups",
}


def is_link_processed(conn, table_name, link):

    if table_name not in ALLOWED_TABLES:
        raise ValueError("Invalid table name")

    with conn.cursor() as cursor:
        cursor.execute(
            f"SELECT 1 FROM {table_name} WHERE link = %s",
            (link,)
        )

        return cursor.fetchone() is not None


def save_processed_link(
    conn,
    table_name,
    link,
    title,
    pub_date,
    authors=None,
    programs=None,
    bugs=None,
    added_date=None,
):

    if table_name not in ALLOWED_TABLES:
        raise ValueError("Invalid table name")

    with conn.cursor() as cursor:

        if table_name == "pentesterland_writeups":

            cursor.execute("""
                INSERT INTO pentesterland_writeups
                    (link, title, pub_date, authors, programs, bugs, added_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (link) DO NOTHING
            """, (
                link,
                title,
                parse_date(pub_date),
                authors or [],
                programs or [],
                bugs or [],
                parse_date(added_date),
            ))

        else:

            cursor.execute(f"""
                INSERT INTO {table_name}
                    (link, title, pub_date)
                VALUES (%s, %s, %s)
                ON CONFLICT (link) DO NOTHING
            """, (
                link,
                title,
                parse_date(pub_date),
            ))

    conn.commit()



# Date parsing

def parse_date(value):

    if not value:
        return None

    if isinstance(value, datetime):
        dt = value

    else:
        value = str(value).strip()

        try:
            dt = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )

        except ValueError:

            try:
                dt = parsedate_to_datetime(value)

            except (ValueError, TypeError, OverflowError):
                log.warning("Could not parse date: %s", value)
                return None

    # PostgreSQL TIMESTAMP columns are timezone-naive here.
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

    return dt



# HTTP retry handling

def get_with_retry(session, url, source="source"):

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = session.get(
                url,
                timeout=TIMEOUT,
            )

        except requests.RequestException as e:

            log.warning(
                "%s request failed (%d/%d): %s",
                source,
                attempt,
                MAX_RETRIES,
                e,
            )

            if attempt == MAX_RETRIES:
                return None

            wait_time = min(2 ** attempt, MAX_RETRY_WAIT)

            log.info("Retrying in %s seconds...", wait_time)
            time.sleep(wait_time)

            continue

        # Success
        if response.status_code == 200:
            return response

        # Not found: no retry needed
        if response.status_code == 404:
            log.info("%s returned 404. Skipping.", url)
            return response

        # Rate limited
        if response.status_code == 429:

            retry_after = response.headers.get("Retry-After")

            wait_time = parse_retry_after(retry_after)

            if wait_time is None:
                wait_time = min(2 ** attempt * 10, MAX_RETRY_WAIT)

            # Avoid sleeping for excessively long server instructions.
            if wait_time > MAX_RETRY_WAIT:

                log.warning(
                    "%s requested a long wait (%s seconds). "
                    "Skipping this feed for this run.",
                    source,
                    wait_time,
                )

                return response

            if attempt == MAX_RETRIES:

                log.warning(
                    "%s still rate-limited after %d attempts. Skipping.",
                    source,
                    MAX_RETRIES,
                )

                return response

            log.warning(
                "%s rate limited (429). Waiting %s seconds. "
                "Attempt %d/%d",
                source,
                wait_time,
                attempt,
                MAX_RETRIES,
            )

            time.sleep(wait_time)
            continue

        # Retry temporary server errors
        if response.status_code in (500, 502, 503, 504):

            if attempt == MAX_RETRIES:
                log.warning(
                    "%s returned HTTP %s after retries.",
                    source,
                    response.status_code,
                )
                return response

            wait_time = min(2 ** attempt, MAX_RETRY_WAIT)

            log.warning(
                "%s returned HTTP %s. Retrying in %s seconds.",
                source,
                response.status_code,
                wait_time,
            )

            time.sleep(wait_time)
            continue

        # Other statuses: don't keep retrying
        log.warning(
            "%s returned HTTP %s. Skipping.",
            source,
            response.status_code,
        )

        return response

    return None


def parse_retry_after(value):

    if not value:
        return None

    # Retry-After can be a number of seconds
    try:
        return int(value)

    except ValueError:
        pass

  
    try:
        retry_date = parsedate_to_datetime(value)

        now = datetime.now(timezone.utc)

        if retry_date.tzinfo is None:
            retry_date = retry_date.replace(tzinfo=timezone.utc)

        return max(
            0,
            int((retry_date - now).total_seconds())
        )

    except (ValueError, TypeError, OverflowError):
        return None



# XML helpers

def safe_text(element, default="No Title"):

    if element is None or element.text is None:
        return default

    return element.text.strip()



#YouTube RSS
def fetch_youtube_feed(session, url):

    response = get_with_retry(
        session,
        url,
        source="YouTube",
    )

    if response is None or response.status_code != 200:
        return []

    try:
        root = ET.fromstring(response.content)

    except ET.ParseError as e:
        log.warning("YouTube XML parsing failed: %s", e)
        return []

    namespace = {
        "atom": "http://www.w3.org/2005/Atom"
    }

    items = []

    for entry in root.findall(".//atom:entry", namespace):

        title = safe_text(
            entry.find("atom:title", namespace)
        )

        link_element = entry.find(
            "atom:link",
            namespace
        )

        link = (
            link_element.attrib.get("href")
            if link_element is not None
            else None
        )

        pub_date = safe_text(
            entry.find("atom:published", namespace),
            default=None,
        )

        if not link:
            continue

        items.append({
            "title": title,
            "link": link,
            "pub_date": pub_date,
        })

    log.info("YouTube: found %d items", len(items))

    return items



# Medium RSS

def parse_medium_feed(content, url):

    try:
        root = ET.fromstring(content)

    except ET.ParseError as e:
        log.warning("RSS parsing failed for %s: %s", url, e)
        return []

    items = []

    # Standard RSS feeds
    channel = root.find("channel")

    if channel is not None:

        for item in channel.findall("item"):

            title = safe_text(
                item.find("title")
            )

            link = safe_text(
                item.find("link"),
                default=None,
            )

            pub_date = safe_text(
                item.find("pubDate"),
                default=None,
            )

            if not link:
                continue

            items.append({
                "title": title,
                "link": link,
                "pub_date": pub_date,
            })

    # Atom fallback
    else:

        namespace = {
            "atom": "http://www.w3.org/2005/Atom"
        }

        for entry in root.findall(
            ".//atom:entry",
            namespace
        ):

            title = safe_text(
                entry.find("atom:title", namespace)
            )

            link_element = entry.find(
                "atom:link",
                namespace
            )

            link = (
                link_element.attrib.get("href")
                if link_element is not None
                else None
            )

            pub_date = safe_text(
                entry.find("atom:published", namespace),
                default=None,
            )

            if not link:
                continue

            items.append({
                "title": title,
                "link": link,
                "pub_date": pub_date,
            })

    return items


def fetch_medium_writeups(session, file_path):

    if not os.path.exists(file_path):

        log.warning("Medium list file not found: %s", file_path)
        return []

    with open(file_path, "r", encoding="utf-8") as file:

        urls = [
            line.strip()
            for line in file
            if line.strip()
            and not line.strip().startswith("#")
        ]

    all_items = []

    log.info("Checking %d Medium feeds...", len(urls))

    for index, url in enumerate(urls, start=1):

        log.info(
            "Medium feed %d/%d",
            index,
            len(urls),
        )

        response = get_with_retry(
            session,
            url,
            source="Medium",
        )

        if response is not None and response.status_code == 200:

            items = parse_medium_feed(
                response.content,
                url,
            )

            all_items.extend(items)

            log.info(
                "Found %d items from %s",
                len(items),
                url,
            )

        else:

            status = (
                response.status_code
                if response is not None
                else "Request failed"
            )

            log.warning(
                "Skipping Medium feed %s (status: %s)",
                url,
                status,
            )

        # Be polite: don't hit Medium feeds back-to-back
        if index < len(urls):
            time.sleep(REQUEST_DELAY)

    log.info(
        "Medium: total items fetched: %d",
        len(all_items),
    )

    return all_items



# Pentester.land

def fetch_pentesterland_writeups(session):

    url = "https://pentester.land/writeups.json"

    response = get_with_retry(
        session,
        url,
        source="Pentester.land",
    )

    if response is None or response.status_code != 200:
        return []

    try:
        data = response.json()

    except ValueError as e:
        log.warning("Invalid Pentester.land JSON: %s", e)
        return []

    writeups = []

    for selected_item in data.get("data", []):

        links = selected_item.get("Links", [])

        for item in links:

            link = item.get("Link")
            title = item.get("Title", "No Title")

            if not link:
                continue

            writeups.append({
                "title": title,
                "link": link,
                "pub_date": selected_item.get("PublicationDate"),
                "authors": selected_item.get("Authors", []),
                "programs": selected_item.get("Programs", []),
                "bugs": selected_item.get("Bugs", []),
                "added_date": selected_item.get("AddedDate"),
            })

    log.info(
        "Pentester.land: found %d writeups",
        len(writeups),
    )

    return writeups



# Discord

def send_to_discord(
    webhook_url,
    title,
    link,
    pub_date,
    extra_fields=None,
):

    if not webhook_url:

        log.info("Discord webhook not configured. Skipping.")
        return False

    title = str(title or "Untitled")[:256]
    link = str(link or "")
    pub_date = str(pub_date or "Unknown")

    fields = [
        {
            "name": "Published on",
            "value": pub_date[:1024],
            "inline": False,
        }
    ]

    if extra_fields:

        for field in extra_fields:

            name = str(field.get("name", "Info"))[:256]
            value = str(field.get("value", "N/A"))[:1024]

            fields.append({
                "name": name,
                "value": value or "N/A",
                "inline": False,
            })

    # Discord embed maximum: 25 fields
    fields = fields[:25]

    embed = {
        "title": title,
        "description": f"[Read writeup]({link})"[:4096],
        "color": 16777215,
        "fields": fields,
    }

    payload = {
        "embeds": [embed]
    }

    try:

        response = requests.post(
            webhook_url,
            json=payload,
            timeout=TIMEOUT,
        )

        if response.status_code in (200, 204):

            log.info("Discord notification sent: %s", title)
            return True

        log.warning(
            "Discord returned HTTP %s",
            response.status_code,
        )

        return False

    except requests.RequestException as e:

        log.warning("Discord request failed: %s", e)
        return False



# Processing


def process_items(
    conn,
    table_name,
    items,
    nodiscord=False,
):

    processed = 0

    for item in items:

        link = item.get("link")
        title = item.get("title", "Untitled")
        pub_date = item.get("pub_date")

        if not link:
            continue

        if is_link_processed(conn, table_name, link):
            continue

        extra_fields = None

        if table_name == "pentesterland_writeups":

            extra_fields = [
                {
                    "name": "Authors",
                    "value": ", ".join(item.get("authors", [])) or "Unknown",
                },
                {
                    "name": "Programs",
                    "value": ", ".join(item.get("programs", [])) or "Unknown",
                },
                {
                    "name": "Bugs",
                    "value": ", ".join(item.get("bugs", [])) or "Unknown",
                },
                {
                    "name": "Added Date",
                    "value": str(item.get("added_date") or "Unknown"),
                },
            ]

        if not nodiscord:

            send_to_discord(
                WEB_HOOK,
                title,
                link,
                pub_date,
                extra_fields,
            )

        save_processed_link(
            conn,
            table_name,
            link,
            title,
            pub_date,
            authors=item.get("authors"),
            programs=item.get("programs"),
            bugs=item.get("bugs"),
            added_date=item.get("added_date"),
        )

        processed += 1

        log.info("Saved new item: %s", title)

    log.info(
        "%s: %d new items saved",
        table_name,
        processed,
    )



# Main
def main():

    import sys

    args = sys.argv[1:]

    if "db" in args:
        setup_database()
        return

    nodiscord = "nodiscord" in args

    conn = None
    session = create_session()

    try:

        conn = get_db_connection()

        # YouTube
        if os.path.exists(YOUTUBE_FILE):

            with open(
                YOUTUBE_FILE,
                "r",
                encoding="utf-8"
            ) as file:

                channels = [
                    line.strip()
                    for line in file
                    if line.strip()
                    and not line.strip().startswith("#")
                ]

            for line in channels:

                try:
                    channel_name, channel_id = line.split(",", 1)

                except ValueError:
                    log.warning("Invalid channel entry: %s", line)
                    continue

                channel_name = channel_name.strip()
                channel_id = channel_id.strip()

                rss_url = (
                    "https://www.youtube.com/feeds/videos.xml"
                    f"?channel_id={channel_id}"
                )

                log.info("Checking YouTube: %s", channel_name)

                videos = fetch_youtube_feed(
                    session,
                    rss_url,
                )

                process_items(
                    conn,
                    "youtube_videos",
                    videos,
                    nodiscord,
                )

        else:

            log.warning("YouTube channel file not found.")

        # Medium
        medium_items = fetch_medium_writeups(
            session,
            MEDIUM_FILE,
        )

        process_items(
            conn,
            "medium_writeups",
            medium_items,
            nodiscord,
        )

        # Pentester.land
        pentesterland_items = fetch_pentesterland_writeups(
            session,
        )

        process_items(
            conn,
            "pentesterland_writeups",
            pentesterland_items,
            nodiscord,
        )

        log.info("All sources processed successfully.")

    except psycopg2.Error as e:

        log.exception("Database error: %s", e)

    except Exception as e:

        log.exception("Unexpected error: %s", e)

    finally:

        if conn is not None:
            conn.close()

        session.close()


if __name__ == "__main__":
    main()
