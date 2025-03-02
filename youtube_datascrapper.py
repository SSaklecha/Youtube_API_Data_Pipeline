import json
import boto3
import requests
import sys
import time
import os
import argparse
import csv

# Attributes to extract from the snippet data
SNIPPET_ATTRS = [
    "title",
    "publishedAt",
    "channelId",
    "channelTitle",
    "categoryId"
]

# Characters that can cause CSV formatting issues
BAD_CHARS = ['\n', '"']

# Define the CSV header with the desired column order
CSV_HEADER = ["video_id"] + SNIPPET_ATTRS + [
    "trending_date", "tags", "view_count", "likes", "dislikes",
    "comment_count", "thumbnail_link", "comments_disabled",
    "ratings_disabled", "description"
]


def load_configuration(api_path, country_codes_path):
    """Load API key and list of country codes from files."""
    with open(api_path, 'r') as api_file:
        key = api_file.readline().strip()
    with open(country_codes_path, 'r') as code_file:
        codes = [line.strip() for line in code_file]
    return key, codes


def clean_value(val):
    """
    Remove problematic characters from the given value
    and wrap it in double quotes.
    """
    value_str = str(val)
    for char in BAD_CHARS:
        value_str = value_str.replace(char, "")
    return f'"{value_str}"'


def fetch_api_page(page_token, region_code):
    """
    Build the request URL for the YouTube API, make the HTTP GET call,
    and return the JSON response.
    """
    api_key = "AIzaSyDX-ZAFb4jfkd1OqWeEb5mneILwDVzqxog"
    url = (
        f"https://www.googleapis.com/youtube/v3/videos?"
        f"part=id,statistics,snippet{page_token}chart=mostPopular&"
        f"regionCode={region_code}&maxResults=50&key={api_key}"
    )
    response = requests.get(url)
    if response.status_code == 429:
        print("Exceeded request limits; please pause and try again later.")
        sys.exit()
    return response.json()


def merge_tags(tag_list):
    """Join tags with a pipe delimiter and sanitize the resulting string."""
    joined = "|".join(tag_list)
    return clean_value(joined)


def extract_video_info(videos):
    """
    Process a list of video items, extracting and sanitizing
    the necessary fields to build CSV rows.
    """
    rows = []
    for video in videos:
        # Skip videos without statistics (likely removed)
        if "statistics" not in video:
            continue

        video_id = clean_value(video['id'])
        snippet = video.get('snippet', {})
        stats = video.get('statistics', {})

        # Extract snippet values safely
        snippet_values = [clean_value(snippet.get(attr, "")) for attr in SNIPPET_ATTRS]

        # Additional details
        description = snippet.get("description", "")
        thumbnail = snippet.get("thumbnails", {}).get("default", {}).get("url", "")
        current_date = time.strftime("%y.%d.%m")
        tags = merge_tags(snippet.get("tags", ["[none]"]))
        view_count = stats.get("viewCount", 0)

        # Check if ratings and comments are enabled
        if 'likeCount' in stats and 'dislikeCount' in stats:
            likes = stats['likeCount']
            dislikes = stats['dislikeCount']
            ratings_disabled = False
        else:
            likes, dislikes, ratings_disabled = 0, 0, True

        if 'commentCount' in stats:
            comment_count = stats['commentCount']
            comments_disabled = False
        else:
            comment_count, comments_disabled = 0, True

        # Compile all data into a CSV row
        row = [video_id] + snippet_values + [
            clean_value(current_date),
            tags,
            clean_value(view_count),
            clean_value(likes),
            clean_value(dislikes),
            clean_value(comment_count),
            clean_value(thumbnail),
            clean_value(comments_disabled),
            clean_value(ratings_disabled),
            clean_value(description)
        ]
        rows.append(",".join(row))
    return rows


def collect_region_pages(region, start_token="&"):
    """
    Iterate through paginated API results for a given region,
    compiling all the video information.
    """
    all_rows = []
    next_token = start_token
    while next_token is not None:
        page_data = fetch_api_page(next_token, region)
        token_value = page_data.get("nextPageToken")
        next_token = f"&pageToken={token_value}&" if token_value is not None else None

        items = page_data.get("items", [])
        all_rows.extend(extract_video_info(items))
    return all_rows


def upload_csv_to_s3(region, csv_rows):
    """
    Write the CSV rows to a local file, adjust the column order,
    and upload the final file to an S3 bucket.
    """
    print(f"Uploading data for region {region}...")
    timestamp = time.strftime('%y%m%d%H%M%S')
    filename = f"{timestamp}_{region}_videos.csv"
    local_file = os.path.join("/tmp", filename)
    s3_key = f"youtube/raw_statistics/region={region.lower()}/{filename}"
    s3_bucket = "youtube-project-analytics-raw-useast1-dev"

    # Write raw CSV data to a local file
    with open(local_file, "w", encoding='utf-8') as f:
        for line in csv_rows:
            f.write(f"{line}\n")

    # Rearrange CSV columns per the desired order
    input_csv = local_file
    output_csv = os.path.join("/tmp", "reformatted_" + filename)
    desired_columns = [
        "video_id",
        "trending_date",
        "title",
        "channel_title",
        "category_id",
        "publish_time",
        "tags",
        "views",
        "likes",
        "dislikes",
        "comment_count",
        "thumbnail_link",
        "comments_disabled",
        "ratings_disabled",
        "video_error_or_removed",
        "description"
    ]

    with open(input_csv, mode='r', newline='', encoding='utf-8') as infile, \
         open(output_csv, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=desired_columns)
        writer.writeheader()
        for row in reader:
            adjusted_row = {
                "video_id": row.get("video_id", ""),
                "trending_date": row.get("trending_date", ""),
                "title": row.get("title", ""),
                "channel_title": row.get("channelTitle", ""),
                "category_id": row.get("categoryId", ""),
                "publish_time": row.get("publishedAt", ""),
                "tags": row.get("tags", ""),
                "views": row.get("view_count", ""),
                "likes": row.get("likes", ""),
                "dislikes": row.get("dislikes", ""),
                "comment_count": row.get("comment_count", ""),
                "thumbnail_link": row.get("thumbnail_link", ""),
                "comments_disabled": row.get("comments_disabled", ""),
                "ratings_disabled": row.get("ratings_disabled", ""),
                "video_error_or_removed": row.get("video_error_or_removed", ""),
                "description": row.get("description", "")
            }
            writer.writerow(adjusted_row)

    # Upload the reformatted CSV to S3
    s3_client = boto3.client('s3')
    s3_client.upload_file(output_csv, s3_bucket, s3_key)


def process_region_data():
    """
    For each region, compile the CSV data and
    save it locally before uploading to S3.
    """
    regions = ["US", "CA", "IN"]
    for reg in regions:
        csv_content = [",".join(CSV_HEADER)] + collect_region_pages(reg)
        upload_csv_to_s3(reg, csv_content)


def lambda_handler(event, context):
    parser = argparse.ArgumentParser()
    # Uncomment the following lines if you want to provide custom file paths:
    # parser.add_argument('--api_file', default='api_key.txt', help='Path to the API key file')
    # parser.add_argument('--countries_file', default='country_codes.txt', help='Path to the country codes file')
    args = parser.parse_args()
    
    process_region_data()
