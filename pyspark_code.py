import sys
from awsglue.transforms import ApplyMapping, ResolveChoice, DropNullFields
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Retrieve job parameters
job_args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
spark_ctx = SparkContext()
glue_ctx = GlueContext(spark_ctx)
spark_session = glue_ctx.spark_session
glue_job = Job(glue_ctx)
glue_job.init(job_args['JOB_NAME'], job_args)

# Define a filter to only process selected regions
region_predicate = "region in ('ca','gb','us')"

# Read the raw YouTube statistics from the Glue catalog with a pushdown predicate
source_frame = glue_ctx.create_dynamic_frame.from_catalog(
    database="db_youtube_raw",
    table_name="raw_statistics",
    transformation_ctx="source_frame",
    push_down_predicate=region_predicate
)

# Map the source columns to the target schema
mapped_frame = ApplyMapping.apply(
    frame=source_frame,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "long"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "long", "views", "long"),
        ("likes", "long", "likes", "long"),
        ("dislikes", "long", "dislikes", "long"),
        ("comment_count", "long", "comment_count", "long"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string")
    ],
    transformation_ctx="mapped_frame"
)

# Resolve ambiguous column types by creating structured types
resolved_frame = ResolveChoice.apply(
    frame=mapped_frame,
    choice="make_struct",
    transformation_ctx="resolved_frame"
)

# Remove any null fields from the dataset
clean_frame = DropNullFields.apply(
    frame=resolved_frame,
    transformation_ctx="clean_frame"
)

# Coalesce the DataFrame into a single partition and convert back to a DynamicFrame
coalesced_df = clean_frame.toDF().coalesce(1)
final_frame = DynamicFrame.fromDF(coalesced_df, glue_ctx, "final_frame")

# Write the processed data to S3 in Parquet format, partitioned by region
glue_ctx.write_dynamic_frame.from_options(
    frame=final_frame,
    connection_type="s3",
    connection_options={
        "path": "s3://de-on-youtube-cleansed-useast1-dev/youtube/raw_statistics/",
        "partitionKeys": ["region"]
    },
    format="parquet",
    transformation_ctx="write_output"
)

# Commit the Glue job
glue_job.commit()