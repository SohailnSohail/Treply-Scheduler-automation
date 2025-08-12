#!/usr/bin/env python3
# Import required libraries
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pymongo import MongoClient
from rich.console import Console

# Initialize console for pretty printing
console = Console()

def get_mongodb_connection(env_var):
    """
    Establishes connection to a MongoDB database using the specified environment variable
    Args:
        env_var (str): Name of the environment variable containing the Mongo URI
    Returns:
        MongoClient: MongoDB client object if connection successful, None otherwise
    """
    load_dotenv()
    mongo_uri = os.getenv(env_var)

    if not mongo_uri:
        console.print(f"[red]{env_var} is not set in .env[/red]")
        return None

    try:
        client = MongoClient(mongo_uri)
        client.admin.command('ping')
        console.print(f"[green]Connected to MongoDB using {env_var}[/green]")
        return client
    except Exception as e:
        console.print(f"[red]Connection failed: {e}[/red]")
        return None

def process_and_write_undelivered_report_to_dev():
    """
    Processes undelivered message data from prod and inserts the summary into dev database
    """
    # Connect to production and development databases
    prod_client = get_mongodb_connection("PROD_MONGO_URI")
    dev_client = get_mongodb_connection("DEV_MONGO_URI")

    if not prod_client or not dev_client:
        return

    prod_db = prod_client["treply"]
    dev_db = dev_client["treply_dev"]
    messages = prod_db["test_twilio_messages"]
    report_collection = dev_db["daily_undelivered_reports"]

    yesterday = datetime.now() - timedelta(days=1)
    # yesterday = datetime(2025, 3, 30)  # Optional manual override

    yesterday_start = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)
    yesterday_end = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59, 999999)

    pipeline = [
        {
            "$match": {
                "$and": [
                    {"status": "undelivered"},
                    {"direction": "outbound-api"},
                    {"error_code": {"$exists": True, "$ne": None}},
                    {"organizationId": {"$exists": True, "$ne": None}},
                    {"channelId": {"$exists": True, "$ne": None}},
                    {"to": {"$exists": True, "$ne": None}},
                    {
                        "$or": [
                            {"date_sent": {"$gte": yesterday_start, "$lte": yesterday_end}},
                            {"date_sent": {"$regex": f"^{yesterday.strftime('%Y-%m-%d')}"}}
                        ]
                    }
                ]
            }
        },
        {
            "$addFields": {
                "date_sent_obj": {
                    "$cond": {
                        "if": {"$eq": [{"$type": "$date_sent"}, "date"]},
                        "then": "$date_sent",
                        "else": {"$dateFromString": {"dateString": "$date_sent"}}
                    }
                }
            }
        },
        {
            "$sort": {
                "to": 1,
                "organizationId": 1,
                "channelId": 1,
                "date_sent_obj": -1,
                "_id": 1
            }
        },
        {
            "$group": {
                "_id": {
                    "to": "$to",
                    "organizationId": "$organizationId",
                    "channelId": "$channelId"
                },
                "date_sent": {"$first": "$date_sent"},
                "error_code": {"$first": "$error_code"},
                "error_count": {"$sum": 1}
            }
        },
        {
            "$lookup": {
                "from": "organization_channel_report_bak",
                "let": {
                    "orgId": "$_id.organizationId",
                    "chanId": "$_id.channelId"
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$organizationId", "$$orgId"]},
                                    {"$eq": ["$channelId", "$$chanId"]}
                                ]
                            }
                        }
                    },
                    {"$limit": 1},
                    {"$project": {"organizationName": 1, "channelName": 1, "_id": 0}}
                ],
                "as": "org_info"
            }
        },
        {
            "$unwind": {
                "path": "$org_info",
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$project": {
                "_id": 0,
                "to": "$_id.to",
                "date_sent": 1,
                "organizationId": "$_id.organizationId",
                "channelId": "$_id.channelId",
                "error_code": 1,
                "error_count": 1,
                "organizationName": {"$ifNull": ["$org_info.organizationName", ""]},
                "channelName": {"$ifNull": ["$org_info.channelName", ""]}
            }
        },
        {
            "$sort": {"date_sent": -1}
        }
    ]

    try:
        results = list(messages.aggregate(pipeline))
        if results:
            for doc in results:
                doc["report_date"] = yesterday.strftime("%Y-%m-%d")
            inserted = report_collection.insert_many(results)
            console.print(f"[green]Inserted {len(inserted.inserted_ids)} documents into 'daily_undelivered_reports'[/green]")
        else:
            console.print("[yellow]No undelivered records found for yesterday.[/yellow]")
    except Exception as e:
        console.print(f"[red]Error during aggregation or insertion: {e}[/red]")
    finally:
        prod_client.close()
        dev_client.close()

def main():
    console.print("[bold blue]Writing Undelivered Phone Numbers Report to Dev DB[/bold blue]")
    process_and_write_undelivered_report_to_dev()

if __name__ == "__main__":
    main()
