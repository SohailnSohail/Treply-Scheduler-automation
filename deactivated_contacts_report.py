#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from rich.console import Console

console = Console()

def get_mongodb_connection(uri_key):
    load_dotenv()
    mongo_uri = os.getenv(uri_key)
    if not mongo_uri:
        console.print(f"[red]{uri_key} is not set in .env[/red]")
        return None
    try:
        client = MongoClient(mongo_uri)
        client.admin.command('ping')
        console.print(f"[green]Connected to MongoDB using {uri_key}[/green]")
        return client
    except Exception as e:
        console.print(f"[red]Connection failed: {e}[/red]")
        return None

def transfer_undelivered_report_to_dev():
    prod_client = get_mongodb_connection("PROD_MONGO_URI")
    dev_client = get_mongodb_connection("DEV_MONGO_URI")
    if not prod_client or not dev_client:
        return

    prod_db = prod_client["treply"]
    messages = prod_db["test_twilio_messages"]

    dev_db = dev_client["treply_dev"]
    dev_collection = dev_db["deactivated_phone_report"]  # New collection

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
                    {"date_sent": {"$exists": True, "$ne": None}}
                ]
            }
        },
        {
            "$addFields": {
                "date_sent_obj": {
                    "$cond": {
                        "if": { "$eq": [{ "$type": "$date_sent" }, "date"] },
                        "then": "$date_sent",
                        "else": { "$dateFromString": { "dateString": "$date_sent" } }
                    }
                }
            }
        },
        {
            "$sort": {
                "to": 1,
                "date_sent_obj": -1
            }
        },
        {
            "$group": {
                "_id": "$to",
                "latest_date_sent": { "$first": "$date_sent" },
                "organizationId": { "$first": "$organizationId" },
                "channelId": { "$first": "$channelId" },
                "error_code": { "$first": "$error_code" }
            }
        },
        {
            "$lookup": {
                "from": "organization_channel_report_bak",
                "let": {
                    "orgId": "$organizationId",
                    "chanId": "$channelId"
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    { "$eq": ["$organizationId", "$$orgId"] },
                                    { "$eq": ["$channelId", "$$chanId"] }
                                ]
                            }
                        }
                    },
                    { "$limit": 1 },
                    {
                        "$project": {
                            "organizationName": 1,
                            "channelName": 1,
                            "_id": 0
                        }
                    }
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
                "to": "$_id",
                "date_sent": "$latest_date_sent",
                "organizationId": 1,
                "channelId": 1,
                "error_code": 1,
                "organizationName": { "$ifNull": ["$org_info.organizationName", ""] },
                "channelName": { "$ifNull": ["$org_info.channelName", ""] }
            }
        }
    ]

    try:
        results = list(messages.aggregate(pipeline))
        if results:
            dev_collection.insert_many(results)
            console.print(f"[green]Inserted {len(results)} documents into 'deactivated_phone_report' collection in dev DB[/green]")
        else:
            console.print("[yellow]No matching undelivered messages found.[/yellow]")

    except Exception as e:
        console.print(f"[red]Error during aggregation/insertion: {e}[/red]")
    finally:
        prod_client.close()
        dev_client.close()

def main():
    console.print("[bold blue]Transferring Undelivered Phone Numbers Report to Dev DB[/bold blue]")
    transfer_undelivered_report_to_dev()

if __name__ == "__main__":
    main()
