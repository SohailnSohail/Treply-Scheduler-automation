#!/usr/bin/env python3
import os
import sys
import argparse
from datetime import datetime, timezone
from pymongo import MongoClient
from bson import ObjectId
from rich.console import Console
from rich.table import Table
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def generate_contact_analysis_report(organization_id, output_file=None, save_to_mongodb=True):
    """Generate a report analyzing contacts in an organization's groups.
    
    Args:
        organization_id (str): The ID of the organization to analyze
        output_file (str, optional): Path to save the detailed report
        save_to_mongodb (bool): Whether to save results to MongoDB
    """
    console = Console()
    
    # Initialize MongoDB connection using common utils function
    try:
        console.print("[cyan]Connecting to MongoDB...[/cyan]")
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
        from src.utils.common import get_mongodb_connection
        
        client = get_mongodb_connection()
        if not client:
            console.print("[red]Failed to connect to MongoDB[/red]")
            return
            
        db = client[client.default_db_name]
        console.print(f"[cyan]Using database: {client.default_db_name}[/cyan]")
    except Exception as e:
        console.print(f"[red]Error connecting to MongoDB: {str(e)}[/red]")
        return

    # Get organization details
    try:
        # Debug: Show collections
        # collections = db.list_collection_names()
        # console.print(f"[cyan]Available collections: {collections}[/cyan]")
        
        # Convert string ID to ObjectId
        org_id = ObjectId(organization_id)
        console.print(f"[cyan]Looking for organization with ID: {org_id} (type: {type(org_id)})[/cyan]")
        
        # Debug: Show total organizations
        total_orgs = db.organizations.count_documents({})
        console.print(f"[cyan]Total organizations in database: {total_orgs}[/cyan]")
        
        # Get organization
        org = db.organizations.find_one({"_id": org_id})
        # console.print(f"[cyan]Organization found: {org}[/cyan]")
        if not org:
            console.print(f"[red]Organization not found with ID: {organization_id}[/red]")
            # Debug: Show a sample organization
            if total_orgs > 0:
                sample_org = db.organizations.find_one({})
                console.print(f"[cyan]Sample organization structure: {sample_org}[/cyan]")
            return
        
        org_name = org.get("legalEntityName", "Unknown")
        console.print(f"[green]Found organization: {org_name}[/green]")
    except Exception as e:
        console.print(f"[red]Error fetching organization: {str(e)}[/red]")
        return

    # Create report tables
    group_table = Table(title=f"Contact Group Analysis for {org_name}", show_header=True, header_style="bold magenta")
    group_table.add_column("Group Name")
    group_table.add_column("Total Contacts", justify="right")
    group_table.add_column("Active", justify="right")
    group_table.add_column("Unsubscribed", justify="right")
    group_table.add_column("Undeliverable", justify="right")
    group_table.add_column("Error Rate", justify="right")

    error_table = Table(title="Error Analysis", show_header=True, header_style="bold magenta")
    error_table.add_column("Error Type")
    error_table.add_column("Count", justify="right")
    error_table.add_column("Description")

    detailed_report = []
    total_stats = {
        "total_contacts": 0,
        "active_contacts": 0,
        "unsubscribed": 0,
        "undeliverable": 0,
        "errors": {}
    }

    # Get all contact groups for the organization
    try:
        # Query for contact groups with both string and ObjectId organizationId
        query = {
            "$or": [
                {"organizationId": org_id},
                {"organizationId": str(org_id)}
            ],
            "active": True
        }
        console.print(f"[cyan]Searching for contact groups with query: {query}[/cyan]")
        
        # First check total count of contact groups
        # total_groups = db.contactgroups.count_documents({})
        # console.print(f"[cyan]Total contact groups in database: {total_groups}[/cyan]")
        
        # Get matching groups
        groups = list(db.contactgroups.find(query))
        console.print(f"[cyan]Found {len(groups)} active contact groups for organization[/cyan]")
        
        # Debug: Show sample of groups if any exist
        # if total_groups > 0:
        #     sample_group = db.contactgroups.find_one({})
        #     console.print(f"[cyan]Sample group structure: {sample_group}[/cyan]")

        for group in groups:
            group_stats = {
                "name": group["name"],
                "total_contacts": 0,
                "active_contacts": 0,
                "unsubscribed": 0,
                "undeliverable": 0,
                "errors": {},
                "contacts": []
            }

            # Get all contacts in the group
            group_contacts = list(db.contactgroups_mappings.find({"groupId": str(group["_id"]), "active": True}))
            console.print(f"[cyan]Found {len(group_contacts)} contacts in group: {group['name']}[/cyan]")
            # Convert string IDs to ObjectIds
            contact_ids = [ObjectId(mapping["contactId"]) for mapping in group_contacts]
            
            # Get contact details
            contacts = list(db.contacts.find({"_id": {"$in": contact_ids}}))
            group_stats["total_contacts"] = len(contacts)
            total_stats["total_contacts"] += len(contacts)

            for contact in contacts:
                contact_detail = {
                    "id": str(contact["_id"]),
                    "name": f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip(),
                    "phone": contact.get("phoneNumber", ""),
                    "status": "active",
                    "errors": []
                }

                # Check if unsubscribed
                unsubscribed = db.unsubscribed_contacts.find_one({
                    "$or": [
                        {"contactId": str(contact["_id"])},
                        {"contactId": contact["_id"]}
                    ],
                    "channelId": {"$exists": True}
                })
                if unsubscribed:
                    contact_detail["status"] = "unsubscribed"
                    group_stats["unsubscribed"] += 1
                    total_stats["unsubscribed"] += 1
                    continue

                # Check for delivery issues
                invalid_contact = db.invalid_contacts.find_one({
                    "$or": [
                        {"contactId": str(contact["_id"])},
                        {"contactId": contact["_id"]}
                    ]
                })
                if invalid_contact:
                    contact_detail["status"] = "undeliverable"
                    contact_detail["errors"] = invalid_contact.get("errorMessages", [])
                    group_stats["undeliverable"] += 1
                    total_stats["undeliverable"] += 1

                    # Track error types
                    for error in invalid_contact.get("errorDetails", []):
                        error_code = error.get("code", "unknown")
                        error_desc = error.get("description", "Unknown error")
                        group_stats["errors"][error_code] = group_stats["errors"].get(error_code, 0) + 1
                        total_stats["errors"][error_code] = total_stats["errors"].get(error_code, 0) + 1
                else:
                    group_stats["active_contacts"] += 1
                    total_stats["active_contacts"] += 1

                group_stats["contacts"].append(contact_detail)

            # Calculate error rate
            error_rate = (group_stats["undeliverable"] / group_stats["total_contacts"] * 100) if group_stats["total_contacts"] > 0 else 0

            # Add to group table
            group_table.add_row(
                group_stats["name"],
                str(group_stats["total_contacts"]),
                str(group_stats["active_contacts"]),
                str(group_stats["unsubscribed"]),
                str(group_stats["undeliverable"]),
                f"{error_rate:.1f}%"
            )

            detailed_report.append(group_stats)

        # Add totals row
        total_error_rate = (total_stats["undeliverable"] / total_stats["total_contacts"] * 100) if total_stats["total_contacts"] > 0 else 0
        group_table.add_row(
            "[bold yellow]TOTAL[/bold yellow]",
            f"[bold yellow]{total_stats['total_contacts']}[/bold yellow]",
            f"[bold yellow]{total_stats['active_contacts']}[/bold yellow]",
            f"[bold yellow]{total_stats['unsubscribed']}[/bold yellow]",
            f"[bold yellow]{total_stats['undeliverable']}[/bold yellow]",
            f"[bold yellow]{total_error_rate:.1f}%[/bold yellow]"
        )

        # Add error analysis
        for error_code, count in total_stats["errors"].items():
            error_desc = next((err["description"] for group in detailed_report 
                             for contact in group["contacts"] 
                             for err in contact.get("errors", []) 
                             if str(err.get("code")) == str(error_code)), "Unknown error")
            error_table.add_row(
                str(error_code),
                str(count),
                error_desc
            )

        # Print tables
        console.print("\n")
        console.print(group_table)
        console.print("\n")
        console.print(error_table)

        console.print("\n[green]Report generated successfully[/green]")
        console.print("\n")
        console.print("[bold]Summary[/bold]")
        console.print(f"Total Contacts: {total_stats['total_contacts']}")
        console.print(f"Active Contacts: {total_stats['active_contacts']}")
        console.print(f"Unsubscribed Contacts: {total_stats['unsubscribed']}")      
        console.print(f"Undeliverable Contacts: {total_stats['undeliverable']}")
        console.print(f"Overall Error Rate: {total_error_rate:.1f}%")
        console.print("\n")
        console.print("[bold]Saving to mongodb[/bold]")

        # Save to MongoDB
        if save_to_mongodb:
            try:
                # Prepare data for MongoDB
                now = datetime.now(timezone.utc)
                mongodb_data = {
                    "organizationId": organization_id,
                    "organizationName": org_name,
                    "reportType": "contact_analysis",
                    "generatedAt": now,
                    "summary": {
                        "totalContacts": total_stats["total_contacts"],
                        "activeContacts": total_stats["active_contacts"],
                        "unsubscribedContacts": total_stats["unsubscribed"],
                        "undeliverableContacts": total_stats["undeliverable"],
                        "errorRate": total_error_rate
                    },
                    "errorAnalysis": [
                        {
                            "errorCode": error_code,
                            "count": count,
                            "description": next((err["description"] for group in detailed_report 
                                              for contact in group["contacts"] 
                                              for err in contact.get("errors", []) 
                                              if str(err.get("code")) == str(error_code)), "Unknown error")
                        } for error_code, count in total_stats["errors"].items()
                    ],
                    "groupDetails": [
                        {
                            "name": group["name"],
                            "totalContacts": group["total_contacts"],
                            "activeContacts": group["active_contacts"],
                            "unsubscribed": group["unsubscribed"],
                            "undeliverable": group["undeliverable"],
                            "errorRate": (group["undeliverable"] / group["total_contacts"] * 100) if group["total_contacts"] > 0 else 0,
                            "contactsWithIssues": [
                                {
                                    "id": contact["id"],
                                    "name": contact["name"],
                                    "phone": contact["phone"],
                                    "status": contact["status"],
                                    "errors": contact["errors"]
                                } for contact in group["contacts"] if contact["status"] != "active"
                            ]
                        } for group in detailed_report
                    ]
                }
                
                # Insert into MongoDB
                # Convert string organization_id to ObjectId
                mongodb_data["organizationId"] = org_id
                result = db.contacts_analysis_report.insert_one(mongodb_data)
                console.print(f"[green]Report saved to MongoDB with ID: {result.inserted_id}[/green]")
            except Exception as e:
                console.print(f"[red]Error saving to MongoDB: {str(e)}[/red]")

        # Save detailed report if requested
        if output_file:
            try:
                with open(output_file, 'w') as f:
                    f.write(f"# Contact Analysis Report for {org_name}\n\n")
                    f.write(f"Generated at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n")
                    
                    f.write("## Summary\n")
                    f.write(f"Total Contacts: {total_stats['total_contacts']}\n")
                    f.write(f"Active Contacts: {total_stats['active_contacts']}\n")
                    f.write(f"Unsubscribed Contacts: {total_stats['unsubscribed']}\n")
                    f.write(f"Undeliverable Contacts: {total_stats['undeliverable']}\n")
                    f.write(f"Overall Error Rate: {total_error_rate:.1f}%\n\n")
                    
                    f.write("## Error Analysis\n")
                    for error_code, count in total_stats["errors"].items():
                        error_desc = next((err["description"] for group in detailed_report 
                                         for contact in group["contacts"] 
                                         for err in contact.get("errors", []) 
                                         if str(err.get("code")) == str(error_code)), "Unknown error")
                        f.write(f"Error {error_code}: {count} occurrences - {error_desc}\n")
                    f.write("\n")
                    
                    f.write("## Group Details\n")
                    for group in detailed_report:
                        f.write(f"\n### {group['name']}\n")
                        f.write(f"Total Contacts: {group['total_contacts']}\n")
                        f.write(f"Active Contacts: {group['active_contacts']}\n")
                        f.write(f"Unsubscribed: {group['unsubscribed']}\n")
                        f.write(f"Undeliverable: {group['undeliverable']}\n")
                        error_rate = (group['undeliverable'] / group['total_contacts'] * 100) if group['total_contacts'] > 0 else 0
                        f.write(f"Error Rate: {error_rate:.1f}%\n\n")
                        
                        f.write("#### Contact Details\n")
                        for contact in group["contacts"]:
                            f.write(f"- {contact['name']} ({contact['phone']}): {contact['status'].upper()}\n")
                            if contact["errors"]:
                                f.write("  Errors:\n")
                                for error in contact["errors"]:
                                    f.write(f"  - {error}\n")
                
                console.print(f"[green]Detailed report saved to {output_file}[/green]")
            except Exception as e:
                console.print(f"[red]Error saving report to file: {str(e)}[/red]")

    except Exception as e:
        console.print(f"[red]Error generating report: {str(e)}[/red]")
        return

def main():
    parser = argparse.ArgumentParser(description='Generate contact analysis report for an organization')
    parser.add_argument('--organization-id', required=True, help='Organization ID')
    parser.add_argument('--output', type=str, help='Output file for detailed report')
    parser.add_argument('--no-mongodb', action='store_true', help='Skip saving to MongoDB')
    
    args = parser.parse_args()
    
    generate_contact_analysis_report(
        organization_id=args.organization_id,
        output_file=args.output,
        save_to_mongodb=not args.no_mongodb
    )

if __name__ == "__main__":
    main()
