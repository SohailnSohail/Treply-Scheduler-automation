#!/usr/bin/env python3
import os
import csv
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from rich.console import Console
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition
import base64

# Initialize console for output
console = Console()

def get_mongodb_connection():
    load_dotenv()
    mongo_uri = os.getenv("PROD_MONGO_URI")
    if not mongo_uri:
        console.print("[red]PROD_MONGO_URI is not set in .env[/red]")
        return None
    try:
        client = MongoClient(mongo_uri)
        client.admin.command('ping')
        console.print("[green]Connected to MongoDB successfully[/green]")
        return client
    except Exception as e:
        console.print(f"[red]MongoDB connection failed: {e}[/red]")
        return None

def generate_html_table(campaigns):
    """Create an HTML table string from campaign list of dicts"""
    if not campaigns:
        return "<p>No campaigns created today.</p>"

    html = """
    <h2>Campaign Report for Today</h2>
    <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse; font-family: Arial, sans-serif;">
      <thead>
        <tr style="background-color: #f2f2f2;">
          <th>Name</th>
          <th>Status</th>
          <th>Created At (UTC)</th>
        </tr>
      </thead>
      <tbody>
    """
    for campaign in campaigns:
        html += f"""
        <tr>
          <td>{campaign['name']}</td>
          <td>{campaign['status']}</td>
          <td>{campaign['createdAt']}</td>
        </tr>
        """
    html += """
      </tbody>
    </table>
    """
    return html

def generate_campaign_report():
    client = get_mongodb_connection()
    if not client:
        return None

    try:
        db = client["treply"]
        campaigns_collection = db["campaigns"]

        utc_today_str = datetime.utcnow().strftime('%Y-%m-%d')
        console.print(f"[blue]Looking for campaigns created on: {utc_today_str} (UTC)[/blue]")

        pipeline = [
            {
                "$addFields": {
                    "createdDate": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$createdAt"
                        }
                    }
                }
            },
            {"$match": {"createdDate": utc_today_str}},
            {"$project": {"_id": 0, "name": 1, "status": 1, "createdAt": 1}}
        ]

        results = list(campaigns_collection.aggregate(pipeline))

        for campaign in results:
            if isinstance(campaign.get('createdAt'), datetime):
                campaign['createdAt'] = campaign['createdAt'].strftime('%Y-%m-%d %H:%M:%S')

        output_file = f"campaign_report_{utc_today_str}.csv"
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = ['name', 'status', 'createdAt']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

        console.print(f"[green]Report generated: {output_file}[/green]")
        console.print(f"[green]Total campaigns created today: {len(results)}[/green]")

        if results:
            console.print("[yellow]Campaigns created today:[/yellow]")
            for c in results:
                console.print(f"- {c['name']}")
        else:
            console.print("[red]No campaigns created today.[/red]")

        return output_file, results

    except Exception as e:
        console.print(f"[red]Error generating report: {e}[/red]")
        return None

    finally:
        if client:
            client.close()

def send_email_report_sendgrid(report_path, campaigns):
    load_dotenv()
    api_key = os.getenv("SENDGRID_API_KEY")
    sender_email = os.getenv("EMAIL_SENDER")
    sender_name = os.getenv("EMAIL_SENDER_NAME", "Campaign Bot")
    recipient_email = os.getenv("EMAIL_RECIPIENT")

    if not all([api_key, sender_email, recipient_email]):
        console.print("[red]Missing SendGrid email environment variables[/red]")
        return False

    try:
        with open(report_path, 'rb') as f:
            data = f.read()
            encoded_file = base64.b64encode(data).decode()

        html_content = generate_html_table(campaigns)

        message = Mail(
            from_email=(sender_email, sender_name),
            to_emails=recipient_email,
            subject=f'Daily Campaign Report - {datetime.utcnow().strftime("%Y-%m-%d")}',
            html_content=html_content
        )

        attachment = Attachment(
            FileContent(encoded_file),
            FileName(os.path.basename(report_path)),
            FileType('text/csv'),
            Disposition('attachment')
        )
        message.attachment = attachment

        sg = SendGridAPIClient(api_key)
        response = sg.send(message)

        if 200 <= response.status_code < 300:
            console.print("[green]Email sent successfully via SendGrid[/green]")
            return True
        else:
            console.print(f"[red]Failed to send email: {response.status_code}[/red]")
            return False

    except Exception as e:
        console.print(f"[red]Exception sending email: {e}[/red]")
        return False

def main():
    console.print("[bold blue]Campaign Report Generator[/bold blue]")
    result = generate_campaign_report()
    if result is None:
        console.print("[red]Failed to generate campaign report[/red]")
        return

    file_path, campaigns = result
    if file_path:
        console.print(f"[green]Report saved at: {file_path}[/green]")
        send_email_report_sendgrid(file_path, campaigns)
    else:
        console.print("[red]No report file to send[/red]")

if __name__ == "__main__":
    main()
