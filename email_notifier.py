"""
Email notification functions for Walmart Leasing Checker
"""

import logging
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from config import EMAIL_SENDER, EMAIL_RECEIVER, EMAIL_PASSWORD

# Configure logging
logger = logging.getLogger(__name__)

def send_email(properties):
    """Send email notification about matching properties."""
    if not properties:
        logger.info("No properties to notify about")
        return
    
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECEIVER
        msg["Subject"] = f"Walmart Leasing Opportunities - {datetime.now().strftime('%Y-%m-%d')}"
        
        # Create HTML content with improved table structure
        html_content = f"""
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                }}
                table {{
                    border-collapse: collapse;
                    width: 100%;
                    margin-bottom: 20px;
                }}
                th, td {{
                    border: 1px solid #dddddd;
                    text-align: left;
                    padding: 8px;
                }}
                tr:nth-child(even) {{
                    background-color: #f2f2f2;
                }}
                th {{
                    background-color: #0071ce;
                    color: white;
                }}
                .check {{
                    color: green;
                    font-weight: bold;
                }}
                .x {{
                    color: red;
                    font-weight: bold;
                }}
                .note {{
                    font-size: 0.8em;
                    color: #666;
                }}
                .warning {{
                    color: #ff6600;
                    font-weight: bold;
                }}
                .space-list {{
                    margin: 0;
                    padding-left: 15px;
                }}
            </style>
        </head>
        <body>
            <h2>Walmart Leasing Opportunities</h2>
            <p>Found {len(properties)} locations matching your criteria:</p>
            <table>
                <tr>
                    <th>Store #</th>
                    <th>Address</th>
                    <th>City</th>
                    <th>ZIP</th>
                    <th>Available Spaces</th>
                    <th>Reviews</th>
                    <th>Status</th>
                </tr>
        """
        
        # Add a plain text version as well
        text_content = "Walmart Leasing Opportunities\n\n"
        text_content += f"Found {len(properties)} locations matching your criteria:\n\n"
        
        # Add each property to the email with improved space formatting
        for prop in properties:
            # Use the leasing site store ID as the primary ID
            store_id = prop.get("store_id", "Unknown")
            store_num = f"Store #{store_id}"
            
            # Original address from leasing site
            leasing_address = prop.get('address', "Unknown")
            
            # Use city and zip from Google data
            city = prop.get('city', "Unknown")
            zip_code = prop.get('zip_code', "Unknown")
            reviews = prop.get("review_count", "N/A")
            
            # Modified: Create a simple link to the Walmart website if available
            website = prop.get("website", "")
            website_html = f"<br><a href='{website}' target='_blank'>Website</a>" if website else ""
            
            # Create space details HTML - improved formatting with bullet points
            space_html = "<ul class='space-list'>"
            space_text = ""
            
            for space in sorted(prop.get("spaces", []), key=lambda x: x.get('sqft', 0)):
                suite = space.get("suite", "TBD")
                sqft = space.get("sqft", "Unknown")
                space_html += f"<li><strong>Suite {suite}</strong>: {sqft} sqft</li>"
                space_text += f"- Suite {suite}: {sqft} sqft\n"
            
            space_html += "</ul>"
            
            # All properties in the final list have been confirmed to NOT have mobile stores
            radius = prop.get('mobile_store_search_radius', '100m')
            mobile_store = f"No mobile stores detected within {radius} <span class='check'>&check;</span>"
            
            # Add to HTML content with improved layout
            html_content += f"""
                <tr>
                    <td><strong>{store_num}</strong>{website_html}</td>
                    <td>{leasing_address}</td>
                    <td>{city}</td>
                    <td>{zip_code}</td>
                    <td>{space_html}</td>
                    <td>{reviews:,}</td>
                    <td>{mobile_store}</td>
                </tr>
            """
            
            # Add to text content
            text_content += f"• {store_num} at {leasing_address} - {city}, {zip_code} - {reviews} reviews - No mobile store *\n"
            text_content += space_text
            text_content += "\n"
        
        # Close the HTML with better explanation of mobile store detection
        html_content += """
            </table>
            <p>This is an automated message from your Walmart Leasing Checker.</p>
            <p><strong>Note:</strong> All listings above have been verified to meet the following criteria:</p>
            <ul>
                <li>Available space under 1000 sqft</li>
                <li>Over 10,000 Google reviews</li>
                <li>No mobile phone repair stores present within 100 meters or at the same address as the Walmart</li>
                <li>Must have verified city and ZIP code information</li>
            </ul>
            <p><strong>How Mobile Store Detection Works:</strong> The system performs multiple checks for each Walmart location:</p>
            <ol>
                <li>It uses Google Maps' "Nearby" feature directly from the Walmart location page to search for "mobile phone repair"</li>
                <li>It performs separate searches for "cell phone repair near [address]" and "mobile repair store near [address]"</li>
                <li>It performs targeted searches for specific brands ("The Fix", "iFixAndRepair", etc.) + the Walmart address</li>
                <li>All search results are analyzed both for distance (within 150m) AND to detect if they're at the same address as the Walmart</li>
                <li>Any stores matching targeted keywords (The Fix, iFixAndRepair, Cellaris, Talk N Fix, Techy, etc.) are flagged</li>
            </ol>
            <p>This enhanced approach ensures we detect both standalone repair shops nearby AND services located within the Walmart itself.</p>
        </body>
        </html>
        """
        
        # Attach both text and HTML parts
        msg.attach(MIMEText(text_content, "plain"))
        msg.attach(MIMEText(html_content, "html"))
        
        # Send the email
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
            logger.info(f"Email sent successfully to {EMAIL_RECEIVER}")
            
    except Exception as e:
        logger.error(f"Error sending email: {str(e)}")
