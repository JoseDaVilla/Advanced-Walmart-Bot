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
        
        # Create HTML content
        html_content = f"""
        <html>
        <head>
            <style>
                table {{
                    border-collapse: collapse;
                    width: 100%;
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
                    <th>Spaces</th>
                    <th>Reviews</th>
                    <th>Mobile Store</th>
                </tr>
        """
        
        # Add a plain text version as well
        text_content = "Walmart Leasing Opportunities\n\n"
        text_content += f"Found {len(properties)} locations matching your criteria:\n\n"
        
        # Add each property to the email
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
            
            # Create space details HTML
            space_html = "<ul>"
            space_text = ""
            
            for space in prop.get("spaces", []):
                suite = space.get("suite", "TBD")
                sqft = space.get("sqft", "Unknown")
                space_html += f"<li>Suite {suite}: {sqft} sqft</li>"
                space_text += f"- Suite {suite}: {sqft} sqft\n"
            
            space_html += "</ul>"
            
            # All properties in the final list have been confirmed to NOT have mobile stores
            radius = prop.get('mobile_store_search_radius', '100m')
            mobile_store = f"No mobile stores detected within {radius} <span class='check'>&check;</span>"
            
            # Add to HTML content - Removed website ID display
            html_content += f"""
                <tr>
                    <td>{store_num}{website_html}</td>
                    <td>{leasing_address}</td>
                    <td>{city}</td>
                    <td>{zip_code}</td>
                    <td>{space_html}</td>
                    <td>{reviews}</td>
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
                <li>No mobile phone repair stores present (checked with Google Maps)</li>
                <li>Must have verified city and ZIP code information</li>
            </ul>
            <p><strong>How Mobile Store Detection Works:</strong> The system performs two searches for each Walmart location:</p>
            <ol>
                <li>First, it uses Google Maps' "Nearby" feature directly from the Walmart location page to search for "mobile phone repair" within 100 meters</li>
                <li>Then, it performs separate searches for "cell phone repair near [address]" and "mobile repair store near [address]" as a backup</li>
                <li>All search results within 100 meters (expanded to 150m for safety) are analyzed for mobile repair keywords</li>
                <li>Any stores matching keywords like "The Fix", "CPR", "Cellaris", etc. will cause the location to be filtered out</li>
            </ol>
            <p>This multi-stage approach ensures we detect both branded stores (like "The Fix") and small independent repair shops near Walmart locations.</p>
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
