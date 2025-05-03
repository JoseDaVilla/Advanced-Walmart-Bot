"""
Funciones de notificación por correo electrónico para el Verificador de Arrendamiento de Walmart
"""

import logging
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from config import EMAIL_SENDER, EMAIL_RECEIVER, EMAIL_PASSWORD

# Configuración de logs
logger = logging.getLogger(__name__)


def send_email(properties):
    """
    Envía notificación por correo electrónico sobre propiedades coincidentes.

    Args:
        properties: Lista de propiedades que coinciden con criterios
    """
    if not properties:
        logger.info("No hay propiedades sobre las cuales notificar")
        return

    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECEIVER
        msg["Subject"] = (
            f"Oportunidades de Arrendamiento en Walmart - {datetime.now().strftime('%Y-%m-%d')}"
        )

        # Crear contenido HTML con estructura de tabla mejorada
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
            <h2>Oportunidades de Arrendamiento en Walmart</h2>
            <p>Se encontraron {len(properties)} ubicaciones que coinciden con sus criterios:</p>
            <table>
                <tr>
                    <th>Tienda #</th>
                    <th>Dirección</th>
                    <th>Ciudad</th>
                    <th>CP</th>
                    <th>Espacios Disponibles</th>
                    <th>Reseñas</th>
                    <th>Estado</th>
                </tr>
        """

        # Agregar versión de texto plano también
        text_content = "Oportunidades de Arrendamiento en Walmart\n\n"
        text_content += f"Se encontraron {len(properties)} ubicaciones que coinciden con sus criterios:\n\n"

        # Agregar cada propiedad al correo electrónico con formato de espacio mejorado
        for prop in properties:
            # Usar el ID de tienda del sitio de arrendamiento como ID principal
            store_id = prop.get("store_id", "Unknown")
            store_num = f"Tienda #{store_id}"

            # Dirección original del sitio de arrendamiento
            leasing_address = prop.get("address", "Unknown")

            # Usar ciudad y código postal de los datos de Google
            city = prop.get("city", "Unknown")
            zip_code = prop.get("zip_code", "Unknown")
            reviews = prop.get("review_count", "N/A")

            # Crear un enlace simple al sitio web de Walmart si está disponible
            website = prop.get("website", "")
            website_html = (
                f"<br><a href='{website}' target='_blank'>Sitio Web</a>"
                if website
                else ""
            )

            # Crear detalles de espacio HTML - formato mejorado con viñetas
            space_html = "<ul class='space-list'>"
            space_text = ""

            for space in sorted(prop.get("spaces", []), key=lambda x: x.get("sqft", 0)):
                suite = space.get("suite", "Por determinar")
                sqft = space.get("sqft", "Unknown")
                space_html += f"<li><strong>Suite {suite}</strong>: {sqft} pies²</li>"
                space_text += f"- Suite {suite}: {sqft} pies²\n"

            space_html += "</ul>"

            # Todas las propiedades en la lista final han sido confirmadas para NO tener tiendas móviles
            radius = prop.get("mobile_store_search_radius", "100m")
            mobile_store = f"No se detectaron tiendas móviles en un radio de {radius} <span class='check'>&check;</span>"

            # Agregar al contenido HTML con diseño mejorado
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

            # Agregar al contenido de texto
            text_content += f"• {store_num} en {leasing_address} - {city}, {zip_code} - {reviews} reseñas - Sin tiendas móviles *\n"
            text_content += space_text
            text_content += "\n"

        # Cerrar el HTML con mejor explicación de detección de tiendas móviles
        html_content += """
            </table>
            <p>Este es un mensaje automatizado de su Verificador de Arrendamiento de Walmart.</p>
            <p><strong>Nota:</strong> Todos los listados anteriores han sido verificados para cumplir con los siguientes criterios:</p>
            <ul>
                <li>Espacio disponible menor a 1000 pies²</li>
                <li>Más de 10,000 reseñas en Google</li>
                <li>Sin tiendas de reparación de teléfonos móviles presentes en un radio de 200 metros o en la misma dirección que el Walmart</li>
                <li>Debe tener información verificada de ciudad y código postal</li>
            </ul>
            <p><strong>Cómo funciona la detección de tiendas móviles:</strong> El sistema realiza múltiples comprobaciones para cada ubicación de Walmart:</p>
            <ol>
                <li>Utiliza la función "Cerca" de Google Maps directamente desde la página de ubicación de Walmart para buscar "reparación de teléfonos móviles"</li>
                <li>Realiza búsquedas separadas para "reparación de teléfonos celulares cerca de [dirección]" y "tienda de reparación móvil cerca de [dirección]"</li>
                <li>Realiza búsquedas específicas para marcas específicas ("The Fix", "iFixAndRepair", "Boost Mobile", etc.) + la dirección de Walmart</li>
                <li>Todos los resultados de búsqueda se analizan tanto por distancia (dentro de 200m) como para detectar si están en la misma dirección que el Walmart</li>
                <li>Se marcan todas las tiendas que coinciden con palabras clave específicas (The Fix, iFixAndRepair, Cellaris, Talk N Fix, Techy, etc.)</li>
                <li>El sistema utiliza múltiples estrategias de búsqueda para garantizar una detección exhaustiva de tiendas móviles</li>
            </ol>
            <p>Este enfoque mejorado garantiza que detectemos tanto tiendas de reparación independientes cercanas como servicios ubicados dentro del propio Walmart.</p>
        </body>
        </html>
        """

        # Adjuntar partes de texto y HTML
        msg.attach(MIMEText(text_content, "plain"))
        msg.attach(MIMEText(html_content, "html"))

        # Enviar el correo electrónico
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
            logger.info(f"Correo electrónico enviado exitosamente a {EMAIL_RECEIVER}")

    except Exception as e:
        logger.error(f"Error al enviar correo electrónico: {str(e)}")
