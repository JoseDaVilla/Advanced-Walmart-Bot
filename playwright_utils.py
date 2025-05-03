"""
Funciones de utilidad para automatización con Playwright
"""

import time
import logging
import os
import random
import tempfile
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from config import PAGE_LOAD_TIMEOUT, SCRIPT_TIMEOUT

# Configuración de logs
logger = logging.getLogger(__name__)


def setup_playwright_browser(headless=True, retries=3, worker_id=0):
    """
    Configura y devuelve una instancia del navegador Playwright con mecanismo de reintentos.

    Args:
        headless: Modo sin interfaz gráfica
        retries: Número de reintentos si falla
        worker_id: ID del trabajador para paralelismo

    Returns:
        Diccionario con instancias de playwright, navegador, contexto y página
    """
    for attempt in range(retries):
        playwright = None
        try:
            # Iniciar Playwright
            playwright = sync_playwright().start()

            # Crear un directorio único para los datos de usuario de este trabajador
            user_data_dir = os.path.join(
                tempfile.gettempdir(),
                f"playwright-profile-{worker_id}-{random.randint(1000, 9999)}",
            )
            os.makedirs(user_data_dir, exist_ok=True)

            # Configurar opciones del navegador
            browser_args = [
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                f"--window-name=worker-{worker_id}",
                "--disable-webgl",
                "--disable-extensions",
                "--disable-browser-side-navigation",
                "--dns-prefetch-disable",
                "--log-level=3",
                "--silent",
            ]

            # Iniciar navegador
            browser = playwright.chromium.launch(
                headless=headless,
                args=browser_args,
                timeout=30000,  # 30 segundos en ms
            )

            # Crear contexto con el directorio de datos de usuario
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=get_random_user_agent(worker_id),
            )

            # Crear página con tiempos de espera apropiados
            page = context.new_page()
            page.set_default_timeout(PAGE_LOAD_TIMEOUT * 1000)  # Convertir a ms
            page.set_default_navigation_timeout(PAGE_LOAD_TIMEOUT * 1000)

            # Probar que el navegador responde con una URL simple (evita la red)
            page.goto("data:text/html,<html><body>Página de Prueba</body></html>")

            logger.info(
                f"Navegador Playwright creado exitosamente en el intento {attempt+1}"
            )

            # Devolver todo lo necesario para limpiar correctamente después
            return {
                "playwright": playwright,
                "browser": browser,
                "context": context,
                "page": page,
            }

        except Exception as e:
            logger.warning(
                f"Intento {attempt+1} falló al crear navegador Playwright: {str(e)}"
            )
            # Limpiar recursos si la inicialización falló
            try:
                if playwright:
                    playwright.stop()
            except:
                pass

            if attempt < retries - 1:
                # Esperar antes de reintentar
                time.sleep(5 * (attempt + 1))
            else:
                logger.error(
                    f"Error al configurar navegador Playwright después de {retries} intentos: {str(e)}"
                )

    return None


def get_random_user_agent(worker_id=0):
    """
    Obtiene un agente de usuario aleatorio con ID de trabajador para garantizar la unicidad.

    Args:
        worker_id: ID del trabajador para agregar al agente de usuario

    Returns:
        Cadena de agente de usuario aleatoria
    """
    user_agents = [
        f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Worker/{worker_id}",
        f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Worker/{worker_id}",
        f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Worker/{worker_id}",
    ]
    return random.choice(user_agents)


def wait_for_element(page, selector, timeout=10):
    """
    Espera a que aparezca un elemento en la página y lo devuelve.

    Args:
        page: Objeto página de Playwright
        selector: Selector CSS del elemento
        timeout: Tiempo máximo de espera en segundos

    Returns:
        Elemento encontrado o None si no se encuentra
    """
    try:
        return page.wait_for_selector(selector, timeout=timeout * 1000, state="visible")
    except PlaywrightTimeoutError:
        logger.warning(f"Tiempo de espera agotado esperando el elemento: {selector}")
        return None


def safe_click(page, selector, timeout=5):
    """
    Hace clic en un elemento de forma segura con métodos alternativos de respaldo.

    Args:
        page: Objeto página de Playwright
        selector: Selector CSS del elemento
        timeout: Tiempo máximo de espera en segundos

    Returns:
        True si el clic fue exitoso, False en caso contrario
    """
    try:
        element = wait_for_element(page, selector, timeout=timeout)
        if element:
            element.click()
            return True
        return False
    except Exception as e:
        try:
            # Intentar clic JavaScript como respaldo
            page.evaluate(f"document.querySelector('{selector}').click()")
            return True
        except Exception as js_e:
            logger.error(
                f"Error al hacer clic en elemento {selector}: {str(e)}, Error JS: {str(js_e)}"
            )
            return False


def scroll_to_element(page, selector):
    """
    Desplaza la página para hacer visible un elemento.

    Args:
        page: Objeto página de Playwright
        selector: Selector CSS del elemento

    Returns:
        True si el desplazamiento fue exitoso, False en caso contrario
    """
    try:
        element = page.query_selector(selector)
        if element:
            element.scroll_into_view_if_needed()
            time.sleep(0.3)  # Breve pausa para que el desplazamiento termine
            return True
        return False
    except Exception as e:
        logger.error(f"Error al desplazar al elemento {selector}: {str(e)}")
        return False


def close_browser(browser_info):
    """
    Cierra correctamente el navegador y todos los recursos asociados.

    Args:
        browser_info: Diccionario con las instancias de Playwright
    """
    if not browser_info:
        return

    try:
        if "page" in browser_info:
            browser_info["page"].close()
    except:
        pass

    try:
        if "context" in browser_info:
            browser_info["context"].close()
    except:
        pass

    try:
        if "browser" in browser_info:
            browser_info["browser"].close()
    except:
        pass

    try:
        if "playwright" in browser_info:
            browser_info["playwright"].stop()
    except:
        pass


def force_click(page, element_or_selector, timeout=5, retries=3):
    """
    Hace clic en un elemento forzadamente usando múltiples estrategias con reintentos.
    Resuelve problemas de clics interceptados y otros problemas relacionados con clics.

    Args:
        page: Objeto página de Playwright
        element_or_selector: ElementHandle de Playwright o cadena de selector CSS
        timeout: Tiempo de espera en segundos para operaciones de clic
        retries: Número de reintentos si el clic falla

    Returns:
        bool: True si el clic tuvo éxito, False en caso contrario
    """
    for attempt in range(retries):
        try:
            # Obtener el elemento si se pasó un selector
            element = element_or_selector
            if isinstance(element_or_selector, str):
                element = page.query_selector(element_or_selector)
                if not element:
                    logger.warning(
                        f"Elemento no encontrado con selector: {element_or_selector}"
                    )
                    if attempt < retries - 1:
                        time.sleep(1)
                        continue
                    return False

            # Estrategia 1: Clic forzado con Playwright
            try:
                element.click(force=True, timeout=timeout * 1000)
                return True
            except Exception as e:
                logger.info(f"Clic forzado falló: {str(e)}")

            # Estrategia 2: Clic JavaScript
            try:
                page.evaluate("arguments[0].click()", element)
                return True
            except Exception as e:
                logger.info(f"Clic JS falló: {str(e)}")

            # Estrategia 3: Disparar evento de ratón
            try:
                page.evaluate(
                    """
                    (element) => {
                        const event = new MouseEvent('click', {
                            view: window,
                            bubbles: true,
                            cancelable: true
                        });
                        element.dispatchEvent(event);
                    }
                """,
                    element,
                )
                time.sleep(0.5)
                return True
            except Exception as e:
                logger.info(f"Envío de evento falló: {str(e)}")

            # Estrategia 4: Intentar hacer clic en el centro del elemento usando page.mouse
            try:
                # Obtener posición y dimensiones del elemento
                box = element.bounding_box()
                if box:
                    center_x = box["x"] + box["width"] / 2
                    center_y = box["y"] + box["height"] / 2

                    # Desplazar para asegurar que el elemento esté a la vista
                    element.scroll_into_view_if_needed()
                    time.sleep(0.3)

                    # Hacer clic en las coordenadas centrales
                    page.mouse.click(center_x, center_y)
                    return True
            except Exception as e:
                logger.info(f"Clic en posición del ratón falló: {str(e)}")

            if attempt < retries - 1:
                logger.info(
                    f"Todas las estrategias de clic fallaron, reintentando ({attempt+1}/{retries})"
                )
                time.sleep(1)
            else:
                logger.warning(
                    f"Error al hacer clic en el elemento después de {retries} intentos"
                )

        except Exception as e:
            logger.error(f"Error en force_click: {str(e)}")
            if attempt < retries - 1:
                time.sleep(1)

    return False


def get_element_html(page, selector_or_element):
    """
    Safely get HTML content from an element or selector with error handling.
    
    Args:
        page: Playwright page object
        selector_or_element: CSS selector string or Playwright element handle
        
    Returns:
        HTML content as string or empty string if an error occurs
    """
    try:
        # If we got a selector string
        if isinstance(selector_or_element, str):
            element = page.query_selector(selector_or_element)
            if not element:
                return ""
            return element.inner_html()
        
        # If we got an element handle
        return selector_or_element.inner_html()
    except Exception as e:
        logger.error(f"Error getting element HTML: {str(e)}")
        return ""


def take_element_screenshot(page, selector_or_element, file_path):
    """
    Take a screenshot of a specific element with error handling.
    
    Args:
        page: Playwright page object
        selector_or_element: CSS selector string or Playwright element handle
        file_path: Path where to save the screenshot
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # If we got a selector string
        if isinstance(selector_or_element, str):
            element = page.query_selector(selector_or_element)
            if not element:
                return False
            element.screenshot(path=file_path)
        else:
            # If we got an element handle
            selector_or_element.screenshot(path=file_path)
        return True
    except Exception as e:
        logger.error(f"Error taking element screenshot: {str(e)}")
        # Try taking a full page screenshot instead
        try:
            page.screenshot(path=file_path)
            return True
        except:
            return False
