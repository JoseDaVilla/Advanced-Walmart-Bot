import time
import logging
import os
import random
import tempfile
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from config import PAGE_LOAD_TIMEOUT, SCRIPT_TIMEOUT

logger = logging.getLogger(__name__)

def setup_playwright_browser(headless=True, retries=3, worker_id=0):

    for attempt in range(retries):
        playwright = None
        try:
            
            playwright = sync_playwright().start()
            
            user_data_dir = os.path.join(
                tempfile.gettempdir(),
                f"playwright-profile-{worker_id}-{random.randint(1000, 9999)}",
            )
            os.makedirs(user_data_dir, exist_ok=True)

            
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

            
            browser = playwright.chromium.launch(
                headless=headless,
                args=browser_args,
                timeout=30000,  
            )

            
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=get_random_user_agent(worker_id),
            )

            
            page = context.new_page()
            page.set_default_timeout(PAGE_LOAD_TIMEOUT * 1000)  
            page.set_default_navigation_timeout(PAGE_LOAD_TIMEOUT * 1000)

            
            page.goto("data:text/html,<html><body>Página de Prueba</body></html>")

            logger.info(
                f"Navegador Playwright creado exitosamente en el intento {attempt+1}"
            )

            
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
            
            try:
                if playwright:
                    playwright.stop()
            except:
                pass

            if attempt < retries - 1:
                
                time.sleep(5 * (attempt + 1))
            else:
                logger.error(
                    f"Error al configurar navegador Playwright después de {retries} intentos: {str(e)}"
                )

    return None


def get_random_user_agent(worker_id=0):

    user_agents = [
        f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Worker/{worker_id}",
        f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Worker/{worker_id}",
        f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Worker/{worker_id}",
    ]
    return random.choice(user_agents)


def wait_for_element(page, selector, timeout=10):

    try:
        return page.wait_for_selector(selector, timeout=timeout * 1000, state="visible")
    except PlaywrightTimeoutError:
        logger.warning(f"Tiempo de espera agotado esperando el elemento: {selector}")
        return None


def safe_click(page, selector, timeout=5):

    try:
        element = wait_for_element(page, selector, timeout=timeout)
        if element:
            element.click()
            return True
        return False
    except Exception as e:
        try:
            
            page.evaluate(f"document.querySelector('{selector}').click()")
            return True
        except Exception as js_e:
            logger.error(
                f"Error al hacer clic en elemento {selector}: {str(e)}, Error JS: {str(js_e)}"
            )
            return False


def scroll_to_element(page, selector):

    try:
        element = page.query_selector(selector)
        if element:
            element.scroll_into_view_if_needed()
            time.sleep(0.3)  
            return True
        return False
    except Exception as e:
        logger.error(f"Error al desplazar al elemento {selector}: {str(e)}")
        return False


def close_browser(browser_info):

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

    for attempt in range(retries):
        try:
            
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

            
            try:
                element.click(force=True, timeout=timeout * 1000)
                return True
            except Exception as e:
                logger.info(f"Clic forzado falló: {str(e)}")

            
            try:
                page.evaluate("arguments[0].click()", element)
                return True
            except Exception as e:
                logger.info(f"Clic JS falló: {str(e)}")

            
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

            
            try:
                
                box = element.bounding_box()
                if box:
                    center_x = box["x"] + box["width"] / 2
                    center_y = box["y"] + box["height"] / 2

                    
                    element.scroll_into_view_if_needed()
                    time.sleep(0.3)

                    
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

    try:
        
        if isinstance(selector_or_element, str):
            element = page.query_selector(selector_or_element)
            if not element:
                return ""
            return element.inner_html()
        
        
        return selector_or_element.inner_html()
    except Exception as e:
        logger.error(f"Error getting element HTML: {str(e)}")
        return ""


def take_element_screenshot(page, selector_or_element, file_path):

    try:
        
        if isinstance(selector_or_element, str):
            element = page.query_selector(selector_or_element)
            if not element:
                return False
            element.screenshot(path=file_path)
        else:
            
            selector_or_element.screenshot(path=file_path)
        return True
    except Exception as e:
        logger.error(f"Error taking element screenshot: {str(e)}")
        
        try:
            page.screenshot(path=file_path)
            return True
        except:
            return False
