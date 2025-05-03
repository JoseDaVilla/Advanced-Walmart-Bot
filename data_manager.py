"""
Gestión de datos para el Verificador de Arrendamiento de Walmart
Maneja carga, guardado y versionado de datos de propiedades
"""

import os
import json
import logging
from datetime import datetime
from config import OUTPUT_DIR

# Configuración de logs
logger = logging.getLogger(__name__)

# Ensure output directory exists at module level
try:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info(f"Ensuring output directory exists: {OUTPUT_DIR}")
except Exception as e:
    logger.error(f"Error creating output directory: {str(e)}")


def load_previous_results():
    """
    Carga propiedades coincidentes identificadas previamente desde archivo.

    Returns:
        Lista de propiedades previamente guardadas
    """
    file_path = os.path.join(OUTPUT_DIR, "matching_properties.json")
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                logger.info(f"Cargadas {len(data)} propiedades de resultados previos")
                return data
        except Exception as e:
            logger.error(f"Error al cargar propiedades coincidentes previas: {str(e)}")
    return []


def is_duplicate_property(new_prop, existing_props):
    """
    Verifica si una propiedad es duplicada de una ya existente en nuestros resultados.

    Args:
        new_prop: Nueva propiedad a verificar
        existing_props: Lista de propiedades existentes

    Returns:
        True si es un duplicado, False en caso contrario
    """
    for prop in existing_props:
        # Comparar ID de tienda y dirección para unicidad
        store_match = new_prop.get("store_id") == prop.get("store_id")
        website_match = (
            new_prop.get("website_store_id") == prop.get("website_store_id")
            and new_prop.get("website_store_id") is not None
        )

        # Si coincide ID de tienda o ID de tienda de sitio web, considerar que es la misma tienda
        if store_match or website_match:
            return True
    return False


def improve_property_data(merged_properties):
    """
    Mejora datos de propiedad llenando información faltante de otras propiedades
    con el mismo ID de tienda o dirección.

    Args:
        merged_properties: Lista de propiedades mezcladas

    Returns:
        Lista de propiedades mejorada
    """
    # Crear diccionarios para buscar propiedades por ID y dirección
    by_store_id = {}
    by_website_id = {}
    by_address = {}

    # Primera pasada - construir diccionarios de búsqueda
    for prop in merged_properties:
        store_id = prop.get("store_id")
        website_id = prop.get("website_store_id")
        address = prop.get("address", "").lower()

        if store_id and store_id not in by_store_id:
            by_store_id[store_id] = prop

        if website_id and website_id not in by_website_id:
            by_website_id[website_id] = prop

        if address and address not in by_address:
            by_address[address] = prop

    # Segunda pasada - completar información faltante
    for prop in merged_properties:
        # Intentar completar ciudad y código postal si se desconocen
        if prop.get("city") == "Unknown" or prop.get("zip_code") == "Unknown":
            store_id = prop.get("store_id")
            website_id = prop.get("website_store_id")
            address = prop.get("address", "").lower()

            # Intentar encontrar otra propiedad con los mismos identificadores
            match = None
            if store_id and store_id in by_store_id and by_store_id[store_id] != prop:
                match = by_store_id[store_id]
            elif (
                website_id
                and website_id in by_website_id
                and by_website_id[website_id] != prop
            ):
                match = by_website_id[website_id]
            elif address and address in by_address and by_address[address] != prop:
                match = by_address[address]

            if match:
                if prop.get("city") == "Unknown" and match.get("city") != "Unknown":
                    prop["city"] = match.get("city")
                    prop["city_source"] = "matched_property"

                if (
                    prop.get("zip_code") == "Unknown"
                    and match.get("zip_code") != "Unknown"
                ):
                    prop["zip_code"] = match.get("zip_code")
                    prop["zip_code_source"] = "matched_property"

    return merged_properties


def save_results_with_versioning(properties):
    """
    Guarda resultados con versionado para evitar sobrescribir datos previos.

    Args:
        properties: Lista de propiedades a guardar

    Returns:
        Lista de propiedades mezcladas
    """
    # Crear nombre de archivo base
    base_filename = "matching_properties"
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Primero, guardar versión actual con marca de tiempo
    versioned_filename = f"{base_filename}_{current_time}.json"
    versioned_path = os.path.join(OUTPUT_DIR, versioned_filename)

    # Siempre guardar los resultados actuales con marca de tiempo
    with open(versioned_path, "w", encoding="utf-8") as f:
        json.dump(properties, f, indent=2)
    logger.info(f"Resultados actuales guardados en {versioned_filename}")

    # Ahora actualizar el archivo principal (mezclado con previos si aplica)
    main_path = os.path.join(OUTPUT_DIR, f"{base_filename}.json")

    # Cargar resultados previos
    previous_results = load_previous_results()

    # Si tenemos resultados previos, mezclar con resultados actuales
    if previous_results:
        # Agregar nuevas propiedades que no sean duplicados
        merged_properties = previous_results.copy()
        new_count = 0

        for prop in properties:
            if not is_duplicate_property(prop, previous_results):
                merged_properties.append(prop)
                new_count += 1

        # Mejorar los datos mezclados
        merged_properties = improve_property_data(merged_properties)

        # Guardar resultados mezclados
        with open(main_path, "w", encoding="utf-8") as f:
            json.dump(merged_properties, f, indent=2)

        logger.info(
            f"Actualizado matching_properties.json con {new_count} nuevas propiedades (total: {len(merged_properties)})"
        )
        return merged_properties
    else:
        # Si no hay resultados previos, simplemente guardar resultados actuales como archivo principal
        with open(main_path, "w", encoding="utf-8") as f:
            json.dump(properties, f, indent=2)
        logger.info(
            f"Creado nuevo matching_properties.json con {len(properties)} propiedades"
        )
        return properties


def save_intermediate_results(properties, filename):
    """
    Guarda resultados intermedios para depuración o análisis.

    Args:
        properties: Lista de propiedades a guardar
        filename: Nombre del archivo
    """
    try:
        # Ensure output directory exists again right before saving
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        file_path = os.path.join(OUTPUT_DIR, filename)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(properties, f, indent=2)
        logger.info(f"Guardadas {len(properties)} propiedades en {filename}")
    except Exception as e:
        logger.error(f"Error al guardar resultados intermedios en {filename}: {str(e)}")
        # Create fallback save path in the current directory if OUTPUT_DIR fails
        try:
            fallback_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
            with open(fallback_path, "w", encoding="utf-8") as f:
                json.dump(properties, f, indent=2)
            logger.warning(f"Fallback save to current directory: {fallback_path}")
        except Exception as fallback_e:
            logger.error(f"Fallback save also failed: {str(fallback_e)}")
