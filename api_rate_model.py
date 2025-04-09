import os
import httpx
import time
import json
import logging
from colorama import Fore, Style, init
import pandas as pd
import asyncio
import numpy as np
from rate_limit_schemas import RateLimit, StorefrontLimit, OldLimit, BoostLimit
from base_params import API_BASE_URL, USER_AGENT, SCRIPT_DIR, LOG_DIR
from fetch_store_rate_limit import fetch_store_rate_limit
#pip install 'httpx[http2]'

STORE_ID = 'STORE_ID'
AUTH_TOKEN = 'TOKEN'
CSV_NAME = 'requests_data.csv'

# >> Parámetros de Rate Limit <<
rate_limit_data:RateLimit = fetch_store_rate_limit(STORE_ID, AUTH_TOKEN)

CSV_FILE_PATH = os.path.join(SCRIPT_DIR, 'tmp', CSV_NAME)


init(autoreset=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "script_run.log")),
        logging.StreamHandler() # También muestra logs en consola
    ]
)

# Códigos de estado HTTP considerados éxito
SUCCESS_STATUS_CODES = {200, 201, 204}
RATE_LIMIT_STATUS_CODE = 429

# --- Funciones Auxiliares ---


def read_csv(file_path: str) -> pd.DataFrame:
    """Lee el CSV de entrada y valida la columna necesaria."""
    try:
        df = pd.read_csv(file_path)
        logging.info(f"CSV leído exitosamente desde: {file_path}")
        if 'product_id' not in df.columns:
            logging.error(f"La columna 'product_id' no se encontró en {file_path}.")
            raise ValueError(f"El archivo CSV debe contener la columna 'product_id'.")
        df['product_id'] = df['product_id'].astype(str)
        return df
    except FileNotFoundError:
        logging.error(f"Archivo CSV no encontrado en: {file_path}")
        raise
    except Exception as e:
        logging.error(f"Error al leer o procesar el archivo CSV: {e}")
        raise

def prepare_request_data(product_id: str) -> tuple:
    """Prepara los datos para una única solicitud PUT."""
    url = f'{API_BASE_URL}/v1/{STORE_ID}/products/{product_id}'
    headers = {
        'Authentication': AUTH_TOKEN,
        'Content-Type': 'application/json',
        'User-Agent': USER_AGENT
    }
    # Payload de ejemplo para PUT.
    payload = {"description": {"es":f"Script update triggered at {time.time()}"}}

    return url, headers, payload, product_id

async def execute_request(
    client: httpx.AsyncClient,
    request_data: tuple,
    concurrency_semaphore: asyncio.Semaphore
) -> list:
    """Ejecuta una única solicitud PUT gestionando la concurrencia."""
    url, headers, payload, product_id = request_data
    response_or_error = None

    async with concurrency_semaphore: # Limita las requests activas simultáneas
        try:
            logging.debug(f"Iniciando request para product_id: {product_id}")
            response = await client.put(url, headers=headers, json=payload, timeout=rate_limit_data.request_timeout)
            response_or_error = response 

            # Loguear específicamente los 429
            if response.status_code == RATE_LIMIT_STATUS_CODE:
                 logging.warning(f"Recibido 429 (Too Many Requests) para product_id: {product_id}")
                 # await asyncio.sleep(0.5)
            elif response.status_code not in SUCCESS_STATUS_CODES:
                 logging.warning(f"Recibido estado {response.status_code} para product_id: {product_id}. URL: {url}")

        except httpx.TimeoutException:
            logging.error(f"Timeout para product_id: {product_id} en URL: {url}")
            response_or_error = "Timeout Error"
        except httpx.RequestError as exc:
            logging.error(f"Error de request para product_id: {product_id} ({exc.__class__.__name__}). URL: {url} | Error: {exc}")
            response_or_error = f"Request Error: {exc}"
        except Exception as exc:
            logging.exception(f"Error inesperado ejecutando request para product_id: {product_id}. URL: {url}") # Log con traceback
            response_or_error = f"Unexpected Error: {exc}"

    return [response_or_error, product_id, url]


async def main() -> pd.DataFrame:
    """Tarea principal que orquesta la lectura, ejecución con rate limit y recolección de resultados."""
    try:
        df_input = read_csv(CSV_FILE_PATH)
        total_requests = len(df_input)
        logging.info(f"Se prepararán {total_requests} requests.")
        if total_requests == 0:
            print(Fore.YELLOW + "El archivo CSV está vacío o no contiene datos procesables.")
            return pd.DataFrame()

    except Exception as e:
        print(f"{Fore.RED}Error al iniciar: {e}")
        return pd.DataFrame() 

    # Preparar todos los datos de request de antemano
    all_request_data = [prepare_request_data(pid) for pid in df_input['product_id']]

    results = []
    tasks = []
    concurrency_semaphore = asyncio.Semaphore(rate_limit_data.max_concurrent_requests)
    requests_sent_count = 0
    last_sustained_request_time = time.monotonic() # Hora de inicio para cálculo de delay

    start_time = time.monotonic()
    logging.info(f"Iniciando envío de {total_requests} requests...")
    print(f"{Fore.CYAN}Iniciando {total_requests} requests con rate limit: {rate_limit_data.burst_limit} burst, {rate_limit_data.sustained_rate_rps} req/s sostenido...")
    print(f"{Fore.CYAN}Límite de concurrencia local: {rate_limit_data.max_concurrent_requests}")
    print(f"{Fore.CYAN}Delay entre requests (tasa sostenida): {rate_limit_data.delay_between_requests:.4f} segundos")

    async with httpx.AsyncClient(http2=True) as client:
        for i, req_data in enumerate(all_request_data):
            requests_sent_count += 1

            # --- Lógica de Rate Limiting ---
            if requests_sent_count > rate_limit_data.burst_limit:
                # Estamos en la fase de tasa sostenida
                current_time = time.monotonic()
                time_since_last = current_time - last_sustained_request_time
                sleep_duration = max(0, rate_limit_data.delay_between_requests - time_since_last)

                if sleep_duration > 0:
                    logging.debug(f"Rate limit: Durmiendo por {sleep_duration:.4f}s antes del request {requests_sent_count}")
                    await asyncio.sleep(sleep_duration)

                # Actualizar la marca de tiempo *después* de la espera (o inmediatamente si no hubo espera)
                # para el cálculo del siguiente request
                last_sustained_request_time = time.monotonic()
            
            # Creamos la tarea para ejecutar el request
            task = asyncio.create_task(execute_request(client, req_data, concurrency_semaphore), name=f"Req-{req_data[3]}")
            tasks.append(task)

            # Pequeña pausa opcional incluso en burst para no saturar la creación de tareas
            # await asyncio.sleep(0.001)

            # Log de progreso (opcional, puede ralentizar si hay muchos requests)
            if (i + 1) % 50 == 0 or (i + 1) == total_requests:
                 elapsed = time.monotonic() - start_time
                 rps_actual = (i + 1) / elapsed if elapsed > 0 else 0
                 print(f"{Fore.BLUE}Progreso: {i+1}/{total_requests} tasks creadas... (RPS actual: {rps_actual:.2f})")


        print(f"\n{Fore.CYAN}Esperando la finalización de {len(tasks)} requests...")
        results = await asyncio.gather(*tasks)
        print(f"{Fore.GREEN}Todos los requests han finalizado.")


    if results:
        df_results = pd.DataFrame(results, columns=['Estado', 'product_id', 'URL_Utilizada'])
        return df_results
    else:
        logging.warning("No se obtuvieron resultados de las tareas.")
        return pd.DataFrame()

if __name__ == "__main__":
    print(f"{Style.BRIGHT}--- Inicio del Script de Requests PUT con Rate Limiting ---{Style.RESET_ALL}")

    if 'tu_' in STORE_ID or 'tu_' in AUTH_TOKEN:
        print(f"{Fore.RED}ERROR: Por favor, edita las variables STORE_ID y AUTH_TOKEN en el script.")
        exit(1)
    if rate_limit_data.sustained_rate_rps <= 0:
        print(f"{Fore.RED}ERROR: SUSTAINED_RATE_RPS debe ser mayor que 0.")
        exit(1)

    ensure_dir_exists(os.path.join(SCRIPT_DIR, 'tmp'))
    ensure_dir_exists(LOG_DIR)

    start_run_time = time.perf_counter()
    final_df = asyncio.run(main())
    end_run_time = time.perf_counter()

    total_time_seconds = end_run_time - start_run_time
    total_processed = len(final_df.index) if not final_df.empty else 0

    print(f"\n{Style.BRIGHT}--- Procesamiento de Logs y Resumen Final ---{Style.RESET_ALL}")
    full_log, failed_log = build_log_paths()
    build_logs(final_df, full_log, failed_log)

    print(f'\n--- {Style.BRIGHT}Estadísticas de Ejecución{Style.RESET_ALL} ---')
    print(f'Total de requests intentados: {total_processed}')
    print(f'Tiempo total de ejecución: {total_time_seconds:.2f} segundos')
    if total_processed > 0 and total_time_seconds > 0:
        overall_rps = total_processed / total_time_seconds
        print(f'RPS promedio general: {overall_rps:.2f} requests/segundo')
    print(f"{Style.BRIGHT}--- Fin del Script ---{Style.RESET_ALL}")