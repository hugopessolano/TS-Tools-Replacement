from rate_limit_schemas import RateLimit, OldLimit, BoostLimit, StorefrontLimit
import requests as r
from schemas.request_schemas import *
import httpx
import asyncio
import pandas as pd
import time
from typing import List
from log_db import stdout_logger, db_logger, db_only_logger
from rich.progress import track
import uuid
import re
from dataframe_manager import DataframeManager

class RequestManager:
    def __init__(self, store_id:str, auth_token:str):
        self._id = str(uuid.uuid4())
        self._api_base_url = "https://api.tiendanube.com/v1"
        self._user_agent = "tech-support@tiendanube.com"
        self._success_status_codes = {200, 201, 204}
        self._rate_limit_status_code = 429

        self._store_id = store_id
        self._auth_token = f'bearer {auth_token}'
        self._headers = self.build_headers()
        self._composite_url = f"{self._api_base_url}/{self._store_id}"
        self._rate_limit = self.fetch_store_rate_limit()
        self._concurrecy_semaphore = asyncio.Semaphore(self.rate_limit.max_concurrent_requests)
        
        stdout_logger.info(f"RequestManager initialized under id {self.id} with store_id: {self._store_id}")

    @property
    def id(self):
        return self._id
    
    @property
    def store_id(self):
        return self._store_id
    
    @store_id.setter
    def store_id(self, value):
        if not isinstance(value, str):
            raise ValueError("store_id must be a string")
        self._store_id = value

    @property
    def auth_token(self):
        return self._auth_token
    
    @auth_token.setter
    def auth_token(self, value):
        if not isinstance(value, str):
            raise ValueError("auth_token must be a string")
        self._auth_token = value

    @property
    def rate_limit(self):
        return self._rate_limit

    @property
    def concurrency_semaphore(self):
        return self._concurrecy_semaphore
    
    def build_headers(self) -> dict:
        headers = {
        'Authentication': self._auth_token,
        'Content-Type': 'application/json',
        'User-Agent': self._user_agent
        }
        return headers
    
    def fetch_store_rate_limit(self) -> RateLimit:
        """
        Fetch the rate limit for a specific store using the provided store ID and token.
        Returns:
            RateLimit: a Pydantic object containing the rate limit information.
        """
        stdout_logger.info(f"Fetching rate limit for store {self.store_id}...")

        url = f"{self._composite_url}/products?page=1&per_page=1"
        request_data = r.get(url,headers=self._headers)
        if request_data.status_code != 200:
            db_logger.bind(operation_id=self.id, request_url=url, request_method='GET', store_id=self.store_id).error(f"Error fetching rate limit: {request_data.status_code} - {request_data.text}")
            raise Exception(f"Error fetching rate limit: {request_data.status_code} - {request_data.text}")
        
        match int(request_data.headers.get('x-rate-limit-limit')):
            case 500:
                stdout_logger.info(f"Setting OldLimit for store {self.store_id}")
                return OldLimit()
            case 1000:
                stdout_logger.info(f"Setting StorefrontLimit for store {self.store_id}")
                return StorefrontLimit()
            case 400:
                stdout_logger.info(f"Setting BoostLimit for store {self.store_id}")
                return BoostLimit()
            case _:
                stdout_logger.info(f"Setting Standard limit for store {self.store_id}")
                return RateLimit()

    async def prepare_request_data(self, request_data: Request) -> RequestData:
        """Prepara los datos para una única solicitud"""
        url = f'{self._composite_url}{request_data.endpoint_url}'
        headers = self._headers
        payload = request_data.payload if 'payload' in type(request_data).model_fields else None
        

        db_only_logger.bind(operation_id=self.id, store_id=self.store_id, request_url=url, request_method=request_data.method, payload=payload if payload else '').debug(f"Preparing request data for {request_data.method} {url}")
        return RequestData(
            url=url,
            headers=headers,
            method=request_data.method,
            payload=payload
        )
    
    async def request_execution(self, client: httpx.AsyncClient, request_data:RequestData) -> RequestData:
        try:
            # Manejar el método HTTP dinámicamente.
            match request_data.method:
                case 'GET':
                    request_data.response = await client.get(url=request_data.url, headers=request_data.headers, timeout=None)
                case 'POST':
                    request_data.response = await client.post(url=request_data.url, headers=request_data.headers, json=request_data.payload, timeout=None)
                case 'PUT':
                    request_data.response = await client.put(url=request_data.url, headers=request_data.headers, json=request_data.payload, timeout=None)
                case 'DELETE':
                    request_data.response = await client.delete(url=request_data.url, headers=request_data.headers, timeout=None)
                case _:  # Si el método no es soportado (No debería ocurrir si se usa normalmente).
                    raise ValueError(f'Método no soportado')

            if request_data.response_status not in self._success_status_codes:
                if request_data.response_status == self._rate_limit_status_code:
                    db_logger.bind(operation_id=self.id, store_id=self.store_id, 
                                request_url=request_data.url, request_method=request_data.method, 
                                payload=request_data.payload if request_data.payload else '',
                                status_code = request_data.response_status
                                ).warning(f"Rate limit exceeded for {request_data.method} {request_data.url}")
                else:    
                    db_logger.bind(operation_id=self.id, store_id=self.store_id, 
                                request_url=request_data.url, request_method=request_data.method, 
                                payload=request_data.payload if request_data.payload else '',
                                status_code=request_data.response_status,
                                response= request_data.response
                                ).error(f"Error processing {request_data.method} for {request_data.url}: {request_data.response_status}")
                return request_data
            
            if request_data.method == 'GET': #Solo logeo a la base los requests GET en modo DEBUG
                db_only_logger.bind(operation_id=self.id, store_id=self.store_id, 
                                    request_url=request_data.url, request_method=request_data.method, 
                                    payload=request_data.payload if request_data.payload else '',
                                    status_code = request_data.response_status
                                    ).debug(f"Processed {request_data.method} for {request_data.url} - {request_data.response_status}")
            else:
                db_only_logger.bind(operation_id=self.id, store_id=self.store_id, 
                                    request_url=request_data.url, request_method=request_data.method, 
                                    payload=request_data.payload if request_data.payload else '',
                                    status_code = request_data.response_status
                                    ).info(f"Processed {request_data.method} for {request_data.url} - {request_data.response_status}")
            return request_data

        except Exception as e:
            db_logger.bind(operation_id=self.id, store_id=self.store_id, request_url=request_data.url, request_method=request_data.method, payload=request_data.payload if request_data.payload else '', error=str(e)).error(f"Error processing {request_data.method} for {request_data.url}: {e}")
            return None
        
    async def execute_request(self, request_data: Request) -> RequestData:
        """Ejecuta una única solicitud"""
        stdout_logger.debug(f"Ejecutando request individual {request_data.method} para {request_data.endpoint_url}...")
        async with httpx.AsyncClient(http2=True) as client:
            stdout_logger.debug("Confirtiendo request_data de schema Request a RequestData...")
            request_data:RequestData = await self.prepare_request_data(request_data)
            response = await self.request_execution(client, request_data)
            stdout_logger.debug(f"Finalizado request individual {request_data.method} para {request_data.url} con status {response.response_status}")
            return response
    
    async def execute_requests(self, request_data_list: list[RequestData]) -> list[RequestData]:
        results = []
        tasks = []
        requests_sent_count = 0
        last_sustained_request_start_time = time.monotonic()
        total_requests = len(request_data_list)

        def release_semaphore(task):
            try:
                pass
            finally:
                self.concurrency_semaphore.release()
        stdout_logger.info(f"Ejecutando {total_requests} requests.")
        expected_runtime = (self.rate_limit.delay_between_requests * total_requests) - (self.rate_limit.burst_limit * self.rate_limit.delay_between_requests)
        stdout_logger.info(f"Tiempo máximo esperado de ejecución: {expected_runtime if expected_runtime > 0 else 'algunos'} segundos.")
        
        async with httpx.AsyncClient(http2=True) as client:
            for i, req_data in track(enumerate(request_data_list), total=total_requests, description="Ejecutando requests..."):
                
                if i == 0:
                    stdout_logger.debug(f"Iniciando Fase de Burst para {self.rate_limit.burst_limit} requests.")
                
                await self.concurrency_semaphore.acquire()
                requests_sent_count += 1
                current_time_before_potential_sleep = time.monotonic() # Tiempo actual

                if requests_sent_count > self.rate_limit.burst_limit:
                    if requests_sent_count == self.rate_limit.burst_limit + 1:
                        stdout_logger.debug(f"Fase de Burst finalizada. Iniciando Fase Sostenida.")

                    time_since_last_start = current_time_before_potential_sleep - last_sustained_request_start_time
                    sleep_duration = max(0, self.rate_limit.delay_between_requests - time_since_last_start)

                    if sleep_duration > 0:
                        #stdout_logger.debug(f"Rate limit: Durmiendo por {sleep_duration:.4f}s antes del request {requests_sent_count}")
                        await asyncio.sleep(sleep_duration)
                    last_sustained_request_start_time = time.monotonic()

                task = asyncio.create_task(self.request_execution(client, req_data), name=f"Req-{req_data.method}-{req_data.url}")
                task.add_done_callback(release_semaphore)

                tasks.append(task)

            stdout_logger.info(f"Esperando la finalización de {len(tasks)} requests...")
            results = await asyncio.gather(*tasks)
            stdout_logger.info("Todos los requests han finalizado.")

        # Filtrar resultados exitosos y fallidos
        successful_requests = [result for result in results if result.response_status in self._success_status_codes and result.response is not None]
        failed_requests = [result for result in results if result.response_status not in self._success_status_codes or result.response is None]
        stdout_logger.info(f"Requests exitosos: {len(successful_requests)}")
        stdout_logger.warning(f"Requests fallidos: {len(failed_requests)}")
        return results # Devuelve la lista de RequestData con sus respuestas (o None si fallaron)
    
    async def prepare_requests(self, request_list: list[Request]) -> list[RequestData]:
        """Prepara múltiples solicitudes"""
        stdout_logger.debug(f"Preparando formato para {len(request_list)} requests.")
        return [await self.prepare_request_data(request_data) for request_data in request_list]
    
    async def mass_execute_requests(self, requests_dataframe: pd.DataFrame) -> list[RequestData]:
        """Ejecuta múltiples solicitudes en masa"""
        stdout_logger.debug(f"Iniciando ejecución de requests en masa...")
        requests:List[Request] = requests_dataframe['request'].to_list()
        request_data_list:List[RequestData] = await self.prepare_requests(requests)
        stdout_logger.debug(f"Preparación de requests finalizada. Ingresando a ejecución por {len(request_data_list)} requests...")
        results = await self.execute_requests(request_data_list)
        stdout_logger.debug(f"Ejecución de requests finalizada. Retornando {len(results)} resultados.")
        return results
    
    def execute_multipage_request(self, request_data: Request) -> List[RequestData]:
        """
        Ejecuta una solicitud GET a un único endpoint, y genera los GETs necesarios para obtener todas las páginas de resultados.
        """
        def extract_last_page(link:str):
            stdout_logger.debug(f"Extrayendo número de última página de {link}")
            pattern = r'<[^>]*?page=(\d+)[^>]*>;\s*rel="last"'
            match = re.search(pattern, link)
            last_page_number = None
            if match:
                try:
                    last_page_number = int(match.group(1))
                    return last_page_number
                except (ValueError, IndexError):
                    stdout_logger.error("Se encontró el patrón, pero no se pudo extraer el número.")
                    return 1
            else:
                stdout_logger.error("No se encontró el patrón en el encabezado 'link'.")
                return 1

        def generate_pages_df(last_page:int):
            stdout_logger.debug(f"Generando DataFrame para {last_page} páginas.")
            params = [{'page': i} for i in range(2, last_page + 1)]
            return pd.DataFrame({'params':params})
        
        stdout_logger.info(f"Ejecutando solicitud multipágina para {request_data.endpoint_url}...")
        stdout_logger.debug(f"Ejecutando primer request")
        request_response = asyncio.run(self.execute_request(request_data))
        
        link = request_response.response_headers['link']
        if not link:
            stdout_logger.info(f"No se encontró el encabezado 'link'. Retornando solo el primer request.")
            return [request_response]
        
        last_page = extract_last_page(link)
        
        if not last_page or last_page == 1:
            stdout_logger.warning(f"No se encontraron mas paginas. Retornando solo el primer request.")
            return [request_response]
        
        pages_dataframe = generate_pages_df(last_page)
        dataframe_manager = DataframeManager(df=pages_dataframe)
        
        stdout_logger.debug(f"Convirtiendo DataFrame a requests...")
        dataframe_manager.parse_requests_into_dataframe(GetRequest, request_data.endpoint)

        request_responses = asyncio.run(self.mass_execute_requests(dataframe_manager.current_df))
        
        return [request_response] + request_responses
