from pydantic import BaseModel, PrivateAttr
from typing import Any, Optional, Dict, Tuple, Union
from urllib.parse import urlencode
from schemas.endpoint_map import Endpoint
from httpx import Response
import json
import logging

class Request(BaseModel):
    endpoint: Endpoint
    method: str
    positional_url_arguments: Optional[Tuple[str, ...]] = None
    endpoint_url: str = ""

    def model_post_init(self, __context: Any) -> None:
        """
        Runs after model initialization and validation.
        Formats the endpoint_url based on endpoint and positional_url_arguments.
        """
        super().model_post_init(__context)

        if self.positional_url_arguments:
            try:
                format_args = {
                    f"arg_{i+1}": arg
                    for i, arg in enumerate(self.positional_url_arguments)
                }
                expected_placeholders = len(format_args)
                actual_placeholders = sum(1 for i in range(1, expected_placeholders + 2) if f"{{arg_{i}}}" in self.endpoint.path_with_positional_args())

                if expected_placeholders != actual_placeholders:
                     raise ValueError(
                         f"Mismatch between placeholders in endpoint '{self.endpoint.path_with_positional_args()}' "
                         f"({actual_placeholders} found like {{arg_N}}) and number of "
                         f"positional_url_arguments ({expected_placeholders} provided)."
                     )

                self.endpoint_url = self.endpoint.path_with_positional_args().format(**format_args)
            except (KeyError, IndexError, ValueError) as e:
                raise ValueError(f"Error formatting endpoint '{self.endpoint}' with arguments {self.positional_url_arguments}: {e}") from e
        else:
            self.endpoint_url = self.endpoint.path_with_positional_args()

class GetRequest(Request):
    params: Optional[Dict[str, Any]] = None
    method: str = "GET"

    def model_post_init(self, __context: Any) -> None:
        """
        Runs after parent model_post_init.
        Appends URL query parameters from 'params' to the endpoint_url.
        """
        super().model_post_init(__context)

        if self.params:
            query_string = urlencode(self.params)
            self.endpoint_url += f"?{query_string}"

class PostRequest(Request):
    payload: Optional[Dict[str, Any]] = None
    method: str = "POST"

class PutRequest(Request):
    payload: Dict[str, Any]
    method: str = "PUT"

class DeleteRequest(Request):
    method: str = "DELETE"

class RequestData(BaseModel):
    """
    Request Data model for the RequestManager class.
    """
    url:str
    headers:dict
    method:str
    payload: Optional[Dict[str, Any]] = None
    response_status: Optional[int] = None
    response_headers: Optional[Dict[str, Any]] = None
    _response: Optional[Union[Dict[str, Any], Response]] = PrivateAttr(default=None)

    class Config:
        arbitrary_types_allowed = True

    @property
    def response(self) -> Optional[Dict[str, Any]]:
        if isinstance(self._response, Response):
            logging.warning("Accessing response property while internal state is still httpx.Response.")
            self._process_assigned_response(self._response)
            return self._response if isinstance(self._response, dict) else None
        return self._response

    @response.setter
    def response(self, value: Optional[Union[Dict[str, Any], Response]]):
        self._process_assigned_response(value)

    def _process_assigned_response(self, value: Optional[Union[Dict[str, Any], Response]]):
        """Helper method to process and store the response value."""
        if isinstance(value, Response):
            httpx_response: Response = value
            status_code = None
            processed_response_body = None

            try:
                status_code = httpx_response.status_code
            except Exception as e:
                logging.error(f"Failed to get status code from httpx.Response: {e}", exc_info=True)
            try:
                processed_response_body = httpx_response.json()
            except json.JSONDecodeError:
                logging.warning(f"Response body is not valid JSON for URL {self.url}. Status: {status_code}")
                processed_response_body = {
                    "error": "JSONDecodeError",
                    "message": "Response body is not valid JSON.",
                    # "raw_body": httpx_response.text[:500] # Limitar tamaño
                }
            except Exception as e:
                logging.error(f"Failed to process response body for URL {self.url}: {e}", exc_info=True)
                processed_response_body = {
                    "error": "ResponseBodyProcessingError",
                    "message": str(e)
                }

            self.response_status = status_code
            self.response_headers = dict(httpx_response.headers)
            self._response = processed_response_body # Almacenar el dict procesado
            logging.debug(f"Processed response for {self.url}: Status={status_code}")

        else:
            self._response = value
            if value is None:
                 self.response_status = None
            elif isinstance(value, dict) and self.response_status is None:
                 logging.debug(f"Dict assigned to response for {self.url} without explicit status.")


if __name__ == '__main__':
    get_request = GetRequest(
        endpoint=Endpoint(path="products/{arg_1}/variants/{arg_2}"),
        positional_url_arguments=("123","456"),
        params={"key": "value"}
        )
    print('ok')