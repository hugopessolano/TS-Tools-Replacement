from rate_limit_schemas import RateLimit, OldLimit, BoostLimit, StorefrontLimit
from base_params import API_BASE_URL,USER_AGENT
import requests as r

def fetch_store_rate_limit(store_id:str, token:str) -> RateLimit:
    """
    Fetch the rate limit for a specific store using the provided store ID and token.

    Args:
        store_id (str): The ID of the store.
        token (str): The authentication token.

    Returns:
        RateLimit: a Pydantic object containing the rate limit information.
    """
    url = f"{API_BASE_URL}/{store_id}/products?page=1&per_page=1"
    request_data = r.get(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json",
            "Authentication": f"bearer {token}"
        }
    )
    if request_data.status_code != 200:
        #TODO: Log this
        raise Exception(f"Error fetching rate limit: {request_data.status_code} - {request_data.text}")
    #TODO: Log this
    match int(request_data.headers.get('x-rate-limit-limit')):
        case 500:
            return OldLimit
        case 1000:
            return StorefrontLimit
        case 400:
            return BoostLimit
        case _:
            return RateLimit

if __name__ == '__main__':
    fetch_store_rate_limit('STORE_ID','ACCESS_TOKEN')