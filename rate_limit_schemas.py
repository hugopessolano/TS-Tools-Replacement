from pydantic import BaseModel

OLD_LIMIT = 500
OLD_RATE  = 1.7
OLD_TAG = 'rate-limit-old'
STOREFRONT_LIMIT = 1000
STOREFRONT_RATE  = 100
STOREFRONT_TAG  = 'rate-limit-storefront'
BOOST_LIMIT = 400
BOOST_RATE  = 20
BOOST_TAG   = 'boost-api-rate-limit'

class RateLimit(BaseModel):
    burst_limit:int = 40  # Límite de requests por segundo durante el burst
    sustained_rate_rps:float = 2 # Límite de requests por segundo durante el periodo sostenido
    # Retardo calculado entre requests sostenidos (no editar directamente)
    delay_between_requests:float = 1.0 / sustained_rate_rps if sustained_rate_rps > 0 else float('inf')

    max_concurrent_requests:int = 400
    request_timeout:float = 60.0 # Timeout para cada request individual en segundos


class OldLimit(RateLimit):
    burst_limit:int = OLD_LIMIT
    sustained_rate_rps:float = OLD_RATE
    delay_between_requests:float = 1.0 / sustained_rate_rps if sustained_rate_rps > 0 else float('inf')

class StorefrontLimit(RateLimit):
    burst_limit:int = STOREFRONT_LIMIT
    sustained_rate_rps:float = STOREFRONT_RATE
    delay_between_requests:float = 1.0 / sustained_rate_rps if sustained_rate_rps > 0 else float('inf')

class BoostLimit(RateLimit):
    burst_limit:int = BOOST_LIMIT
    sustained_rate_rps:float = BOOST_RATE
    delay_between_requests:float = 1.0 / sustained_rate_rps if sustained_rate_rps > 0 else float('inf')

if __name__ == '__main__':
    test = BoostLimit()
    print(test.model_dump())