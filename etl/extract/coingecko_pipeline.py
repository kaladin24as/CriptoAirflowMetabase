import dlt
import requests
from typing import Iterator, Dict, Any, List
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

load_dotenv()

COINGECKO_API_BASE = "https://api.coingecko.com/api/v3"


class CoinGeckoAPI:
    
    def __init__(self):
        self.base_url = COINGECKO_API_BASE
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'CryptoDataPipeline/1.0'
        })
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        url = f"{self.base_url}/{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {endpoint}: {e}")
            raise
    
    def get_market_data(self, vs_currency: str = 'usd', per_page: int = 250) -> List[Dict]:
        params = {
            'vs_currency': vs_currency,
            'order': 'market_cap_desc',
            'per_page': per_page,
            'page': 1,
            'sparkline': False,
            'price_change_percentage': '1h,24h,7d,30d'
        }
        return self._make_request('coins/markets', params)
    
    def get_trending(self) -> Dict:
        return self._make_request('search/trending')
    
    def get_global_data(self) -> Dict:
        return self._make_request('global')


@dlt.resource(name="market_data", write_disposition="merge", primary_key="id")
def extract_market_data(api: CoinGeckoAPI) -> Iterator[Dict[str, Any]]:
    print("Extracting market data from CoinGecko...")
    
    data = api.get_market_data(per_page=250)
    
    for coin in data:
        coin['extracted_at'] = datetime.now(timezone.utc).isoformat()
        coin['last_updated_dt'] = datetime.fromisoformat(
            coin['last_updated'].replace('Z', '+00:00')
        ).isoformat() if coin.get('last_updated') else None
        
        yield coin
    
    print(f"✓ Extracted {len(data)} cryptocurrencies")


@dlt.resource(name="trending_coins", write_disposition="replace")
def extract_trending(api: CoinGeckoAPI) -> Iterator[Dict[str, Any]]:
    print("Extracting trending coins...")
    
    data = api.get_trending()
    
    if 'coins' in data:
        for idx, item in enumerate(data['coins'], 1):
            coin = item.get('item', {})
            yield {
                'rank': idx,
                'id': coin.get('id'),
                'coin_id': coin.get('coin_id'),
                'name': coin.get('name'),
                'symbol': coin.get('symbol'),
                'market_cap_rank': coin.get('market_cap_rank'),
                'thumb': coin.get('thumb'),
                'small': coin.get('small'),
                'large': coin.get('large'),
                'slug': coin.get('slug'),
                'price_btc': coin.get('price_btc'),
                'score': coin.get('score'),
                'extracted_at': datetime.now(timezone.utc).isoformat()
            }
        print(f"✓ Extracted {len(data['coins'])} trending coins")


@dlt.resource(name="global_stats", write_disposition="append")
def extract_global_stats(api: CoinGeckoAPI) -> Iterator[Dict[str, Any]]:
    print("Extracting global market statistics...")
    
    response = api.get_global_data()
    data = response.get('data', {})
    
    market_cap_percentage = data.get('market_cap_percentage', {})
    
    yield {
        'active_cryptocurrencies': data.get('active_cryptocurrencies'),
        'upcoming_icos': data.get('upcoming_icos'),
        'ongoing_icos': data.get('ongoing_icos'),
        'ended_icos': data.get('ended_icos'),
        'markets': data.get('markets'),
        'total_market_cap_usd': data.get('total_market_cap', {}).get('usd'),
        'total_volume_usd': data.get('total_volume', {}).get('usd'),
        'market_cap_change_percentage_24h': data.get('market_cap_change_percentage_24h_usd'),
        'btc_dominance': market_cap_percentage.get('btc'),
        'eth_dominance': market_cap_percentage.get('eth'),
        'updated_at': data.get('updated_at'),
        'extracted_at': datetime.now(timezone.utc).isoformat()
    }
    
    print("✓ Extracted global statistics")


@dlt.source
def coingecko_source() -> list:
    api = CoinGeckoAPI()
    
    return [
        extract_market_data(api),
        extract_trending(api),
        extract_global_stats(api)
    ]


def run_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="coingecko_crypto",
        destination="postgres",
        dataset_name="crypto_raw",
        progress="log"
    )
    
    db_host = os.getenv('DATA_DB_HOST', 'localhost')
    db_port = os.getenv('DATA_DB_PORT', '5432')
    db_name = os.getenv('DATA_DB_NAME', 'warehouse')
    db_user = os.getenv('DATA_DB_USER', 'postgres')
    db_password = os.getenv('DATA_DB_PASSWORD', 'postgres')
    
    credentials = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    print("=" * 60)
    print("CoinGecko Cryptocurrency Data Pipeline")
    print("=" * 60)
    print(f"Database: {db_host}:{db_port}/{db_name}")
    print(f"Dataset: crypto_raw")
    print("=" * 60)
    
    load_info = pipeline.run(
        coingecko_source(),
        credentials=credentials
    )
    
    print("\n" + "=" * 60)
    print("Pipeline Execution Summary")
    print("=" * 60)
    print(f"Status: {load_info}")
    print("=" * 60)
    
    return load_info


if __name__ == "__main__":
    try:
        load_info = run_pipeline()
        print("\n✓ Pipeline completed successfully!")
    except Exception as e:
        print(f"\n✗ Pipeline failed: {e}")
        raise
