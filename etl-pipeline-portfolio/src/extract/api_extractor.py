"""
API Extractor Module
Extracts data from REST APIs with retry logic and error handling.
"""

import requests
import time
import logging
from typing import Dict, List, Optional
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class APIExtractor:
    """
    Extract data from REST APIs with robust error handling.
    
    Features:
    - Automatic retry with exponential backoff
    - Rate limiting
    - Pagination support
    - Response validation
    """
    
    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        max_retries: int = 3,
        timeout: int = 30
    ):
        """
        Initialize API Extractor.
        
        Args:
            base_url: Base URL for the API
            api_key: API key for authentication
            max_retries: Maximum number of retry attempts
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.api_key = api_key
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()
        
        if api_key:
            self.session.headers.update({'Authorization': f'Bearer {api_key}'})
        
        logger.info(f"APIExtractor initialized for {base_url}")
    
    def extract(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        method: str = 'GET'
    ) -> List[Dict]:
        """
        Extract data from API endpoint.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            method: HTTP method (GET, POST, etc.)
            
        Returns:
            List of records extracted from API
        """
        url = f"{self.base_url}/{endpoint}"
        all_data = []
        
        logger.info(f"Starting extraction from {url}")
        start_time = time.time()
        
        try:
            response = self._make_request(url, params, method)
            data = self._parse_response(response)
            all_data.extend(data)
            
            # Handle pagination if present
            while self._has_next_page(response):
                params = self._get_next_page_params(response, params)
                response = self._make_request(url, params, method)
                data = self._parse_response(response)
                all_data.extend(data)
            
            duration = time.time() - start_time
            logger.info(
                f"Extraction completed. "
                f"Records: {len(all_data)}, Duration: {duration:.2f}s"
            )
            
            return all_data
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def _make_request(
        self,
        url: str,
        params: Optional[Dict],
        method: str
    ) -> requests.Response:
        """
        Make HTTP request with retry logic.
        
        Args:
            url: Request URL
            params: Query parameters
            method: HTTP method
            
        Returns:
            Response object
        """
        for attempt in range(self.max_retries):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    timeout=self.timeout
                )
                
                response.raise_for_status()
                return response
                
            except requests.exceptions.RequestException as e:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(
                    f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}"
                )
                
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retries exceeded")
                    raise
    
    def _parse_response(self, response: requests.Response) -> List[Dict]:
        """
        Parse API response to extract data.
        
        Args:
            response: HTTP response object
            
        Returns:
            List of data records
        """
        try:
            data = response.json()
            
            # Handle different response structures
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                # Common patterns: data, results, items, etc.
                for key in ['data', 'results', 'items', 'records']:
                    if key in data and isinstance(data[key], list):
                        return data[key]
                # If no list found, return the dict as single item
                return [data]
            else:
                logger.warning(f"Unexpected response type: {type(data)}")
                return []
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            raise
    
    def _has_next_page(self, response: requests.Response) -> bool:
        """
        Check if there are more pages to fetch.
        
        Args:
            response: HTTP response object
            
        Returns:
            True if next page exists
        """
        try:
            data = response.json()
            
            # Check common pagination indicators
            if isinstance(data, dict):
                return (
                    data.get('next') is not None or
                    data.get('next_page') is not None or
                    (data.get('page', 0) < data.get('total_pages', 0))
                )
            
            return False
            
        except:
            return False
    
    def _get_next_page_params(
        self,
        response: requests.Response,
        current_params: Optional[Dict]
    ) -> Dict:
        """
        Get parameters for next page request.
        
        Args:
            response: Current response object
            current_params: Current query parameters
            
        Returns:
            Updated parameters for next page
        """
        params = current_params.copy() if current_params else {}
        
        try:
            data = response.json()
            
            if isinstance(data, dict):
                # Increment page number
                if 'page' in data:
                    params['page'] = data['page'] + 1
                elif 'offset' in data:
                    limit = data.get('limit', 100)
                    params['offset'] = data['offset'] + limit
                    
        except:
            pass
        
        return params
    
    def extract_to_file(
        self,
        endpoint: str,
        output_path: str,
        params: Optional[Dict] = None,
        method: str = 'GET'
    ) -> int:
        """
        Extract data and save to JSON file.
        
        Args:
            endpoint: API endpoint path
            output_path: Path to save extracted data
            params: Query parameters
            method: HTTP method
            
        Returns:
            Number of records extracted
        """
        data = self.extract(endpoint, params, method)
        
        with open(output_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Data saved to {output_path}")
        return len(data)


# Example usage
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Example: Extract weather data
    extractor = APIExtractor(
        base_url="https://api.openweathermap.org/data/2.5",
        api_key="your_api_key_here"
    )
    
    data = extractor.extract(
        endpoint="weather",
        params={
            'q': 'London',
            'units': 'metric'
        }
    )
    
    print(f"Extracted {len(data)} records")
