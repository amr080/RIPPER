import asyncio
import aiohttp
import random
import logging
import warnings
import json
import re
import platform
import time
import sys
from typing import Set, List, Dict, Optional
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse, unquote, parse_qs
from bs4 import BeautifulSoup
import brotli
import gzip
import zlib
from aiohttp import ClientTimeout, ClientSession, TCPConnector
from aiohttp.client_exceptions import ClientError
from datetime import datetime
import hashlib
import os
import socket
from fake_useragent import UserAgent
import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class CrawlConfig:
    max_depth: int = 4
    max_concurrent: int = 100
    timeout: int = 30
    chunk_size: int = 1000
    retry_attempts: int = 5
    retry_delay: int = 2
    request_delay: tuple = (1, 3)
    respect_robots: bool = True
    follow_links: bool = True
    download_files: bool = True
    verify_ssl: bool = False
    save_state: bool = True
    state_file: str = 'crawler_state.json'
    output_dir: str = 'downloads'

    def __post_init__(self):
        self.file_patterns: List[str] = [
            # Document files
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
            '.txt', '.rtf', '.csv', '.odt', '.ods', '.odp',
            # Archive files
            '.zip', '.gz', '.rar', '.7z', '.tar', '.bz2',
            # Data files
            '.json', '.xml', '.yaml', '.sql', '.db',
            # Common patterns
            'download', 'document', 'file', 'attachment', 'export',
            'backup', 'dump', 'archive', 'report'
        ]

        self.content_types: List[str] = [
            'application/pdf',
            'application/msword',
            'application/vnd.openxmlformats-officedocument',
            'text/plain',
            'application/zip',
            'application/x-rar-compressed',
            'application/x-7z-compressed',
            'application/json',
            'application/xml'
        ]

        self.excluded_patterns: List[str] = [
            # Media files
            '.jpg', '.jpeg', '.png', '.gif', '.svg', '.ico', '.webp',
            '.mp3', '.mp4', '.wav', '.avi', '.mov',
            # Web assets
            '.css', '.js', '.woff', '.woff2', '.ttf', '.eot',
            # Tracking and ads
            'tracking', 'analytics', 'advertisement', 'pixel',
            'banner', 'click', 'track', 'log.',
            # Social media
            'facebook', 'twitter', 'linkedin', 'instagram',
            # Common excludes
            'comment', 'feed', 'rss', 'sitemap'
        ]

        # Enhanced browser simulation
        self.browser_headers: Dict[str, str] = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'sec-ch-ua': '"Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }

        # Cookie handling
        self.cookies: Dict[str, str] = {
            'wordpress_test_cookie': 'WP Cookie check',
            'session': hashlib.md5(str(time.time()).encode()).hexdigest(),
            'consent': 'true',
            'timezone': 'UTC'
        }

        # Proxy configuration
        self.proxy_settings: Dict[str, str] = {
            'http': os.getenv('HTTP_PROXY', ''),
            'https': os.getenv('HTTPS_PROXY', '')
        }

        # Rate limiting
        self.rate_limits: Dict[str, int] = {
            'requests_per_second': 2,
            'max_requests_per_domain': 100,
            'max_retries': 5
        }

        try:
            self.user_agent = UserAgent()
        except:
            self.user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
            ]


class EnhancedWebCrawler:
    def __init__(self, base_urls: List[str], config: CrawlConfig = None):
        self.base_urls = base_urls
        self.config = config or CrawlConfig()
        self.domains = {urlparse(url).netloc for url in base_urls}
        self.visited = set()
        self.files = set()
        self.failed_urls = set()
        self.rate_limits = {}
        self.session = None
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.start_time = datetime.now()

        # Initialize directories
        os.makedirs(self.config.output_dir, exist_ok=True)

        # State management
        self.state = self.load_state() if self.config.save_state else {}

        # Domain-specific information
        self.domain_info = {domain: {
            'is_wordpress': None,
            'api_endpoints': set(),
            'robots_txt': None,
            'last_request_time': 0,
            'request_count': 0
        } for domain in self.domains}

    async def create_session(self):
        """Create an enhanced aiohttp session with proper configuration"""
        timeout = ClientTimeout(total=self.config.timeout)
        connector = TCPConnector(
            ssl=self.config.verify_ssl,
            limit=self.config.max_concurrent,
            ttl_dns_cache=300,
            force_close=True,
            enable_cleanup_closed=True
        )

        # Create session with enhanced settings
        session = ClientSession(
            timeout=timeout,
            connector=connector,
            headers=self.get_headers(),
            cookies=self.config.cookies,
            trust_env=True
        )

        # Add proxy if configured
        if any(self.config.proxy_settings.values()):
            session._proxy = self.config.proxy_settings
            session._proxy_auth = None

        return session

    def get_headers(self) -> Dict[str, str]:
        """Get randomized headers for browser simulation"""
        try:
            user_agent = self.config.user_agent.random
        except:
            user_agent = random.choice(self.config.user_agents)

        headers = {
            'User-Agent': user_agent,
            **self.config.browser_headers
        }

        # Add random viewport and screen resolution
        headers['Viewport-Width'] = str(random.choice([1024, 1366, 1920]))
        headers['Screen-Resolution'] = random.choice(['1920x1080', '1366x768', '2560x1440'])

        return headers

    async def detect_wordpress(self, domain: str) -> bool:
        """Detect if a domain is running WordPress"""
        wp_paths = ['/wp-login.php', '/wp-admin/', '/wp-content/', '/wp-includes/']
        for path in wp_paths:
            url = f"https://{domain}{path}"
            try:
                async with self.session.head(url, allow_redirects=False) as response:
                    if response.status in [200, 301, 302, 403]:
                        self.domain_info[domain]['is_wordpress'] = True
                        return True
            except:
                continue
        self.domain_info[domain]['is_wordpress'] = False
        return False

    async def handle_rate_limits(self, domain: str):
        """Implement rate limiting per domain"""
        current_time = time.time()
        domain_info = self.domain_info[domain]

        # Check domain request count
        if domain_info['request_count'] >= self.config.rate_limits['max_requests_per_domain']:
            raise Exception(f"Rate limit exceeded for domain: {domain}")

        # Calculate delay
        time_since_last = current_time - domain_info['last_request_time']
        if time_since_last < 1 / self.config.rate_limits['requests_per_second']:
            await asyncio.sleep(random.uniform(*self.config.request_delay))

        domain_info['last_request_time'] = current_time
        domain_info['request_count'] += 1

    async def decompress_content(self, response) -> str:
        """Enhanced content decompression with multiple fallbacks"""
        content = await response.read()
        encoding = response.headers.get('Content-Encoding', '').lower()

        decompression_methods = {
            'br': lambda x: brotli.decompress(x).decode('utf-8'),
            'gzip': lambda x: gzip.decompress(x).decode('utf-8'),
            'deflate': lambda x: zlib.decompress(x).decode('utf-8')
        }

        try:
            if encoding in decompression_methods:
                try:
                    return decompression_methods[encoding](content)
                except:
                    # Fallback decompression attempts
                    for method in decompression_methods.values():
                        try:
                            return method(content)
                        except:
                            continue

            # Last resort: try direct decoding
            return content.decode('utf-8', errors='ignore')

        except Exception as e:
            logger.error(f"Decompression error ({encoding}): {e}")
            return content.decode('utf-8', errors='ignore')

    async def fetch_with_retry(self, url: str, depth: int) -> tuple:
        """Enhanced URL fetching with advanced retry logic"""
        domain = urlparse(url).netloc
        await self.handle_rate_limits(domain)

        for attempt in range(self.config.retry_attempts):
            try:
                async with self.session.get(
                        url,
                        allow_redirects=True,
                        verify_ssl=self.config.verify_ssl,
                        timeout=self.config.timeout
                ) as response:

                    if response.status == 200:
                        content = await self.decompress_content(response)
                        return content, response.headers

                    elif response.status in [301, 302, 303, 307, 308]:
                        redirect_url = response.headers.get('Location')
                        if redirect_url:
                            return await self.fetch_with_retry(redirect_url, depth)

                    elif response.status == 429:  # Too Many Requests
                        retry_after = int(response.headers.get('Retry-After', self.config.retry_delay))
                        await asyncio.sleep(retry_after)
                        continue

                    else:
                        logger.warning(f"HTTP {response.status} for {url}")
                        self.failed_urls.add(url)
                        return None, None

            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt < self.config.retry_attempts - 1:
                    await asyncio.sleep(self.config.retry_delay * (attempt + 1))  # Exponential backoff
                continue

        self.failed_urls.add(url)
        return None, None

    async def process_url(self, url: str, depth: int):
        """Enhanced URL processing with content analysis"""
        if depth > self.config.max_depth or url in self.visited:
            return

        url = self.clean_url(url)
        if not url or url in self.visited:
            return

        self.visited.add(url)
        logger.info(f"Processing: {url}")

        # Check domain-specific information
        domain = urlparse(url).netloc
        if self.domain_info[domain]['is_wordpress'] is None:
            await self.detect_wordpress(domain)

        if self.is_file_url(url):
            await self.process_file_url(url)
            return

        content, headers = await self.fetch_with_retry(url, depth)
        if not content:
            return

        # Process based on content type
        content_type = headers.get('content-type', '').lower()

        if any(ct in content_type for ct in self.config.content_types):
            await self.process_file_content(url, content, content_type)
            return

        if content_type.startswith(('text/html', 'application/xml', 'application/json')):
            urls = await self.extract_urls(content, url, content_type)

            tasks = []
            for new_url in urls:
                parsed_new_url = urlparse(new_url)
                if parsed_new_url.netloc in self.domains:
                    tasks.append(self.process_url(new_url, depth + 1))
                elif self.is_file_url(new_url):
                    tasks.append(self.process_file_url(new_url))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def process_file_url(self, url: str):
        """Handle file URL discovery and processing"""
        clean_url = self.clean_url(url)
        if clean_url and clean_url not in self.files:
            self.files.add(clean_url)
            logger.info(f"Found file: {clean_url}")

            if self.config.download_files:
                await self.download_file(clean_url)

    async def download_file(self, url: str):
        """Download and save discovered files"""
        try:
            async with self.session.get(url, allow_redirects=True) as response:
                if response.status == 200:
                    filename = os.path.join(
                        self.config.output_dir,
                        hashlib.md5(url.encode()).hexdigest() + '_' +
                        os.path.basename(urlparse(url).path) or 'unnamed_file'
                    )

                    with open(filename, 'wb') as f:
                        while True:
                            chunk = await response.content.read(self.config.chunk_size)
                            if not chunk:
                                break
                            f.write(chunk)

                    logger.info(f"Downloaded: {url} -> {filename}")
        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")

    async def extract_urls(self, content: str, base_url: str, content_type: str) -> Set[str]:
        """Enhanced URL extraction with content-type specific handling"""
        urls = set()

        try:
            if 'json' in content_type:
                # Handle JSON content
                try:
                    data = json.loads(content)
                    urls.update(self.extract_urls_from_json(data))
                except json.JSONDecodeError:
                    pass
            else:
                # Handle HTML/XML content
                soup = BeautifulSoup(content, 'lxml')

                # Extract URLs from various HTML elements
                for tag in soup.find_all(['a', 'link', 'iframe', 'img', 'source', 'script']):
                    url = tag.get('href') or tag.get('src')
                    if url:
                        full_url = urljoin(base_url, url)
                        if self.is_valid_url(full_url):
                            urls.add(full_url)

                # Extract URLs from text content
                text_urls = re.findall(r'https?://[^\s<>"]+|www\.[^\s<>"]+', str(soup))
                for url in text_urls:
                    if not url.startswith('http'):
                        url = f'http://{url}'
                    if self.is_valid_url(url):
                        urls.add(url)

                # Extract URLs from data attributes
                for tag in soup.find_all(attrs={'data-url': True}):
                    url = tag.get('data-url')
                    if url:
                        full_url = urljoin(base_url, url)
                        if self.is_valid_url(full_url):
                            urls.add(full_url)

                # Extract URLs from inline styles
                style_urls = re.findall(r'url\(["\']?(.*?)["\']?\)', str(soup))
                for url in style_urls:
                    full_url = urljoin(base_url, url)
                    if self.is_valid_url(full_url):
                        urls.add(full_url)

                # Extract potential API endpoints
                api_patterns = [
                    r'/api/v\d+/',
                    r'/wp-json/',
                    r'/rest/',
                    r'/graphql',
                    r'/endpoint/'
                ]
                for pattern in api_patterns:
                    api_urls = re.findall(pattern, str(soup))
                    for url in api_urls:
                        full_url = urljoin(base_url, url)
                        if self.is_valid_url(full_url):
                            urls.add(full_url)
                            self.domain_info[urlparse(base_url).netloc]['api_endpoints'].add(full_url)

        except Exception as e:
            logger.error(f"Error extracting URLs from {base_url}: {e}")

        return urls

    def extract_urls_from_json(self, data) -> Set[str]:
        """Extract URLs from JSON content"""
        urls = set()
        if isinstance(data, dict):
            for value in data.values():
                if isinstance(value, str) and (value.startswith('http://') or value.startswith('https://')):
                    if self.is_valid_url(value):
                        urls.add(value)
                elif isinstance(value, (dict, list)):
                    urls.update(self.extract_urls_from_json(value))
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
                    urls.update(self.extract_urls_from_json(item))
                elif isinstance(item, str) and (item.startswith('http://') or item.startswith('https://')):
                    if self.is_valid_url(item):
                        urls.add(item)
        return urls

    def clean_url(self, url: str) -> str:
        """Enhanced URL cleaning and normalization"""
        try:
            url = unquote(url)
            url = re.sub(r'["\\\u003e\u0026]+', '', url)
            url = url.rstrip(',.)')

            # Remove tracking parameters
            parsed = urlparse(url)
            query = parse_qs(parsed.query)

            # Remove common tracking parameters
            tracking_params = {'utm_', 'ref_', 'source', 'campaign', 'medium', 'content'}
            query = {k: v for k, v in query.items() if not any(k.startswith(t) for t in tracking_params)}

            # Reconstruct URL
            clean_url = parsed._replace(query=urlencode(query, doseq=True)).geturl()

            # Remove query parameters for file URLs
            if any(ext in clean_url.lower() for ext in self.config.file_patterns):
                clean_url = clean_url.split('?')[0]

            return clean_url
        except:
            return url

    def is_valid_url(self, url: str) -> bool:
        """Enhanced URL validation"""
        if not url:
            return False
        try:
            parsed = urlparse(url)
            return all([
                bool(parsed.netloc),
                parsed.scheme in ['http', 'https'],
                not any(p in url.lower() for p in self.config.excluded_patterns),
                len(url) < 2000  # Max URL length
            ])
        except:
            return False

    def is_file_url(self, url: str) -> bool:
        """Enhanced file URL detection"""
        url_lower = url.lower()
        return any(p in url_lower for p in self.config.file_patterns)

    def save_state(self):
        """Save crawler state to file"""
        if self.config.save_state:
            state = {
                'visited': list(self.visited),
                'files': list(self.files),
                'failed_urls': list(self.failed_urls),
                'domain_info': {k: {**v, 'api_endpoints': list(v['api_endpoints'])}
                                for k, v in self.domain_info.items()},
                'timestamp': datetime.now().isoformat()
            }
            with open(self.config.state_file, 'w') as f:
                json.dump(state, f, indent=2)

    def load_state(self) -> dict:
        """Load crawler state from file"""
        try:
            if os.path.exists(self.config.state_file):
                with open(self.config.state_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error loading state: {e}")
        return {}

    async def crawl(self) -> Set[str]:
        """Main crawl method with enhanced error handling and state management"""
        self.session = await self.create_session()
        try:
            tasks = [self.process_url(url, 0) for url in self.base_urls]
            await asyncio.gather(*tasks)

            # Save final state
            self.save_state()

            return self.files
        finally:
            await self.session.close()


async def main():
    # Configure sites to crawl
    sites = [
        "https://panteracapital.com/portfolio/",
        "https://panteracapital.com/"
    ]

    # Enhanced configuration
    config = CrawlConfig(
        max_depth=4,
        max_concurrent=150,
        timeout=30,
        retry_attempts=5,
        request_delay=(1, 3),
        download_files=True,
        save_state=True,
        output_dir='downloads'
    )

    # Initialize and run crawler
    try:
        crawler = EnhancedWebCrawler(sites, config)
        files = await crawler.crawl()

        # Process results
        clean_files = {crawler.clean_url(f) for f in files}
        clean_files = {f for f in clean_files if f and crawler.is_valid_url(f)}

        # Prepare detailed results
        results = {
            'files': sorted(clean_files),
            'count': len(clean_files),
            'visited_urls': len(crawler.visited),
            'failed_urls': list(crawler.failed_urls),
            'wordpress_sites': {domain: info['is_wordpress']
                                for domain, info in crawler.domain_info.items()},
            'api_endpoints': {domain: list(info['api_endpoints'])
                              for domain, info in crawler.domain_info.items()},
            'execution_time': str(datetime.now() - crawler.start_time)
        }

        # Output results
        print(f"\nCrawl Summary:")
        print(f"Execution Time: {results['execution_time']}")
        print(f"Found {len(clean_files)} files from {results['visited_urls']} URLs")
        print(f"Failed URLs: {len(crawler.failed_urls)}")

        print("\nDiscovered Files:")
        for file in sorted(clean_files):
            print(f"- {file}")

        print("\nFailed URLs:")
        for failed_url in sorted(crawler.failed_urls):
            print(f"- {failed_url}")

        # Save detailed results
        with open('archive/found_files.json', 'w') as f:
            json.dump(results, f, indent=2)

    except Exception as e:
        logger.error(f"Crawler error: {e}")
        raise


if __name__ == "__main__":
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
