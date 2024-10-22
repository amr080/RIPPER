import asyncio
import aiohttp
from urllib.parse import urljoin, urlparse, unquote
import logging
from bs4 import BeautifulSoup
import warnings
import json
import re
import platform
from typing import Set, List
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import brotli  # Add this import
import gzip
import zlib
from aiohttp import ClientTimeout
from aiohttp.client_exceptions import ClientError


@dataclass
class CrawlConfig:
    max_depth: int = 4
    max_concurrent: int = 100
    timeout: int = 20
    chunk_size: int = 1000
    retry_attempts: int = 3
    retry_delay: int = 1

    def __post_init__(self):
        self.file_patterns = [
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
            '.txt', '.csv', '.zip', '.gz', '.rar', '.7z',
            'download', 'document', 'file', 'attachment'
        ]

        self.excluded_patterns = [
            '.jpg', '.jpeg', '.png', '.gif', '.css', '.js',
            'tracking', 'analytics', 'advertisement'
        ]

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
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.session = None

    async def create_session(self):
        """Create an aiohttp session with proper configuration"""
        timeout = ClientTimeout(total=self.config.timeout)
        return aiohttp.ClientSession(
            timeout=timeout,
            connector=aiohttp.TCPConnector(
                ssl=False,
                limit=self.config.max_concurrent,
                ttl_dns_cache=300,
                use_dns_cache=True
            ),
            headers=self.get_headers()
        )

    def get_headers(self):
        """Get request headers with rotating user agent"""
        return {
            'User-Agent': self.config.user_agents[len(self.visited) % len(self.config.user_agents)],
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Connection': 'keep-alive'
        }

    async def decompress_content(self, response):
        """Handle various content encodings including brotli"""
        content = await response.read()
        encoding = response.headers.get('Content-Encoding', '').lower()

        try:
            if encoding == 'br':
                return brotli.decompress(content).decode('utf-8')
            elif encoding == 'gzip':
                return gzip.decompress(content).decode('utf-8')
            elif encoding == 'deflate':
                return zlib.decompress(content).decode('utf-8')
            else:
                return content.decode('utf-8')
        except Exception as e:
            print(f"Decompression error ({encoding}): {e}")
            return content.decode('utf-8', errors='ignore')

    async def fetch_with_retry(self, url: str, depth: int):
        """Fetch URL with retry logic"""
        for attempt in range(self.config.retry_attempts):
            try:
                async with self.session.get(url, ssl=False) as response:
                    if response.status == 200:
                        return await self.decompress_content(response), response.headers
                    elif response.status == 429:  # Too Many Requests
                        retry_after = int(response.headers.get('Retry-After', self.config.retry_delay))
                        await asyncio.sleep(retry_after)
                        continue
                    else:
                        print(f"HTTP {response.status} for {url}")
                        return None, None
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt < self.config.retry_attempts - 1:
                    await asyncio.sleep(self.config.retry_delay)
                continue
        return None, None

    async def process_url(self, url: str, depth: int):
        """Process a single URL"""
        if depth > self.config.max_depth or url in self.visited:
            return

        url = self.clean_url(url)
        if not url or url in self.visited:
            return

        self.visited.add(url)
        print(f"Checking: {url}")

        if self.is_file_url(url):
            self.files.add(url)
            print(f"Found file: {url}")
            return

        content, headers = await self.fetch_with_retry(url, depth)
        if not content:
            return

        if headers.get('content-type', '').lower().startswith(('text/html', 'application/xml')):
            urls = await asyncio.get_event_loop().run_in_executor(
                self.executor, self.extract_urls, content, url
            )

            tasks = []
            for new_url in urls:
                if urlparse(new_url).netloc in self.domains:
                    tasks.append(self.process_url(new_url, depth + 1))
                elif self.is_file_url(new_url):
                    clean_url = self.clean_url(new_url)
                    if clean_url:
                        self.files.add(clean_url)
                        print(f"Found file: {clean_url}")

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    def clean_url(self, url: str) -> str:
        """Clean and normalize URLs."""
        try:
            # Remove URL encoding
            url = unquote(url)
            # Remove common artifacts
            url = re.sub(r'["\\\u003e\u0026]+', '', url)
            # Remove trailing punctuation
            url = url.rstrip(',.)')
            # Remove query parameters for file URLs
            if any(ext in url.lower() for ext in self.config.file_patterns):
                url = url.split('?')[0]
            return url
        except:
            return url

    def is_valid_url(self, url: str) -> bool:
        if not url:
            return False
        try:
            parsed = urlparse(url)
            return (bool(parsed.netloc) and
                    parsed.scheme in ['http', 'https'] and
                    not any(p in url.lower() for p in self.config.excluded_patterns))
        except:
            return False

    def is_file_url(self, url: str) -> bool:
        url = url.lower()
        return any(p in url for p in self.config.file_patterns)

    def extract_urls(self, html: str, base_url: str) -> Set[str]:
        urls = set()
        try:
            soup = BeautifulSoup(html, 'lxml')

            # Extract URLs from tags
            for tag in soup.find_all(['a', 'link', 'iframe', 'img', 'source', 'script']):
                url = tag.get('href') or tag.get('src')
                if url:
                    full_url = urljoin(base_url, url)
                    if self.is_valid_url(full_url):
                        urls.add(full_url)

            # Extract URLs from text
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

        except Exception as e:
            print(f"Error extracting URLs from {base_url}: {e}")

        return urls
    async def crawl(self) -> Set[str]:
        """Main crawl method"""
        self.session = await self.create_session()
        try:
            tasks = [self.process_url(url, 0) for url in self.base_urls]
            await asyncio.gather(*tasks)
        finally:
            await self.session.close()
        return self.files


async def main():
    sites = [
        "https://a16z.com/portfolio/",
        "https://a16z.com/"
    ]

    config = CrawlConfig(
        max_depth=4,
        max_concurrent=150,
        timeout=30,
        retry_attempts=3
    )

    crawler = EnhancedWebCrawler(sites, config)
    files = await crawler.crawl()

    clean_files = {crawler.clean_url(f) for f in files}
    clean_files = {f for f in clean_files if f and crawler.is_valid_url(f)}

    results = {
        'files': sorted(clean_files),
        'count': len(clean_files),
        'visited_urls': len(crawler.visited)
    }

    print(f"\nFound {len(clean_files)} files from {results['visited_urls']} URLs:")
    for file in sorted(clean_files):
        print(f"- {file}")

    with open('archive/found_files.json', 'w') as f:
        json.dump(results, f, indent=2)


if __name__ == "__main__":
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())