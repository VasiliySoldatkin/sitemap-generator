import asyncio
import aiohttp
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser
from urllib.parse import urlsplit, urlunsplit
import re
import time
from xmltodict import unparse
import logging

class SitemapGenerator:
    def __init__(self, url, parse_robots=False, max_urls=5_000):
        self.num_urls = 0
        self.num_crawled = 0
        self.count_xmls = 0
        self.max_urls = max_urls
        self.parse_robots = parse_robots
        self.not_parseable = (
            ".epub", ".mobi", ".docx", ".doc", ".opf", ".7z", ".ibooks", ".cbr", ".avi", ".mkv", ".mp4", ".jpg",
            ".jpeg",
            ".png", ".gif", ".pdf", ".iso", ".rar", ".tar", ".tgz", ".zip", ".dmg", ".exe")

        self.url = url
        self.url_parsed = urlparse(url)
        if not self.url_parsed.path.endswith('/'):
            self.url = url + '/'
        self.urls_to_crawl = []
        self.crawled_or_crawling = set()
        self.for_sitemap = []
        self.url_info = {}

        self.url_parsed = urlparse(url)
        self.target_domain = self.url_parsed.netloc
        self.target_scheme = self.url_parsed.scheme
        self.domain = self.url_parsed.scheme + '://' + self.target_domain
        self.excluded = set()

        if self.parse_robots:
            self.robots()
        self.session = None
        self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
        self.t = 0

    async def gather_with_concurrency(self, n, *tasks):
        semaphore = asyncio.Semaphore(n)
        async def sem_task(task):
            async with semaphore:
                return await task

        return await asyncio.gather(*(sem_task(task) for task in tasks))

    def run(self):
        self.t = time.time()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.BFS())
        print(self.num_urls, time.time() - self.t)

    def gen_of_urls(self):
        while len(self.urls_to_crawl) > 0:
            g = self.urls_to_crawl.pop()
            self.crawled_or_crawling.add(g)
            self.for_sitemap.append(g)
            self.num_crawled += 1
            self.url_info[g] = {'loc': g}
            yield g

    async def BFS(self):
        self.urls_to_crawl.append(self.url)

        connector = aiohttp.TCPConnector(limit=30)
        timeout_seconds = 10
        session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=timeout_seconds, sock_read=timeout_seconds)
        self.session = aiohttp.ClientSession(connector=connector,
                                             trust_env=True,
                                             timeout=session_timeout,
                                             cookie_jar=aiohttp.CookieJar())

        while len(self.urls_to_crawl) > 0:
            tasks = [asyncio.create_task(self.crawl(url)) for url in self.gen_of_urls()]
            await self.gather_with_concurrency(30, *tasks)
        await self.session.close()

    async def crawl(self, current_url):
        if self.num_crawled > self.max_urls:
            return
        linkregex = re.compile(b'(?i)href=["\']?([^\s"\'<>]+)')
        url = urlparse(current_url)
        if not url.path.endswith(self.not_parseable):
            try:
                response = await self.session.get(current_url,
                                                  headers={'User-Agent': self.user_agent},
                                                  ssl=False,
                                                  allow_redirects=False)
            except Exception as e:
                print(current_url, e)
                self.excluded.add(current_url)
                return
            else:
                if 200 <= response.status <= 307:
                    try:
                        html = await response.read()
                    except Exception as e:
                        print(current_url, e)
                        return
                else:
                    logging.exception(f'HTTP Status code {response.status}')
                    return
                self.url_info[current_url]['loc'] = current_url
                try:
                    self.url_info[current_url]['lastmod'] = response.headers['Last-Modified']
                except KeyError:
                    pass
                links = linkregex.findall(html)
                for link in links:
                    link = link.decode("utf-8", errors="ignore")
                    if link.startswith('/'):
                        link = url.scheme + '://' + url[1] + link
                    elif link.startswith('#'):
                        link = url.scheme + '://' + url[1] + url[2] + link
                    elif link.startswith(("mailto", "tel")):
                        continue
                    elif not link.startswith(('http', "https")):
                        link = self.clean_link(urljoin(current_url, link))

                    if "#" in link:
                        link = link[:link.index('#')]

                    parsed_link = urlparse(link)
                    domain_link = parsed_link.netloc
                    if link in self.crawled_or_crawling:
                        continue
                    if link in self.urls_to_crawl:
                        continue
                    if domain_link != self.target_domain:
                        continue

                    if parsed_link.path in ["", "/"] and parsed_link.query == '':
                        continue
                    if "javascript" in link:
                        continue
                    if self.parse_robots:
                        if link in self.excluded:
                            continue
                        if self.in_excluded_link(link):
                            continue
                        if not self.can_fetch_robots(link):
                            continue
                    self.urls_to_crawl.append(link)
                    self.num_urls += 1
                    if self.num_urls % 100 == 0:
                        print(time.time() - self.t, self.num_urls)

    def clean_link(self, link):
        parts = list(urlsplit(link))
        parts[2] = self.resolve_url_path(parts[2])
        return urlunsplit(parts)

    @staticmethod
    def resolve_url_path(path):
        segments = path.split('/')
        segments = [segment + '/' for segment in segments[:-1]] + [segments[-1]]
        resolved = []
        for segment in segments:
            if segment in ('../', '..'):
                if resolved[1:]:
                    resolved.pop()
            elif segment not in ('./', '.'):
                resolved.append(segment)
        return ''.join(resolved)

    @staticmethod
    def tab_string(string):
        s = string.split('\n')
        tabulated_string = ''
        for line in s:
            tabulated_string += '\t' + line + '\n'
        return tabulated_string

    def sitemap_end(self, sitemap_locs, xml):
        locs = self.tab_string(sitemap_locs)
        sitemapindex_close_tag = '</sitemapindex>'
        xml += locs
        xml += sitemapindex_close_tag
        file_name = f'{self.target_domain}({self.count_xmls}).xml' if self.count_xmls > 0 else f'{self.target_domain}.xml'
        with open(file_name, 'w') as f:
            f.write(xml)
            self.count_xmls += 1

    def write_sitemap_xml(self):
        print(len(self.for_sitemap))
        xml = '<?xml version=\"1.0\" encoding=\"utf-8\"?>\n'
        sitemapindex_open_tag = '<sitemapindex xmlns="http://www.google.com/schemas/sitemap/0.84">'
        xml += sitemapindex_open_tag
        sitemap_locs = ''
        urls_in_xml = 0
        count_xmls = 0
        for link in self.for_sitemap:
            link_dict = {'sitemap': [
                self.url_info[link]]}
            xml_link = unparse(link_dict, pretty=True).split('\n', 1)[1]
            sitemap_locs += '\n' + xml_link
            urls_in_xml += 1
            if urls_in_xml % 50000 == 0:
                self.sitemap_end(sitemap_locs, xml)
                print(count_xmls)
                xml = '<?xml version=\"1.0\" encoding=\"utf-8\"?>\n'
                xml += sitemapindex_open_tag
                sitemap_locs = ''
        if urls_in_xml % 50000 != 0:
            self.sitemap_end(sitemap_locs, xml)
        """sitemap_locs = self.tab_string(sitemap_locs)
        sitemapindex_close_tag = '</sitemapindex>'
        xml += sitemap_locs
        xml += sitemapindex_close_tag
        count_xmls += 1
        with open(f'{self.target_domain}{count_xmls}.xml', 'w') as f:
            f.write(xml)"""


    def robots(self):
        robots_url = urljoin(self.domain, "robots.txt")
        self.rob_pars = RobotFileParser()
        self.rob_pars.set_url(robots_url)
        self.rob_pars.read()


    def can_fetch_robots(self, link):
        if self.rob_pars.can_fetch(self.user_agent, link):
            return True
        if link not in self.excluded:
            self.excluded.add(link)
        return False


    def in_excluded_link(self, link):
        for l in self.excluded:
            if l in link:
                return True
        return False


if __name__ == '__main__':
    import sys
    if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
        policy = asyncio.WindowsSelectorEventLoopPolicy()
        asyncio.set_event_loop_policy(policy)
    c = SitemapGenerator('https://vk.com', parse_robots=True)
    c.run()
    c.write_sitemap_xml()
