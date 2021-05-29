import argparse
import generator

parser = argparse.ArgumentParser(description='')
parser.add_argument('--domain', type=str, help='Адрес сайта', required=True)
parser.add_argument('--parse_robots', help='Парсинг robots.txt', action='store_true', default=False, required=False)
parser.add_argument('--max_urls', type=int, default=100_000_000_000_000_000, required=False)
parser.add_argument('--user_agent', type=str, default='*', required=False)
parser.add_argument('--workers', type=int, help='Ограничение по количеству одновременных подключений через TCP', default=30, required=False)
parser.add_argument('--excluded', help='Черный список путей :)', nargs='+', default=set(), required=False)
params = parser.parse_args().__dict__
print(params)
if __name__ == '__main__':
    sitemap = generator.SitemapGenerator(**params)
    sitemap.run()
    sitemap.write_sitemap_xml()