Extractor

This folder contains two workflows:

1) Extract endpoints from calibre recipes into a sources SQLite DB.
2) Fetch news into a news SQLite DB using either recipe mode or RSS fallback.

Quick start

1) Create a venv (optional)
   python -m venv .venv
   .venv/bin/python -V

2) Extract sources
   .venv/bin/python extract_sources.py --verbose

3) Fetch news (one-shot)
   .venv/bin/python fetch_news.py --mode auto --verbose

4) Fetch news every 10 minutes
   .venv/bin/python fetch_news.py --mode auto --interval 600 --verbose

Notes

- Recipe mode uses calibre's recipe classes and download logic. It is closer to what calibre can parse.
- Recipe mode may require extra dependencies such as mechanize, feedparser, and lxml.
- RSS mode uses a simple XML parser and only fetches feed URLs.
- Both modes write into extractor/data/news.db and dedupe articles by fingerprint.

DB locations

- extractor/data/sources.db
- extractor/data/news.db
