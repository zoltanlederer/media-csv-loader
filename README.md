# Media CSV Loader

Takes multiple CSV files from different sources (Plex exports, IMDB and TMDB list exports), loads them with pandas, standardises the column names and data types so they match, detects duplicates across sources and combines the best available data into one row per title, merges everything into one clean master CSV, and saves it. This master file becomes the foundation for all remaining projects.

## Requirements

- Python 3.x
- pandas

## Installation

Clone the repo:
```bash
git clone https://github.com/zoltanlederer/media-csv-loader.git
cd media-csv-loader
```

Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage & Options

Usage:
```bash
python3 media_loader.py
```

Options:
```bash
  -h, --help        show this help message and exit
  --plex-movies     add the path of the exported (movie) file from plex
  --plex-tv         add the path of the exported (tv show) file from plex
  --imdb-movies     add the path of the exported (movie) file from imdb
  --imdb-tv         add the path of the exported (tv show) file from imdb
  --tmdb-movies     add the path of the exported (movie) file from tmdb
  --tmdb-tv         add the path of the exported (tv show) file from tmdb
  --output          path and filename to save the master CSV e.g. "master.csv" or "/Users/zoli/data/master.csv"
```

## Examples
```bash
python3 media_loader.py --imdb-tv "IMDB TV Shows.csv" --plex-tv "Plex TV Shows.csv" --imdb-movies "IMDB Movies.csv" --plex-movies "Plex Movies.csv" --output 'master.csv'

🗂️ Loading 4 files...
  — Plex Movies.csv
  — Plex TV Shows.csv
  — IMDB Movies.csv
  — IMDB TV Shows.csv

✏️ Standardising columns...
🔄 Deduplicating...

💾 Master CSV saved to master.csv

📊 Total titles:    3459
    — plex only:    1760
    — imdb only:      70
    — plex_imdb:    1629
```