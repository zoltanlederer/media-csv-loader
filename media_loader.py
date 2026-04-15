"""
Media CSV Loader

Takes multiple CSV files from different sources (Plex exports, IMDB list exports, and local folder scans). loads them with pandas, standardises the column names and data types so they match, detects duplicates (titles that appear in both lists), merges everything into one clean master CSV, and saves it. This master file becomes the foundation for all remaining projects.
"""

import sys
import argparse
import pandas as pd


def load_csv(filepath):
    """ Load csv file including error handling """
    try:
        return pd.read_csv(filepath)
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        sys.exit()


def standardise_columns(df, source):
    """ Create the standard column names for the Master CSV """
    df_renamed = df.rename(columns={
        'Const': 'imdb_id',
        'Title': 'title',
        'Year': 'year',
        'Genres': 'genres',
        'IMDb Rating': 'imdb_rating',
        'Runtime (mins)': 'runtime_mins',
        'Title Type': 'type',
        'duration': 'runtime_mins'
    })
    
    df_renamed['source'] = source # add source column e.g. plex_movies, plex_tv, imdb_movies, imdb_tv, local_movies, local_tv
    # print(df_renamed['type'])
    # print(source)
    if 'tv' in source.lower():
        df_renamed['type'] = 'tv_show'
    
    if 'movie' in source.lower():
        df_renamed['type'] = 'movie'

    return df_renamed


# main logic
parser = argparse.ArgumentParser(description='The program creates a master csv file from different sources.')
parser.add_argument('--plex-movies', help='add the path of the exported (movie) file from plex')
parser.add_argument('--plex-tv', help='add the path of the exported (tv show) file from plex')
parser.add_argument('--imdb-movies', help='add the path of the exported (movie) file from imdb')
parser.add_argument('--imdb-tv', help='add the path of the exported (tv show) file from imdb')
parser.add_argument('--local-movies', help='add the path of the exported (movie) file from local scan')
parser.add_argument('--local-tv', help='add the path of the exported (tv show) file from local scan')
args = parser.parse_args()


# Exit if no files were provided
file_count = 0
for key, value in vars(args).items():
    if value is not None:
        file_count += 1

if file_count == 0:
    print('Please add at least one file.')
    sys.exit()


df = load_csv(args.imdb_tv)

imdb_tv = standardise_columns(df, 'imdb_tv')
# print(imdb_tv)
print(imdb_tv[['type']])
