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

    if 'tv' in source.lower():
        df_renamed['type'] = 'tv_show'
    elif 'movie' in source.lower():
        df_renamed['type'] = 'movie'

    return df_renamed


def get_input_files(args):
    """ Collect provided file paths from arguments and return them as a list of tuples. Exits if no files were provided. """
    file_count = 0
    input_files = [] # e.g. [('imdb_tv', 'path/to/imdb_tv.csv'), ('plex_movies', 'path/to/plex_movies.csv')]
    for key, value in vars(args).items():
        if value is not None:
            file_count += 1
            input_files.append((key, value))

    if file_count == 0:
        print('Please add at least one file.')
        sys.exit()

    return input_files


def load_all_files(input_files):
    """ Load and standardise all provided CSV files. Returns a list of DataFrames. """
    all_dataframes = []
    for source, filepath in input_files:
        df = load_csv(filepath)
        standardised_df = standardise_columns(df, source)
        all_dataframes.append(standardised_df)
    return all_dataframes


# main logic
parser = argparse.ArgumentParser(description='The program creates a master csv file from different sources.')
parser.add_argument('--plex-movies', help='add the path of the exported (movie) file from plex')
parser.add_argument('--plex-tv', help='add the path of the exported (tv show) file from plex')
parser.add_argument('--imdb-movies', help='add the path of the exported (movie) file from imdb')
parser.add_argument('--imdb-tv', help='add the path of the exported (tv show) file from imdb')
parser.add_argument('--local-movies', help='add the path of the exported (movie) file from local scan')
parser.add_argument('--local-tv', help='add the path of the exported (tv show) file from local scan')
args = parser.parse_args()


input_files = get_input_files(args)
loaded_files = load_all_files(input_files)
