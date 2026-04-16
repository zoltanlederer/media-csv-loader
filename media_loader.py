"""
Media CSV Loader

Takes multiple CSV files from different sources (Plex exports, IMDB and TMDB list exports), loads them with pandas, standardises the column names and data types so they match, detects duplicates (titles that appear in both lists), merges everything into one clean master CSV, and saves it. This master file becomes the foundation for all remaining projects.
"""

import sys
import argparse
import pandas as pd

SOURCE_MAP = {
    'plex_movies': 'plex',
    'plex_tv': 'plex',
    'imdb_movies': 'imdb',
    'imdb_tv': 'imdb',
    'tmdb_movies': 'tmdb',
    'tmdb_tv': 'tmdb',
}

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
        # IMDB columns
        'Const': 'imdb_id',
        'Title': 'title',
        'Year': 'year',
        'Genres': 'genres',
        'IMDb Rating': 'imdb_rating',
        'Runtime (mins)': 'runtime_mins',
        'Title Type': 'type',
        'Release Date': 'release_date',
        'Original Title': 'original_title',
        'Directors': 'directors',

        # Plex columns
        'duration': 'runtime_mins',
        'summary': 'description',
        'originallyAvailableAt': 'originally_available_at',
        'seasonCount': 'number_of_seasons',
        'leafCount': 'number_of_episodes',

        # TMDB columns
        'id': 'tmdb_id',
        'name': 'title',
        'original_name': 'original_title',
        'overview': 'description',
        'first_air_date': 'release_date',
        'episode_run_time': 'runtime_mins',
        'runtime': 'runtime_mins',
        'vote_average': 'tmdb_rating',
    })
    
    df_renamed['source'] = SOURCE_MAP.get(source, source) # add source column e.g. plex, imdb, tmdb

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


def merge_dataframes(loaded_files):
    """ Merge a list of DataFrames into one combined DataFrame. """
    return pd.concat(loaded_files)



# main logic
parser = argparse.ArgumentParser(description='The program creates a master csv file from different sources.')
parser.add_argument('--plex-movies', help='add the path of the exported (movie) file from plex')
parser.add_argument('--plex-tv', help='add the path of the exported (tv show) file from plex')
parser.add_argument('--imdb-movies', help='add the path of the exported (movie) file from imdb')
parser.add_argument('--imdb-tv', help='add the path of the exported (tv show) file from imdb')
parser.add_argument('--tmdb-movies', help='add the path of the exported (movie) file from tmdb')
parser.add_argument('--tmdb-tv', help='add the path of the exported (tv show) file from tmdb')
args = parser.parse_args()


input_files = get_input_files(args)
loaded_files = load_all_files(input_files)
combined_df = merge_dataframes(loaded_files)

# print(combined_df)

# find a title that exists in both sources
pd.set_option('display.max_columns', None)
# print(combined_df[combined_df['imdb_id'] == 'tt1845307'])
duplicate = combined_df[combined_df['imdb_id'] == 'tt0848228']
for idx, row in duplicate.iterrows():
    print(f"\n--- Row {idx} (source: {row['source']}) ---")
    for col, val in row.items():
        print(f"{col}: {val}")

# for df in loaded_files:
#     print(df['type'])

# how many DataFrames
# print(len(loaded_files))

# # see each one briefly
# for df in loaded_files:
#     print(df.shape)  # rows and columns
#     print(df.head(2))
#     print()