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

SOURCE_PRIORITY = {'plex': 0, 'imdb': 1, 'tmdb': 2}

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


def handle_duplicates(combined_df):
    """ Deduplicate titles across sources, combine best available data into one row per title, update source labels, and drop unwanted columns. """

    # Sort by source priority so Plex rows come first, then IMDB, then TMDB
    sorted_df = combined_df.copy()  # copy() creates a new DataFrame — without this, changes would affect the original
    sorted_df['priority'] = sorted_df['source'].map(SOURCE_PRIORITY)  # map() replaces each source value with its priority number e.g. plex → 0
    sorted_df = sorted_df.sort_values('priority')  # sort rows so lower priority numbers (Plex) come first
    sorted_df = sorted_df.drop(columns=['priority'])  # remove the helper column — it was only needed for sorting

    # Collect imdb_ids per source to detect which titles appear in multiple sources
    plex_ids = set(sorted_df[sorted_df['source'] == 'plex']['imdb_id'])  # filter rows where source is plex, then collect their imdb_ids into a set
    imdb_ids = set(sorted_df[sorted_df['source'] == 'imdb']['imdb_id'])  # same for imdb
    tmdb_ids = set(sorted_df[sorted_df['source'] == 'tmdb']['imdb_id'])  # same for tmdb

    # Find overlapping titles between sources
    plex_and_imdb = plex_ids & imdb_ids  # & on sets returns only values that appear in both (intersection)
    plex_and_tmdb = plex_ids & tmdb_ids
    imdb_and_tmdb = imdb_ids & tmdb_ids
    plex_and_imdb_and_tmdb = plex_ids & imdb_ids & tmdb_ids  # titles that appear in all three sources

    # Split into titles with and without imdb_id — groupby drops NaN keys
    has_imdb_id = sorted_df[sorted_df['imdb_id'].notna()]  # notna() returns True for non-empty values — keep only rows that have an imdb_id
    no_imdb_id = sorted_df[sorted_df['imdb_id'].isna()]  # isna() returns True for empty values — keep only rows missing an imdb_id

    # Deduplicate by imdb_id, keeping first non-NaN value per column (Plex data takes priority)
    has_imdb_id = has_imdb_id.groupby('imdb_id').first()  # group rows by imdb_id and keep the first non-NaN value per column — deduplicates titles

    # Fill missing release_date from originally_available_at then drop unwanted columns
    columns_to_drop = ['originally_available_at', 'titleSort', 'Position', 'Created',
                       'Modified', 'Description', 'URL', 'Num Votes', 'Your Rating', 'Date Rated']
    has_imdb_id['release_date'] = has_imdb_id['release_date'].fillna(has_imdb_id['originally_available_at'])  # fillna() fills empty values with the value from another column
    has_imdb_id = has_imdb_id.drop(columns=columns_to_drop)  # drop() removes columns we don't need in the master CSV
    no_imdb_id = no_imdb_id.drop(columns=columns_to_drop)  # same cleanup for rows that had no imdb_id

    # Update source column to reflect which sources each title appears in
    has_imdb_id.loc[has_imdb_id.index.isin(plex_and_imdb), 'source'] = 'plex_imdb'  # loc[] updates specific rows — index.isin() checks if the imdb_id is in the set
    has_imdb_id.loc[has_imdb_id.index.isin(plex_and_tmdb), 'source'] = 'plex_tmdb'
    has_imdb_id.loc[has_imdb_id.index.isin(imdb_and_tmdb), 'source'] = 'imdb_tmdb'
    has_imdb_id.loc[has_imdb_id.index.isin(plex_and_imdb_and_tmdb), 'source'] = 'plex_imdb_tmdb'  # must be last — overwrites any two-source value set above
    has_imdb_id = has_imdb_id.reset_index()  # after groupby, imdb_id becomes the index — reset_index() moves it back to a regular column

    # Combine deduplicated titles with titles that had no imdb_id
    return pd.concat([has_imdb_id, no_imdb_id])  # pd.concat() stacks DataFrames vertically — combines both groups back into one


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
master_df = handle_duplicates(combined_df)

# print(f"Total rows: {len(master_df)}")
# print(master_df['imdb_id'].isna().sum(), "rows without imdb_id")
# print(master_df[master_df['title'] == '2 Broke Girls'][['title', 'source']])

# master_df.to_csv('test_output.csv', index=False)
