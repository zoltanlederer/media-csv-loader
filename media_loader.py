"""
Media CSV Loader

Takes multiple CSV files from different sources (Plex exports, IMDB and TMDB list exports), loads them with pandas, standardises the column names and data types so they match, detects duplicates (titles that appear in both lists), merges everything into one clean master CSV, and saves it. This master file becomes the foundation for all remaining projects.
"""

import os
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
        if key == 'output': # exclude --output from the loop
            continue
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
    has_imdb_id = has_imdb_id.groupby('imdb_id').first()  # group by imdb_id and keep first non-NaN value per column — imdb_id becomes the index after this, no longer a regular column

    # Fill missing release_date from originally_available_at then drop unwanted columns
    columns_to_drop = ['originally_available_at', 'titleSort', 'Position', 'Created',
                       'Modified', 'Description', 'URL', 'Num Votes', 'Your Rating', 'Date Rated']
    has_imdb_id['release_date'] = has_imdb_id['release_date'].fillna(has_imdb_id['originally_available_at'])  # fillna() fills empty values with the value from another column
    has_imdb_id = has_imdb_id.drop(columns=columns_to_drop)  # drop() removes columns we don't need in the master CSV
    no_imdb_id = no_imdb_id.drop(columns=columns_to_drop)  # same cleanup for rows that had no imdb_id

    # Update source column to reflect which sources each title appears in
    has_imdb_id.loc[has_imdb_id.index.isin(plex_and_imdb), 'source'] = 'plex_imdb'  # loc[] updates specific rows — index.isin() checks if the imdb_id (now the index after groupby) is in the set
    has_imdb_id.loc[has_imdb_id.index.isin(plex_and_tmdb), 'source'] = 'plex_tmdb'
    has_imdb_id.loc[has_imdb_id.index.isin(imdb_and_tmdb), 'source'] = 'imdb_tmdb'
    has_imdb_id.loc[has_imdb_id.index.isin(plex_and_imdb_and_tmdb), 'source'] = 'plex_imdb_tmdb'  # must be last — overwrites any two-source value set above
    has_imdb_id = has_imdb_id.reset_index()  # after groupby, imdb_id becomes the index — reset_index() moves it back to a regular column

    # Combine deduplicated titles with titles that had no imdb_id
    return pd.concat([has_imdb_id, no_imdb_id])  # pd.concat() stacks DataFrames vertically — combines both groups back into one


def save_csv(master_df, output):
    """ Save master csv file to default or user selected path """
    try:
        master_df.to_csv(output, index=False)
    except OSError as error:
        print(f'Could not create file: {error}')
        sys.exit()
    except Exception as error:
        print(f'Something went wrong while writing the file: {error}')
        sys.exit()


def print_summary(master_df, output):
    """ Print a summary of the master CSV including total titles and breakdown by source. """
    total = len(master_df['title'])
    plex = len(master_df[master_df['source'] == 'plex'])
    imdb = len(master_df[master_df['source'] == 'imdb'])
    tmdb = len(master_df[master_df['source'] == 'tmdb'])
    plex_imdb = len(master_df[master_df['source'] == 'plex_imdb'])
    plex_tmdb = len(master_df[master_df['source'] == 'plex_tmdb'])
    imdb_tmdb = len(master_df[master_df['source'] == 'imdb_tmdb'])
    plex_imdb_tmdb = len(master_df[master_df['source'] == 'plex_imdb_tmdb'])


    print(f"\n💾 Master CSV saved to {output}")
    print(f"\n📊 {'Total titles:':<14} {total:>6}")


    if plex:
        print(f"    {'— plex only:':<14} {plex:>5}")
    if imdb:
        print(f"    {'— imdb only:':<14} {imdb:>5}")
    if tmdb:
        print(f"    {'— tmdb only:':<14} {tmdb:>5}")
    if plex_imdb:
        print(f"    {'— plex_imdb:':<14} {plex_imdb:>5}")
    if plex_tmdb:
        print(f"    {'— plex_tmdb:':<14} {plex_tmdb:>5}")
    if imdb_tmdb:
        print(f"    {'— imdb_tmdb:':<14} {imdb_tmdb:>5}")
    if plex_imdb_tmdb:
        print(f"    {'— plex_imdb_tmdb:':<14} {plex_imdb_tmdb:>5}")

    print('\n')


# main logic
parser = argparse.ArgumentParser(description='The program creates a master csv file from different sources.')
parser.add_argument('--plex-movies', help='add the path of the exported (movie) file from plex')
parser.add_argument('--plex-tv', help='add the path of the exported (tv show) file from plex')
parser.add_argument('--imdb-movies', help='add the path of the exported (movie) file from imdb')
parser.add_argument('--imdb-tv', help='add the path of the exported (tv show) file from imdb')
parser.add_argument('--tmdb-movies', help='add the path of the exported (movie) file from tmdb')
parser.add_argument('--tmdb-tv', help='add the path of the exported (tv show) file from tmdb')
parser.add_argument('--output', default='master.csv', help='path and filename to save the master CSV e.g. "master.csv" or "/Users/zoli/data/master.csv"')
args = parser.parse_args()

output = args.output

input_files = get_input_files(args)

print(f"\n🗂️ Loading {len(input_files)} files...")
for key, value in input_files:
    print(f"  — {os.path.basename(value)}")

print("\n✏️  Standardising columns...")
loaded_files = load_all_files(input_files)

print("🔄 Deduplicating...")
combined_df = merge_dataframes(loaded_files)
master_df = handle_duplicates(combined_df)

save_csv(master_df, output)
print_summary(master_df, output)