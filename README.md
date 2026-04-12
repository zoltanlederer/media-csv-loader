# Media CSV Loader

> 🚧 Work in progress

Takes two CSV files — the Plex export and an IMDB watchlist export — loads both with pandas, standardises the column names and data types so they match, detects duplicates (titles that appear in both lists), merges everything into one clean master CSV, and saves it. This master file becomes the foundation for all remaining projects.