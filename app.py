import os
import re
import json
import sqlite3
from flask import Flask, render_template, request, send_file, redirect, url_for, session
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from werkzeug.utils import secure_filename
from rapidfuzz import process, fuzz
import tempfile
import openpyxl
from openpyxl import Workbook
import logging
from rapidfuzz import process, fuzz
from collections import defaultdict
from typing import List, Set, Dict, Tuple, Iterator
import tempfile

# Set up pymedtermino before importing SNOMEDCT
import pymedtermino

# Set the DATA_DIR for pymedtermino
pymedtermino.DATA_DIR = "/home/opc"

# Corrected: Remove the .sqlite3 extension and store the connection
db = pymedtermino.connect_sqlite3("/home/opc/snomedct")

# Now import SNOMEDCT after setting DATA_DIR and connecting
from pymedtermino.snomedct import SNOMEDCT

app = Flask(__name__)
app.secret_key = 'alsdkfja2342342adflkadjf' 

# Configure upload and processed folders
UPLOAD_FOLDER = 'uploads'
PROCESSED_FOLDER = 'processed_files'
ALLOWED_EXTENSIONS = {'xlsx'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['PROCESSED_FOLDER'] = PROCESSED_FOLDER

# Ensure the upload and processed directories exist
os.makedirs('uploads', exist_ok=True)
os.makedirs('processed_files', exist_ok=True)

# Helper function to check file extension
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Home route for the web hub
@app.route('/')
def home():
    return render_template('home.html')

CUSTOM_DICT_FILE = "custom_dictionary.json"

# Load SNOMED words once at startup
print("Loading SNOMED CT terms...")
snomed_words = set()

def get_snomed_words():
    # Use the existing database connection
    cursor = db.cursor()
    
    # Query the active descriptions from the database
    cursor.execute("SELECT term FROM Descriptions WHERE active=1")
    snomed_terms = cursor.fetchall()
    cursor.close()
    
    # Extract words and convert them to a set
    words = set()
    for term in snomed_terms:
        if term[0]:
            words.update(re.findall(r'\b\w+\b', term[0].lower()))
    return words

snomed_words = get_snomed_words()
print(f"Loaded {len(snomed_words)} unique words from SNOMED CT")

# Utility functions (from your script, adjusted for web context)
def load_custom_dictionary():
    try:
        with open(CUSTOM_DICT_FILE, 'r') as f:
            return set(json.load(f))
    except FileNotFoundError:
        return set()

def save_custom_dictionary(custom_dict):
    with open(CUSTOM_DICT_FILE, 'w') as f:
        json.dump(list(custom_dict), f)

def match_case(word, corrected_word):
    if word.isupper():
        return corrected_word.upper()
    elif word.islower():
        return corrected_word.lower()
    elif word.istitle():
        return corrected_word.capitalize()
    else:
        return corrected_word

def process_alias_word(word, custom_words):
    # Replace '*' and '-' with '_'
    word = word.replace('*', '_')
    word = word.replace('-', '_')
    
    # Check if the word is in the custom dictionary (case-insensitive)
    if word in custom_words or word.lower() in custom_words:
        return word.upper()
    # Handle camel case words for alias, and convert to upper case
    elif re.search(r'[a-z][A-Z]', word):
        return re.sub(r'([a-z])([A-Z])', r'\1_\2', word).upper()
    else:
        return word.upper()

def convert_name(name, append_text, use_spellcheck=True, custom_dict=set(), suppress_final_output=False):
    # Initialize words
    custom_words = set(['IgM', 'IgG', 'IgD', 'IgE', 'IgA', 'CSF', 'rRNA', 'DNA', 'mRNA'])
    custom_words.update(custom_dict)
    
    # Create a combined set of words for spell-checking
    combined_words = snomed_words.union(word.lower() for word in custom_words)

    corrected_words = []
    # Split the name into words and punctuation
    words = re.findall(r'\w+|\s+|[^\w\s]+', name)

    for original_word in words:
        if not original_word.strip() or not use_spellcheck or not original_word.isalpha():
            corrected_words.append(original_word)
        elif original_word.lower() in combined_words or original_word in custom_words:
            corrected_words.append(original_word)
        else:
            # Spellcheck logic
            matches = process.extract(original_word.lower(), combined_words, limit=1, scorer=fuzz.ratio)
            if matches and matches[0][1] >= 80:
                suggested_word = matches[0][0]
                if original_word.lower() != suggested_word and not re.search(r'\d', original_word):
                    # Here, instead of input(), we need to handle this interaction in the web interface
                    # We'll store the necessary information in session and prompt the user
                    session['spellcheck_needed'] = True
                    session['original_word'] = original_word
                    session['suggested_word'] = suggested_word
                    session['pending_name'] = name
                    session['corrected_words'] = corrected_words
                    session['words_remaining'] = words[words.index(original_word)+1:]
                    session['append_text'] = append_text
                    session['use_spellcheck'] = use_spellcheck
                    session['custom_dict'] = list(custom_dict)
                    return None, None  # Indicate that spellcheck interaction is needed
                else:
                    corrected_words.append(original_word)
            else:
                corrected_words.append(original_word)

    # Join words for the name, preserving original spacing and punctuation
    processed_name = ''.join(corrected_words)

    # Now build alias_name from processed_name
    alias_name = processed_name

    # Remove text within parentheses for alias processing
    alias_name = re.sub(r'\([^)]*\)', '', alias_name)
    alias_name = re.sub(r"['+/,<>:\.\-]", ' ', alias_name)
    alias_name = alias_name.replace('%', ' PCT')

    # Split alias_name into words
    alias_words = re.findall(r'\b[\w*-]+\b', alias_name)

    # Process alias words
    alias_corrected_words = []
    for alias_word in alias_words:
        alias_corrected_words.append(process_alias_word(alias_word, custom_words))

    # Join alias words to form the alias
    processed_alias = '_'.join(filter(bool, alias_corrected_words))

    # Append text to the end of the alias
    if append_text.upper() not in processed_alias:
        processed_alias = f"{processed_alias.rstrip('_')}_{append_text.upper()}"

    return processed_name, processed_alias

#stop undoing here
@app.route('/alias-converter', methods=['GET', 'POST'])
def alias_converter():
    if 'history' not in session:
        session['history'] = []
        session['state'] = 'start'

    history = session['history']
    state = session['state']

    if request.method == 'POST':
        user_input = request.form['user_input'].strip()
        if user_input.lower() == 'reset':
            session.clear()
            history = []
            history.append("Do you want to use spell check? (y/n):")
            session['state'] = 'start'
            session['history'] = history
            return render_template('terminal.html', history=history)

        history.append(f">>> {user_input}")

        # Handle the different states
        if state == 'start':
            session['use_spellcheck'] = user_input.lower() == 'y'
            session['state'] = 'append_text'
            history.append("Enter the text you want to append (e.g., OBSTYPE):")
        elif state == 'append_text':
            session['append_text'] = user_input
            session['state'] = 'add_mode'
            history.append("Do you want to perform a single add or multi add? (s/m) (or type 'exit' to quit):")
        elif state == 'add_mode':
            if user_input.lower() == 's':
                session['state'] = 'single_add'
                history.append("Enter the name you want to convert (or type 'back' to return to the main menu):")
            elif user_input.lower() == 'm':
                session['state'] = 'multi_add'
                history.append("Enter the names you want to convert, one per line (type 'done' on a new line to finish or 'back' to return to the main menu):")
            elif user_input.lower() == 'exit':
                # Clear the session and reset to start
                session.clear()
                history = []
                history.append("Do you want to use spell check? (y/n):")
                session['state'] = 'start'
                session['history'] = history
            else:
                history.append("Invalid selection. Please choose 's' for single add, 'm' for multi add, or 'exit' to quit.")
        elif state == 'single_add':
            if user_input.lower() == 'back':
                session['state'] = 'add_mode'
                history.append("Do you want to perform a single add or multi add? (s/m) (or type 'exit' to quit):")
            else:
                # Process the single name
                custom_dict = load_custom_dictionary()
                result = convert_name(
                    user_input,
                    session['append_text'],
                    session['use_spellcheck'],
                    custom_dict=custom_dict
                )
                if result == (None, None):
                    # Spellcheck interaction needed
                    history.append(f"Potential misspelling detected: '{session['original_word']}'")
                    history.append(f"Would you like to update it to '{session['suggested_word']}'? (y/n):")
                    session['state'] = 'spellcheck_prompt_single'
                else:
                    processed_name, processed_alias = result
                    history.append("Converted Names:")
                    history.append(processed_name)
                    history.append("Converted Aliases:")
                    history.append(processed_alias)
                    # Remain in single_add state
                    history.append("Enter the name you want to convert (or type 'back' to return to the main menu):")
                    session['state'] = 'single_add'
        elif state == 'multi_add':
            if user_input.lower() == 'back':
                session['state'] = 'add_mode'
                history.append("Do you want to perform a single add or multi add? (s/m) (or type 'exit' to quit):")
            elif user_input.lower() == 'done':
                # Move to processing state
                session['state'] = 'processing_multi_add'
                return alias_converter()
            else:
                # Split the input into lines
                names = user_input.strip().splitlines()
                names = [name.strip() for name in names if name.strip()]
                if not names:
                    history.append("No names entered. Please enter at least one name or type 'back' to return.")
                else:
                    # Initialize variables
                    session['remaining_names'] = names
                    session['converted_names'] = []
                    session['converted_aliases'] = []
                    session['state'] = 'processing_multi_add'
                    # Start processing
                    return alias_converter()
        elif state == 'processing_multi_add':
            # Process names in a loop
            remaining_names = session.get('remaining_names', [])
            converted_names = session.get('converted_names', [])
            converted_aliases = session.get('converted_aliases', [])
            custom_dict = load_custom_dictionary()
            spellcheck_needed = False

            while remaining_names and not spellcheck_needed:
                name = remaining_names.pop(0)
                session['remaining_names'] = remaining_names

                # Initialize variables for the new name
                session['corrected_words'] = []
                session['words_remaining'] = []
                session['original_word'] = ''
                session['suggested_word'] = ''
                session['current_name'] = name

                result = convert_name(
                    name,
                    session['append_text'],
                    session['use_spellcheck'],
                    custom_dict=custom_dict,
                    suppress_final_output=True
                )

                if result == (None, None):
                    # Spellcheck interaction needed
                    history.append(f"Potential misspelling detected: '{session['original_word']}' in '{name}'")
                    history.append(f"Would you like to update it to '{session['suggested_word']}'? (y/n):")
                    session['state'] = 'spellcheck_prompt_multi'
                    spellcheck_needed = True
                    break  # Exit the loop to wait for user input
                else:
                    processed_name, processed_alias = result
                    converted_names.append(processed_name)
                    converted_aliases.append(processed_alias)
                    session['converted_names'] = converted_names
                    session['converted_aliases'] = converted_aliases

            if not spellcheck_needed and not remaining_names:
                # All names processed, display results
                history.append("Converted Names:")
                history.append("\n".join(converted_names))
                history.append("Converted Aliases:")
                history.append("\n".join(converted_aliases))
                # Loop back to add_mode
                history.append("Do you want to perform a single add or multi add? (s/m) (or type 'exit' to quit):")
                session['state'] = 'add_mode'

            return render_template('terminal.html', history=history)
        elif state == 'spellcheck_prompt_single':
            # Handle user response to spellcheck for single add
            if user_input.lower() == 'y':
                # User accepted the suggestion
                corrected_word = match_case(session['original_word'], session['suggested_word'])
                session['corrected_words'].append(corrected_word)
            else:
                # User declined the suggestion, use the original word
                session['corrected_words'].append(session['original_word'])

            # Continue processing the remaining words
            name = session['pending_name']
            words_remaining = session.get('words_remaining', [])
            corrected_words = session.get('corrected_words', [])
            custom_dict = set(session.get('custom_dict', []))

            while words_remaining:
                original_word = words_remaining.pop(0)
                session['words_remaining'] = words_remaining
                if not original_word.strip() or not session['use_spellcheck'] or not original_word.isalpha():
                    corrected_words.append(original_word)
                elif original_word.lower() in snomed_words or original_word in custom_dict:
                    corrected_words.append(original_word)
                else:
                    # Check for potential misspelling
                    combined_words = snomed_words.union(word.lower() for word in custom_dict)
                    matches = process.extract(original_word.lower(), combined_words, limit=1, scorer=fuzz.ratio)
                    if matches and matches[0][1] >= 80:
                        suggested_word = matches[0][0]
                        if original_word.lower() != suggested_word and not re.search(r'\d', original_word):
                            # Need to prompt user for this word
                            session['original_word'] = original_word
                            session['suggested_word'] = suggested_word
                            # Update the state to prompt for this word
                            history.append(f"Potential misspelling detected: '{session['original_word']}'")
                            history.append(f"Would you like to update it to '{session['suggested_word']}'? (y/n):")
                            return render_template('terminal.html', history=history)
                        else:
                            corrected_words.append(original_word)
                    else:
                        corrected_words.append(original_word)

            # Generate the corrected name and alias after processing all words
            processed_name = ''.join(corrected_words)
            alias_name = re.sub(r'\([^)]*\)', '', processed_name)
            alias_name = re.sub(r"['+/,<>:\.\-]", ' ', alias_name)
            alias_name = alias_name.replace('%', ' PCT')
            alias_words = re.findall(r'\b[\w*-]+\b', alias_name)
            alias_corrected_words = [process_alias_word(word, custom_dict) for word in alias_words]
            processed_alias = '_'.join(filter(bool, alias_corrected_words))
            if session['append_text'].upper() not in processed_alias:
                processed_alias = f"{processed_alias.rstrip('_')}_{session['append_text'].upper()}"

            # Display the final corrected name and alias
            history.append("Converted Names:")
            history.append(processed_name)
            history.append("Converted Aliases:")
            history.append(processed_alias)

            # Remain in single_add state
            history.append("Enter the name you want to convert (or type 'back' to return to the main menu):")
            session['state'] = 'single_add'

        elif state == 'spellcheck_prompt_multi':
            if user_input.lower() == 'y':
                corrected_word = match_case(session['original_word'], session['suggested_word'])
            else:
                corrected_word = session['original_word']

            # Append corrected word to corrected_words
            corrected_words = session.get('corrected_words', [])
            corrected_words.append(corrected_word)
            session['corrected_words'] = corrected_words

            words_remaining = session.get('words_remaining', [])
            custom_dict = set(session.get('custom_dict', []))

            while words_remaining:
                original_word = words_remaining.pop(0)
                session['words_remaining'] = words_remaining
                if not original_word.strip() or not session['use_spellcheck'] or not original_word.isalpha():
                    corrected_words.append(original_word)
                elif original_word.lower() in snomed_words or original_word in custom_dict:
                    corrected_words.append(original_word)
                else:
                    # Spellcheck logic
                    combined_words = snomed_words.union(word.lower() for word in custom_dict)
                    matches = process.extract(original_word.lower(), combined_words, limit=1, scorer=fuzz.ratio)
                    if matches and matches[0][1] >= 80:
                        suggested_word = matches[0][0]
                        if original_word.lower() != suggested_word and not re.search(r'\d', original_word):
                            # Need to prompt user
                            session['original_word'] = original_word
                            session['suggested_word'] = suggested_word
                            # Prompt the user again
                            history.append(f"Potential misspelling detected: '{session['original_word']}' in '{session['current_name']}'")
                            history.append(f"Would you like to update it to '{session['suggested_word']}'? (y/n):")
                            return render_template('terminal.html', history=history)
                        else:
                            corrected_words.append(original_word)
                    else:
                        corrected_words.append(original_word)

            # Generate the corrected name and alias after processing all words
            processed_name = ''.join(corrected_words)
            alias_name = re.sub(r'\([^)]*\)', '', processed_name)
            alias_name = re.sub(r"['+/,<>:\.\-]", ' ', alias_name)
            alias_name = alias_name.replace('%', ' PCT')
            alias_words = re.findall(r'\b[\w*-]+\b', alias_name)
            alias_corrected_words = [process_alias_word(word, custom_dict) for word in alias_words]
            processed_alias = '_'.join(filter(bool, alias_corrected_words))
            if session['append_text'].upper() not in processed_alias:
                processed_alias = f"{processed_alias.rstrip('_')}_{session['append_text'].upper()}"

            # Append to converted names
            converted_names = session.get('converted_names', [])
            converted_aliases = session.get('converted_aliases', [])
            converted_names.append(processed_name)
            converted_aliases.append(processed_alias)
            session['converted_names'] = converted_names
            session['converted_aliases'] = converted_aliases

            # Reset corrected words and continue processing
            session['corrected_words'] = []
            session['state'] = 'processing_multi_add'
            # Call the function again to process the next name
            return alias_converter()
        else:
            session.clear()
            history = []
            history.append("Do you want to use spell check? (y/n):")
            session['state'] = 'start'
            session['history'] = history

        # Update the session
        session['history'] = history
    else:
        # Handle GET requests without resetting the application
        history = session.get('history', [])
        return render_template('terminal.html', history=history)

    return render_template('terminal.html', history=history)

# Code Compare Dropped and Duplicates Report route
@app.route('/compare-dropped-duplicates', methods=['GET', 'POST'])
def compare_dropped_duplicates():
    if request.method == 'POST':
        uploaded_file = request.files['file']
        if uploaded_file and allowed_file(uploaded_file.filename):
            filename = secure_filename(uploaded_file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            uploaded_file.save(file_path)

            # Call the function to process the file (rename columns + dropped/duplicates)
            processed_file_path = process_dropped_duplicates(file_path)

            # Return the processed file for download
            return send_file(processed_file_path, as_attachment=True)

    return render_template('compare_dropped_duplicates.html')

# Rename columns and run dropped/duplicates logic
def process_dropped_duplicates(file_path):
    # Step 1: Load the Excel file and rename columns
    df = pd.read_excel(file_path)
    df.rename(columns={df.columns[3]: 'Old Concept Name', df.columns[6]: 'New Concept Name'}, inplace=True)

    # Output columns
    columns_to_output = ['Code System', 'Code', 'Names', 'Old Concept Name', 'New Concept Name']

    # Function Definitions (from your original script)
    # Function - All Duplicates (Red, White, and Green)
    def process_all_dups(df):
        df['Normalized_Code'] = df['Code'].astype(str).str.rstrip('.0')
        grouped = df.groupby(['Code System', 'Normalized_Code'])
        matching_codes = []
        for (code_system, code), group in grouped:
            removed_from_concept = group['Old Concept Name'].notnull() & group['New Concept Name'].isnull()
            added_to_concept = group['Old Concept Name'].isnull() & group['New Concept Name'].notnull()
            remains_in_concept = group['Old Concept Name'].notnull() & group['New Concept Name'].notnull()
            if removed_from_concept.any() and added_to_concept.any() and remains_in_concept.any():
                matching_codes.append((code_system, code))
        matching_codes_df = df[(df[['Code System', 'Normalized_Code']].apply(tuple, axis=1).isin(matching_codes))]
        return matching_codes_df[columns_to_output]

    # Function - Green/White Duplicates
    def process_gw_dups(df):
        removed_codes = df[df['Old Concept Name'].notnull() & df['New Concept Name'].isnull()]['Code'].unique()
        initial_matches = df[df['Old Concept Name'].notnull() & df['New Concept Name'].notnull()]
        duplicates = []
        for _, group in initial_matches.groupby(['Code System', 'Code']):
            initial_match_rows = group
            subsequent_rows = df[(df['Code System'] == group['Code System'].iloc[0]) &
                                (df['Code'] == group['Code'].iloc[0]) &
                                (df.index > initial_match_rows.index.max())]
            if (subsequent_rows['Old Concept Name'].isnull() & subsequent_rows['New Concept Name'].notnull()).any():
                duplicates.append((group['Code System'].iloc[0], group['Code'].iloc[0]))
        duplicates = [dup for dup in duplicates if dup[1] not in removed_codes]
        duplicate_codes_df = df[(df[['Code System', 'Code']].apply(tuple, axis=1).isin(duplicates))]
        return duplicate_codes_df[duplicate_codes_df['New Concept Name'].notnull()][columns_to_output]

    # Function - Red/White Duplicates
    def process_rw_dups(df):
        df['Code'] = df['Code'].astype(str)
        added_codes = df[df['Old Concept Name'].isnull() & df['New Concept Name'].notnull()]['Code'].unique()
        df = df[~df['Code'].isin(added_codes)]
        initial_matches = df[df['Old Concept Name'].notnull() & df['New Concept Name'].notnull()]
        matching_codes = []
        for _, group in initial_matches.groupby(['Code System', 'Code']):
            subsequent_rows = df[(df['Code System'] == group['Code System'].iloc[0]) & (df['Code'] == group['Code'].iloc[0])]
            if (subsequent_rows['Old Concept Name'].notnull() & subsequent_rows['New Concept Name'].isnull()).any():
                matching_codes.append((group['Code System'].iloc[0], group['Code'].iloc[0]))
        matching_codes_df = df[df[['Code System', 'Code']].apply(tuple, axis=1).isin(matching_codes)]
        return matching_codes_df[columns_to_output]
    
    # Function - Dropped
    def process_dropped_codes(df):
        removed_not_added = []
        for code in df['Code'].unique():
            code_df = df[df['Code'] == code]
            if code_df['Old Concept Name'].notnull().any() and not code_df['New Concept Name'].notnull().any():
                removed_not_added.append(code)
        removed_not_added_df = df[df['Code'].isin(removed_not_added)]
        columns_to_output_dropped = ['Code System', 'Code', 'Names', 'Old Concept Name']
        return removed_not_added_df[columns_to_output_dropped]

    # Function - Code Additions
    def process_code_additions(df):
        added_codes_df = df[df['Old Concept Name'].isnull() & df['New Concept Name'].notnull()]
        columns_to_output_additions = ['Code System', 'Code', 'Names', 'New Concept Name']
        return added_codes_df[columns_to_output_additions]

    # Run the processing functions
    all_dups_df = process_all_dups(df)
    gw_dups_df = process_gw_dups(df)
    rw_dups_df = process_rw_dups(df)
    dropped_codes_df = process_dropped_codes(df)
    code_additions_df = process_code_additions(df)

    # Load the workbook (use openpyxl)
    wb = load_workbook(file_path)

    # Create Dropped sheet
    if "Dropped" in wb.sheetnames:
        wb.remove(wb["Dropped"])
    ws_dropped = wb.create_sheet("Dropped", 1)

    # Dropped codes data and styles
    for col, column_name in enumerate(dropped_codes_df.columns, start=1):
        ws_dropped.cell(row=1, column=col, value=column_name)
        ws_dropped.cell(row=1, column=col).font = Font(bold=True)
    for row_idx, data_row in enumerate(dropped_codes_df.itertuples(index=False), start=2):
        for col_idx, value in enumerate(data_row, start=1):
            ws_dropped.cell(row=row_idx, column=col_idx, value=value)
    red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    red_font = Font(color='9C0006')
    for row in ws_dropped.iter_rows(min_row=2, max_row=ws_dropped.max_row, min_col=1, max_col=ws_dropped.max_column):
        old_concept = row[3].value
        if old_concept:
            for cell in row:
                cell.fill = red_fill
                cell.font = red_font

    # Create Duplicates sheet
    if "Duplicates" in wb.sheetnames:
        wb.remove(wb["Duplicates"])
    ws = wb.create_sheet("Duplicates")

    # Write data for Duplicates sheet with headers
    def write_data_with_header(ws, start_row, header, data):
        ws.cell(row=start_row, column=1, value=header)
        ws.cell(row=start_row, column=1).font = Font(bold=True)
        ws.cell(row=start_row, column=1).alignment = Alignment(horizontal='center')
        ws.merge_cells(start_row=start_row, start_column=1, end_row=start_row, end_column=len(columns_to_output))
        
        for col, column_name in enumerate(columns_to_output, start=1):
            ws.cell(row=start_row + 1, column=col, value=column_name)
            ws.cell(row=start_row + 1, column=col).font = Font(bold=True)
        
        for row_idx, data_row in enumerate(data.itertuples(index=False), start=start_row + 2):
            for col_idx, value in enumerate(data_row, start=1):
                ws.cell(row=row_idx, column=col_idx, value=value)
        
        return start_row + data.shape[0] + 3 

    # Duplicates sheet data and styles
    next_row = write_data_with_header(ws, 1, "Green, White, and Red Duplicates", all_dups_df)
    next_row = write_data_with_header(ws, next_row, "Green and White Duplicates", gw_dups_df)
    next_row = write_data_with_header(ws, next_row, "Red and White Duplicates", rw_dups_df)

    green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    green_font = Font(color='006100')

    # Dictionary to store the rows for each unique code
    code_system_rows = {}

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        code_system = row[0].value  # Column A
        code = row[1].value  # Column B
        old_concept = row[3].value
        new_concept = row[4].value
        
        if isinstance(old_concept, str) and not isinstance(new_concept, str):
            for cell in row:
                cell.fill = red_fill
                cell.font = red_font
        elif not isinstance(old_concept, str) and isinstance(new_concept, str):
            for cell in row:
                cell.fill = green_fill
                cell.font = green_font
        
        # Store the row number for each unique code and code system combination
        key = (code_system, code)
        if key not in code_system_rows:
            code_system_rows[key] = []
        code_system_rows[key].append(row[0].row)

    # Add borders to the outer edges of each set of codes
    medium_side = Side(style='medium')

    for (code_system, code), rows in code_system_rows.items():
        if len(rows) > 1:  # Only add borders if there's more than one row with the same code and code system
            min_row = min(rows)
            max_row = max(rows)
            
            # Apply top border
            for cell in ws[min_row]:
                cell.border = cell.border + Border(top=medium_side)
            
            # Apply bottom border
            for cell in ws[max_row]:
                cell.border = cell.border + Border(bottom=medium_side)
            
            # Apply left and right borders
            for row_idx in range(min_row, max_row + 1):
                ws.cell(row=row_idx, column=1).border = ws.cell(row=row_idx, column=1).border + Border(left=medium_side)
                ws.cell(row=row_idx, column=ws.max_column).border = ws.cell(row=row_idx, column=ws.max_column).border + Border(right=medium_side)

    # Create Code Additions sheet and styles
    if "Additions" in wb.sheetnames:
        wb.remove(wb["Additions"])
    ws_added = wb.create_sheet("Additions", 2)
    for col, column_name in enumerate(code_additions_df.columns, start=1):
        ws_added.cell(row=1, column=col, value=column_name)
        ws_added.cell(row=1, column=col).font = Font(bold=True)
    for row_idx, data_row in enumerate(code_additions_df.itertuples(index=False), start=2):
        for col_idx, value in enumerate(data_row, start=1):
            ws_added.cell(row=row_idx, column=col_idx, value=value)

    for row in ws_added.iter_rows(min_row=2, max_row=ws_added.max_row, min_col=1, max_col=ws_added.max_column):
        for cell in row:
            cell.fill = green_fill
            cell.font = green_font

    # Save the workbook to the processed files folder
    output_file_name = 'processed_' + os.path.basename(file_path)
    output_file_path = os.path.join(app.config['PROCESSED_FOLDER'], output_file_name)
    wb.save(output_file_path)

    return output_file_path

# Code Compare Grouper Check route
@app.route('/compare-grouper-check', methods=['GET', 'POST'])
def compare_grouper_check():
    if request.method == 'POST':
        # Handle file upload
        uploaded_file = request.files.get('file')
        if uploaded_file and allowed_file(uploaded_file.filename):
            filename = secure_filename(uploaded_file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            uploaded_file.save(file_path)

            # Process the uploaded file
            try:
                processed_file_path = process_grouper_check(file_path)

                # Return the processed file for download
                return send_file(processed_file_path, as_attachment=True)
            except Exception as e:
                # Log the error and return an error message
                print(f"Error processing file: {e}")
                error_message = f"An error occurred during processing: {e}"
                return render_template('compare_grouper_check.html', error=error_message)
        else:
            # File not allowed or not provided
            error_message = "Please upload a valid .xlsx file."
            return render_template('compare_grouper_check.html', error=error_message)

    elif request.method == 'GET' and 'download_template' in request.args:
        # User requested to download the template
        template_file_path = generate_template()
        return send_file(template_file_path, as_attachment=True)

    return render_template('compare_grouper_check.html')

def generate_template():
    # Create a new workbook
    wb = Workbook()

    # Create the Instructions sheet
    ws_instructions = wb.active
    ws_instructions.title = "Instructions"

    # List of instruction lines
    instructions_lines = [
        "Concepts Tab - add concept names from the more recent release sorted alphabetically",
        "Relationships tab - add concept names from Concept 2 column sorted alphabetically",
        "Note - please do not apply any formatting or links"
    ]

    # Write each instruction line into separate rows
    for idx, line in enumerate(instructions_lines, start=1):
        ws_instructions.cell(row=idx, column=1, value=line)

    # Create the Concepts Tab
    ws_concepts = wb.create_sheet("Concepts Tab")
    ws_concepts.cell(row=1, column=1, value="Concept Name")

    # Create the Relationships Tab
    ws_relationships = wb.create_sheet("Relationships Tab")
    ws_relationships.cell(row=1, column=1, value="Concept Name")

    # Save the workbook to a temporary file
    template_file_path = os.path.join(app.config['PROCESSED_FOLDER'], 'Groupers_Template.xlsx')
    wb.save(template_file_path)

    return template_file_path

# Placeholder for Grouper Check processing
def process_grouper_check(file_path):
    # Load the workbook
    wb = openpyxl.load_workbook(file_path)

    # Get the sheets
    try:
        concepts_sheet = wb["Concepts Tab"]
        relationships_sheet = wb["Relationships Tab"]
    except KeyError as e:
        raise Exception(f"Missing sheet in the uploaded file: {e}")

    # Extract values from both sheets (skip the header)
    concepts_values = set()
    for row in concepts_sheet.iter_rows(min_row=2, values_only=True):
        for cell in row:
            if cell:
                concepts_values.add(cell)

    relationships_values = set()
    for row in relationships_sheet.iter_rows(min_row=2, values_only=True):
        for cell in row:
            if cell:
                relationships_values.add(cell)

    # Find unique and common values
    unique_to_concepts = concepts_values - relationships_values
    unique_to_relationships = relationships_values - concepts_values
    common_values = concepts_values.intersection(relationships_values)

    # Create a new sheet for results
    if "Check Groupers" in wb.sheetnames:
        wb.remove(wb["Check Groupers"])
    check_groupers = wb.create_sheet("Check Groupers")

    # Write headers and unique values
    headers = ["Unique to Concepts Tab", "Unique to Relationships Tab", "No Review Required"]
    row = 1
    for header in headers:
        check_groupers.cell(row=row, column=1, value=header).font = Font(bold=True)
        row += 1

        if header == "Unique to Concepts Tab":
            for value in sorted(unique_to_concepts):
                check_groupers.cell(row=row, column=1, value=value)
                row += 1
        elif header == "Unique to Relationships Tab":
            for value in sorted(unique_to_relationships):
                check_groupers.cell(row=row, column=1, value=value)
                row += 1
        else:  # No Review Required
            check_groupers.cell(row=row, column=1, value="Concepts Tab")
            check_groupers.cell(row=row, column=2, value="Relationships Tab")
            row += 1
            for value in sorted(common_values):
                check_groupers.cell(row=row, column=1, value=value)
                check_groupers.cell(row=row, column=2, value=value)
                row += 1

        row += 1  # Add a blank row after each section

    # Auto-adjust column widths
    for column_cells in check_groupers.columns:
        length = max(len(str(cell.value or "")) for cell in column_cells)
        check_groupers.column_dimensions[column_cells[0].column_letter].width = length + 2

    # Save the workbook to the processed files folder
    output_file_name = 'processed_grouper_' + os.path.basename(file_path)
    output_file_path = os.path.join(app.config['PROCESSED_FOLDER'], output_file_name)
    wb.save(output_file_path)

    return output_file_path

def analyze_excel(file_path):
    # Load the workbook
    wb = openpyxl.load_workbook(file_path)
    
    # Get the sheets
    sheet1 = wb['Sheet1']
    sheet2 = wb['Sheet2']
    # Remove 'Sheet3' if it already exists to avoid duplicate sheets
    if 'Sheet3' in wb.sheetnames:
        wb.remove(wb['Sheet3'])
    sheet3 = wb.create_sheet('Sheet3')
    
    # Function to clean concept names
    def clean_concept(concept):
        if not concept:
            return ''
        # Remove text within parentheses
        concept_clean = re.sub(r'\(.*?\)', '', concept)
        # Strip and lowercase
        concept_clean = concept_clean.strip().lower()
        return concept_clean
    
    # Extract and clean concepts from Sheet1
    concepts = set()
    for row in sheet1.iter_rows(min_row=2, values_only=True):
        if row[0]:
            concept = clean_concept(row[0])
            if concept:  # Only add non-empty concepts
                concepts.add(concept)
    
    # Write header to Sheet3
    sheet3.append(['Code System Name', 'Code', 'Names', 'Concept Names'])
    
    # Filter and write data to Sheet3
    for row in sheet2.iter_rows(min_row=2, values_only=True):
        if len(row) < 4 or not all(row[:4]):
            # Skip rows that don't have all required columns
            continue
        
        code_system, code, names, concept_names = row
        
        # Split concept names on newlines only, and remove empty strings
        row_concepts = [concept.strip() for concept in re.split(r'[\n]+', concept_names) if concept.strip()]
        # Clean concepts
        row_concepts_clean = [clean_concept(concept) for concept in row_concepts]
        
        match_found = False
        for concept_clean in row_concepts_clean:
            if concept_clean and concept_clean in concepts:
                match_found = True
                break
        
        if match_found:
            sheet3.append([code_system, code, names, concept_names])
    
    # Save the workbook to the processed files folder
    output_file_name = 'processed_' + os.path.basename(file_path)
    output_file_path = os.path.join(app.config['PROCESSED_FOLDER'], output_file_name)
    wb.save(output_file_path)
    
    return output_file_path

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'xlsx'}

@app.route('/non-exclusive-personalized', methods=['GET', 'POST'])
def non_exclusive_personalized():
    if request.method == 'POST':
        uploaded_file = request.files.get('file')
        if uploaded_file and allowed_file(uploaded_file.filename):
            filename = secure_filename(uploaded_file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            uploaded_file.save(file_path)
            
            try:
                processed_file_path = analyze_excel(file_path)
                return send_file(processed_file_path, as_attachment=True)
            except Exception as e:
                error_message = f"An error occurred during processing: {e}"
                return render_template('nonexclusive_personalized.html', error=error_message)
        else:
            error_message = "Please upload a valid .xlsx file."
            return render_template('nonexclusive_personalized.html', error=error_message)
    return render_template('nonexclusive_personalized.html') 

def get_snomed_words():
    # Use the existing database connection
    cursor = db.cursor()
    
    # Query the active descriptions from the database
    cursor.execute("SELECT term FROM Descriptions WHERE active=1")
    snomed_terms = cursor.fetchall()
    cursor.close()
    
    # Extract words and convert them to a set
    words = set()
    for term in snomed_terms:
        if term[0]:
            words.update(re.findall(r'\b\w+\b', term[0].lower()))
    return words

snomed_words = get_snomed_words()
print(f"Loaded {len(snomed_words)} unique words from SNOMED CT")

@app.route('/snomed_spell_checker', methods=['GET', 'POST'])
def snomed_spell_checker():
    if request.method == 'POST':
        # Handle file upload
        if 'file' not in request.files:
            return "No file uploaded", 400
        file = request.files['file']
        if file.filename == '':
            return "No file selected", 400
        if file:
            # Save the uploaded file to a temporary location
            filename = secure_filename(file.filename)
            temp_dir = tempfile.gettempdir()
            input_file_path = os.path.join(temp_dir, filename)
            file.save(input_file_path)
            
            # Run the script on the uploaded file
            try:
                output_file_path = run_spell_check_script(input_file_path)
                # After processing, send the output file to the user
                return send_file(output_file_path, as_attachment=True)
            except Exception as e:
                return f"An error occurred: {e}", 500
    else:
        return render_template('snomed_spell_checker.html')

def run_spell_check_script(input_file_path):
    # Read the input Excel file
    df = pd.read_excel(input_file_path)

    # Standardize column names
    df.rename(columns={df.columns[3]: 'Old Concept Name', df.columns[6]: 'New Concept Name'}, inplace=True)

    # Run spell check using the preloaded SNOMED words
    spell_check_results = perform_spell_check(df, 'New Concept Name', snomed_words)

    # Save the results to a new Excel file
    output_file_path = os.path.splitext(input_file_path)[0] + '_spell_check.xlsx'

    # Write results to Excel
    wb = load_workbook(input_file_path)
    if "Spell Check Report" in wb.sheetnames:
        wb.remove(wb["Spell Check Report"])
    ws_spell_check = wb.create_sheet("Spell Check Report")
    for col, column_name in enumerate(spell_check_results.columns, start=1):
        ws_spell_check.cell(row=1, column=col, value=column_name)
        ws_spell_check.cell(row=1, column=col).font = Font(bold=True)
    for row, data_row in enumerate(spell_check_results.itertuples(index=False), start=2):
        for col, value in enumerate(data_row, start=1):
            ws_spell_check.cell(row=row, column=col, value=value)

    wb.save(output_file_path)

    return output_file_path

def perform_spell_check(df, column_name, snomed_words):
    spell_check_results = []

    unique_concepts = df[column_name].dropna().unique()

    for concept_name in unique_concepts:
        misspelled_words = []
        words = re.findall(r'\b\w+\b', concept_name.lower())
        
        for word in words:
            if word not in snomed_words:
                # Check if it's a potential misspelling
                matches = process.extract(word, snomed_words, limit=1, scorer=fuzz.ratio)
                if matches and matches[0][1] >= 80:  # If similarity is 80% or higher
                    misspelled_words.append(f"{word} (possible: {matches[0][0]})")
                else:
                    misspelled_words.append(word)
        
        if misspelled_words:
            description = f"Potential misspellings: {', '.join(misspelled_words)}"
            needs_review = 'Yes'
        else:
            description = "No spelling issues detected"
            needs_review = 'No'

        spell_check_results.append({
            'Original Value': concept_name,
            'Description': description,
            'Needs Review': needs_review
        })

    return pd.DataFrame(spell_check_results)

import cohere
import openai
from tqdm import tqdm
import http.client as http_client
import time
import json
import random
import shutil
import gc
from flask import make_response, after_this_request, Response, stream_with_context
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

COHERE_API_KEY = os.getenv("COHERE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
model = "gpt-4o-mini"

co_client = cohere.Client(COHERE_API_KEY)  # Using the synchronous Cohere client
openai.api_key = OPENAI_API_KEY
ALLOWED_EXTENSIONS = {'xlsx'}

class ProgressTracker:
    def __init__(self):
        self.progress = 0
        self.message = ""
        
    def update(self, progress, message=""):
        self.progress = progress
        self.message = message

progress_tracker = ProgressTracker()

@app.route('/progress')
def progress():
    def generate():
        while True:
            # Send progress updates
            data = {
                'progress': progress_tracker.progress,
                'message': progress_tracker.message
            }
            yield f"data: {json.dumps(data)}\n\n"
            time.sleep(0.5)  # Update every 500ms
            if progress_tracker.progress >= 100:
                break
    return Response(stream_with_context(generate()), mimetype='text/event-stream')

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def standardize_column_names(file_path: str) -> str:
    """
    Standardizes column names by renaming specific columns to 'Old Concept Name' and 'New Concept Name'.
    Returns the path of the updated file.
    """
    # Load the Excel file
    df = pd.read_excel(file_path)
    
    # Rename specific columns
    if len(df.columns) > 6:  # Ensure there are enough columns to rename
        df.rename(columns={df.columns[3]: 'Old Concept Name', df.columns[6]: 'New Concept Name'}, inplace=True)
    
    # Save the modified file back to the same path
    df.to_excel(file_path, index=False)
    return file_path

def get_sheet_name(file_path: str) -> str:
    """Get the name of the first sheet in the workbook."""
    try:
        xlsx = openpyxl.load_workbook(file_path, read_only=True)
        sheet_name = xlsx.sheetnames[0]  # Get the first sheet name
        xlsx.close()
        return sheet_name
    finally:
        del xlsx
        gc.collect()

#code additions summary
def code_additions(file_path: str) -> str:
    output_file = os.path.join(os.path.dirname(file_path), 'additions_output.txt')
    flagged_file = os.path.join(os.path.dirname(file_path), 'additions_flagged.csv')
    
    # Remove asyncio.run since we're no longer using async
    process_large_dataset_stream_additions(file_path, output_file, flagged_file)
    return output_file
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Reduce logging level for Cohere library
logging.getLogger('cohere').setLevel(logging.WARNING)

EXCLUDED_CODE_SYSTEMS = {'Read Codes v2', 'Read Codes v3', 'ICD-10-SE'}

def clean_text_additions(text: str) -> str:
    if text is None:
        return ""
    if not isinstance(text, str):
        try:
            text = str(text)
        except:
            return ""
    # Preserve alphanumeric characters, spaces, hyphens, commas, and periods
    text = re.sub(r'[^\w\s\-,.]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def extract_active_code_additions(text: str) -> str:
    if not isinstance(text, str):
        return ""

    lines = text.strip().split('\n')

    asterisk_match = re.findall(r'\*(.*?)\*', text)
    if asterisk_match:
        return asterisk_match[0].strip()

    for line in reversed(lines):
        parentheses_match = re.match(r'(.*)\s+\([^)]+\)$', line)
        if parentheses_match:
            return parentheses_match.group(1).strip()

    return lines[-1].strip()

def stream_excel_additions(file_path: str, sheet_name: str = None, chunk_size: int = 1000) -> Iterator[pd.DataFrame]:
    try:
        xlsx = openpyxl.load_workbook(file_path, read_only=True)
        if sheet_name is None:
            sheet_name = xlsx.sheetnames[0]  # Use first sheet if none specified
        sheet = xlsx[sheet_name]
        
        rows = sheet.iter_rows(values_only=True)
        header = next(rows)  # Get the header row
        
        chunk = []
        for row in rows:
            chunk.append(row)
            if len(chunk) == chunk_size:
                df = pd.DataFrame(chunk, columns=header)
                yield df[['Code System', 'Code', 'Names', 'Old Concept Name', 'New Concept Name']]
                chunk = []
        
        if chunk:  # Don't forget the last chunk if it's not full
            df = pd.DataFrame(chunk, columns=header)
            yield df[['Code System', 'Code', 'Names', 'Old Concept Name', 'New Concept Name']]
    finally:
        xlsx.close()
        del xlsx
        gc.collect()   

def preprocess_row_additions(row: pd.Series) -> Tuple[str, str, str, str]:
    code_system = clean_text_additions(str(row.get('Code System', '')))
    code = clean_text_additions(str(row.get('Code', '')))
    name = clean_text_additions(extract_active_code_additions(str(row.get('Names', ''))))
    new_concept = clean_text_additions(str(row.get('New Concept Name', '')))
    return code_system, code, name, new_concept

def validate_new_concept_additions(new_concept: str, name: str) -> Tuple[bool, str]:
    """
    Validate the New Concept Name entry and return a tuple of (is_valid, reason).
    """
    if not new_concept or new_concept.isspace():
        return False, "Empty or whitespace"
    
    if len(new_concept) < 3:
        return False, "Unusually short"
    
    if len(new_concept) > 200:
        return False, "Unusually long"
    
    common_placeholders = ['N/A', 'None', 'Unknown', 'TBD', 'To Be Determined']
    if new_concept.lower() in (placeholder.lower() for placeholder in common_placeholders):
        return False, f"Common placeholder: {new_concept}"
    
    if new_concept.lower() == name.lower():
        return False, "Identical to Names entry"
    
    return True, "Valid"

def process_chunk_additions(chunk: pd.DataFrame) -> Tuple[Dict[str, Set[str]], List[Tuple[str, str, str, str]]]:
    grouped = defaultdict(set)
    flagged_entries = []
    
    for _, row in chunk.iterrows():
        code_system, code, name, new_concept = preprocess_row_additions(row)
        old_concept = clean_text_additions(str(row.get('Old Concept Name', '')))
        
        if code_system not in EXCLUDED_CODE_SYSTEMS:
            is_valid, reason = validate_new_concept_additions(new_concept, name)
            if is_valid and new_concept != old_concept:
                grouped[new_concept].add(name)
                if old_concept:
                    grouped[old_concept].discard(name)
            elif not is_valid:
                flagged_entries.append((new_concept, code_system, code, reason))
    
    # Remove any concepts that ended up with no codes
    grouped = {k: v for k, v in grouped.items() if v}
    
    return grouped, flagged_entries

def summarize_concept_additions(concept: str, names: Set[str]) -> str:
    if not names:
        return f"{concept}: No new codes were added to this concept."

    names_text = "\n".join(names)
    try:  
        prompt = f"""Summarize the following information for the concept '{concept}':

        {names_text}
                "1. Begin by listing out every unique value in the 'New Concept Name' column. Every unique concept should be listed only once, with no duplicates or omissions.\n"
                "2. For each concept identified in step 1, concisely summarize the values in the 'Names' column that are associated with that Concept.\n"
                "3. Present the summary in the following format: Concept: [preamble] for [code summaries]. Rotate through the following preambles for variety: 'Included codes', 'Added codes specifying', 'Codes were added', 'Added codes for'.\n"
                "4. Do not interpret any of the codes. Only provide a summarization of them.\n"
                "5. Do not include any references to 'Code System' or 'Code' in your summary\n"
                "Example Input:\n"
                "Concepts, Codes\n"
                "Colon Cancer, Stage I Colon Cancer AJCC v8\n"
                "Colon Cancer, Stage I Colon Cancer AJCC v6 and v7\n"
                "Colon Cancer, Ca transverse colon\n"
                "Colon Cancer, Ca descending colon\n"
                "Colon Cancer, Ca ascending colon\n"
                "Colon Cancer, Ca splenic flexure - colon\n"
                "Colon Cancer, Ca sigmoid colon\n"
                "Desired AI Output:\n"
                "Colon Cancer: Includes codes related to colon cancer of various regions of the colon, including transverse, ascending, descending, and sigmoid colon and splenic flexure. Additionally, codes related to staging by various systems, such as AJCC (American Joint Committee on Cancer)."
            )
        Format the summary as:
        {concept}: [Your summary here]"""

        response = co_client.generate(
            model="command-r-plus",
            prompt=prompt,
            max_tokens=250,
            temperature=0.1
        )
        summary = response.generations[0].text.strip()
        if summary.lower().startswith(concept.lower() + ":"):
            summary = summary.split(":", 1)[1].strip()
        return f"{concept}: {summary}"
    except Exception as e:
        logging.error(f"AI generation error for concept {concept}")
        return f"{concept}: Error generating summary"

def get_total_rows_additions(file_path: str) -> int:
    try:
        xl = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        sheet = xl.active  # Selects the first sheet in the workbook
        total_rows = sheet.max_row - 1  # Subtract 1 to exclude header
        return total_rows
    finally:
        xl.close()
        del xl
        gc.collect()

def write_report_additions(output_file: str, summaries: List[str], deprecated_concepts: Set[str]) -> None:
    # Sort summaries alphabetically by concept name
    sorted_summaries = sorted(summaries, key=lambda x: x.split(':', 1)[0].lower())
    
    with open(output_file, "w", encoding='utf-8') as f:
        # Write introductory sentence
        f.write("The following concepts were created or modified and codes were added as described:\n\n")
        
        # Write summaries
        for summary in sorted_summaries:
            f.write(f"{summary}\n\n")
        
        # Write deprecated concepts
        if deprecated_concepts:
            f.write("\nThe following concepts were deprecated:\n")
            for concept in sorted(deprecated_concepts):
                f.write(f"{concept}\n")

def process_large_dataset_stream_additions(file_path: str, output_file: str, flagged_file: str) -> None:
    sheet_name = get_sheet_name(file_path)
    total_rows = get_total_rows_additions(file_path)
    data_stream = stream_excel_additions(file_path, sheet_name)
    
    all_data = defaultdict(set)
    all_flagged_entries = []
    deprecated_concepts = set()
    processed_rows = 0

    # Process chunks with progress updates
    with tqdm(total=total_rows, desc="Processing", unit="rows") as pbar:
        for chunk in data_stream:
            chunk_data, flagged_entries = process_chunk_additions(chunk)
            for concept, names in chunk_data.items():
                all_data[concept].update(names)
            all_flagged_entries.extend(flagged_entries)

            chunk_size = len(chunk)
            processed_rows += chunk_size
            pbar.update(chunk_size)
            
            # Update progress tracker
            progress = min(95, int((processed_rows / total_rows) * 100))
            progress_tracker.update(progress, f"Processing rows: {processed_rows}/{total_rows}")

    sorted_concepts = dict(sorted(all_data.items()))
    
    # Process summaries using ThreadPoolExecutor for parallel processing
    progress_tracker.update(96, "Generating summaries...")
    summaries = []
    with ThreadPoolExecutor() as executor:
        # Use list to force immediate execution of all tasks
        future_to_concept = {
            executor.submit(summarize_concept_additions, concept, names): concept 
            for concept, names in sorted_concepts.items()
        }
        
        for future in concurrent.futures.as_completed(future_to_concept):
            try:
                summary = future.result()
                summaries.append(summary)
            except Exception as e:
                concept = future_to_concept[future]
                logging.error(f"Error processing concept {concept}: {str(e)}")
                summaries.append(f"{concept}: Error generating summary")

    progress_tracker.update(98, "Writing report...")
    write_report_additions(output_file, summaries, deprecated_concepts)
    
    with open(flagged_file, "w", encoding='utf-8') as f:
        f.write("Concept Name,Code System,Code,Reason\n")
        for entry in sorted(all_flagged_entries):
            f.write(f"{','.join(entry)}\n")
    
    progress_tracker.update(100, "Processing complete")

#code removals summary
def code_removals(file_path: str) -> str:
    output_file = os.path.join(os.path.dirname(file_path), 'removals_output.txt')
    flagged_file = os.path.join(os.path.dirname(file_path), 'removals_flagged.csv')

    try:
        # Remove asyncio.run since we're no longer using async
        process_large_dataset_stream_removals(file_path, output_file, flagged_file)
    except Exception as e:
        logging.error(f"An error occurred during processing: {e}")
        raise

    if not os.path.exists(output_file):
        raise FileNotFoundError(f"Expected output file not found: {output_file}")

    return output_file

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set up HTTP request logging
http_client.HTTPConnection.debuglevel = 0  # Change to 1 for more verbose output

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

EXCLUDED_CODE_SYSTEMS = {'Read Codes v2', 'Read Codes v3', 'ICD-10-SE'}

def clean_text_removals(text: str) -> str:
    if text is None:
        return ""
    if not isinstance(text, str):
        try:
            text = str(text)
        except:
            return ""
    # Preserve alphanumeric characters, spaces, hyphens, commas, and periods
    text = re.sub(r'[^\w\s\-,.]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def extract_active_code_removals(text: str) -> str:
    if not isinstance(text, str):
        return ""

    lines = text.strip().split('\n')

    asterisk_match = re.findall(r'\*(.*?)\*', text)
    if asterisk_match:
        return asterisk_match[0].strip()

    for line in reversed(lines):
        parentheses_match = re.match(r'(.*)\s+\([^)]+\)$', line)
        if parentheses_match:
            return parentheses_match.group(1).strip()

    return lines[-1].strip()

def stream_excel_removals(file_path: str, sheet_name: str = None, chunk_size: int = 1000) -> Iterator[pd.DataFrame]:
    try:
        xlsx = openpyxl.load_workbook(file_path, read_only=True)
        if sheet_name is None:
            sheet_name = xlsx.sheetnames[0]  # Use first sheet if none specified
        sheet = xlsx[sheet_name]
        
        rows = sheet.iter_rows(values_only=True)
        header = next(rows)  # Get the header row
        
        chunk = []
        for row in rows:
            chunk.append(row)
            if len(chunk) == chunk_size:
                df = pd.DataFrame(chunk, columns=header)
                yield df[['Code System', 'Code', 'Names', 'Old Concept Name', 'New Concept Name']]
                chunk = []
        
        if chunk:  # Don't forget the last chunk if it's not full
            df = pd.DataFrame(chunk, columns=header)
            yield df[['Code System', 'Code', 'Names', 'Old Concept Name', 'New Concept Name']]
    finally:
        xlsx.close()
        del xlsx
        gc.collect()

def preprocess_row_removals(row: pd.Series) -> Tuple[str, str, str, str, str]:
    code_system = clean_text_removals(str(row.get('Code System', '')))
    code = clean_text_removals(str(row.get('Code', '')))
    name = clean_text_removals(extract_active_code_removals(str(row.get('Names', ''))))
    old_concept = clean_text_removals(str(row.get('Old Concept Name', '')))
    new_concept = clean_text_removals(str(row.get('New Concept Name', '')))
    return code_system, code, name, old_concept, new_concept

def validate_old_concept_removals(old_concept: str, name: str) -> Tuple[bool, str]:
    """
    Validate the Old Concept Name entry and return a tuple of (is_valid, reason).
    """
    if not old_concept or old_concept.isspace():
        return False, "Empty or whitespace"
    
    if len(old_concept) < 3:
        return False, "Unusually short"
    
    if len(old_concept) > 200:
        return False, "Unusually long"
    
    common_placeholders = ['N/A', 'None', 'Unknown', 'TBD', 'To Be Determined']
    if old_concept.lower() in (placeholder.lower() for placeholder in common_placeholders):
        return False, f"Common placeholder: {old_concept}"
    
    if old_concept.lower() == name.lower():
        return False, "Identical to Names entry"
    
    return True, "Valid"

def process_chunk_removals(chunk: pd.DataFrame) -> Tuple[Dict[str, Set[str]], List[Tuple[str, str, str, str]]]:
    grouped = defaultdict(set)
    flagged_entries = []
    
    for _, row in chunk.iterrows():
        code_system, code, name, old_concept, new_concept = preprocess_row_removals(row)
        
        if code_system not in EXCLUDED_CODE_SYSTEMS:
            is_valid, reason = validate_old_concept_removals(old_concept, name)
            if is_valid and old_concept != new_concept:
                grouped[old_concept].add(name)
                logging.debug(f"Added code {code} to concept {old_concept} because it was moved to {new_concept}")
            elif not is_valid:
                flagged_entries.append((old_concept, code_system, code, reason))
                logging.debug(f"Flagged code {code} for concept {old_concept}: {reason}")
            else:
                logging.debug(f"Skipped code {code} for concept {old_concept} because it remains in the same concept")
    
    # Remove any concepts that ended up with no codes
    grouped = {k: v for k, v in grouped.items() if v}
    
    return grouped, flagged_entries

def summarize_concept_removals(concept: str, names: Set[str]) -> str:
    if not names:
        return f"{concept}: No codes were removed from this concept."

    names_text = "\n".join(names)
    try:
        prompt = f"""Summarize the following concept:
Concept: {concept}
Names:
{names_text}

1. Each concept should be summarized only once, with no duplicates or omissions.
2. For each concept, concisely summarize the values in the 'Names' column that are associated with that concept.
3. Present the summary in the following format: Concept: [preamble] for [code summaries].
4. Rotate through these two preambles only: 'Removed codes', 'Codes were removed specifying'.
5. Do not interpret any of the codes. Only provide a summarization of them.
6. Do not include any references to 'Code System' or 'Code' in your summary.

Example of desired output:
Connective Tissue Disorder: Removed codes for unspecified, localized, and other specified connective tissue disorders, with additional codes specifying the body site affected.

Concepts to summarize:

"""
        try:
            # First try OpenAI
            response = openai.ChatCompletion.create(
                model="gpt-4",  # Ensure this matches your available model
                messages=[
                    {"role": "system", "content": "You are a helpful assistant skilled in summarizing medical concepts."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=200
            )
            summary = response.choices[0].message.content.strip()
        except Exception as openai_error:
            # If OpenAI fails, try Cohere as backup
            try:
                response = co_client.generate(
                    model="command-r-plus",
                    prompt=prompt,
                    max_tokens=250,
                    temperature=0.1
                )
                summary = response.generations[0].text.strip()
            except Exception as cohere_error:
                # If both fail, log the errors and raise
                logging.error(f"OpenAI error: {str(openai_error)}")
                logging.error(f"Cohere error: {str(cohere_error)}")
                raise Exception("Both API calls failed")

        # Remove duplicate concept name if present
        if summary.lower().startswith(concept.lower() + ":"):
            summary = summary.split(":", 1)[1].strip()
        return f"{concept}: {summary}"
    except Exception as e:
        logging.error(f"AI generation error for concept {concept}: {str(e)}")
        # Try a simple fallback summarization
        try:
            # Create a basic summary without AI
            name_count = len(names)
            name_sample = list(names)[:3]  # Take up to 3 examples
            if name_count <= 3:
                name_list = ", ".join(name_sample)
            else:
                name_list = f"{', '.join(name_sample[:2])} and {name_count-2} more"
            return f"{concept}: Removed codes including {name_list}"
        except:
            return f"{concept}: Error generating summary"

def get_total_rows_removals(file_path: str) -> int:
    try:
        xl = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        sheet = xl.active  # Selects the first sheet in the workbook
        total_rows = sheet.max_row - 1  # Subtract 1 to exclude header
        return total_rows
    finally:
        xl.close()
        del xl
        gc.collect()

def write_report_removals(output_file: str, summaries: List[str]) -> None:
    with open(output_file, "w", encoding='utf-8') as f:
        # Write introductory sentence
        f.write("The following concepts had codes removed as described:\n\n")
        
        # Write sorted summaries
        sorted_summaries = sorted(summaries, key=lambda x: x.split(':', 1)[0].lower())
        for summary in sorted_summaries:
            f.write(f"{summary}\n\n")

def process_large_dataset_stream_removals(file_path: str, output_file: str, flagged_file: str) -> None:
    sheet_name = get_sheet_name(file_path)
    total_rows = get_total_rows_removals(file_path)
    data_stream = stream_excel_removals(file_path, sheet_name)
    
    all_data = defaultdict(set)
    all_flagged_entries = []
    processed_rows = 0

    # Process chunks with progress updates
    with tqdm(total=total_rows, desc="Processing", unit="rows") as pbar:
        for chunk in data_stream:
            chunk_data, flagged_entries = process_chunk_removals(chunk)
            for concept, names in chunk_data.items():
                all_data[concept].update(names)
            all_flagged_entries.extend(flagged_entries)

            chunk_size = len(chunk)
            processed_rows += chunk_size
            pbar.update(chunk_size)
            
            # Update progress tracker
            progress = min(95, int((processed_rows / total_rows) * 100))
            progress_tracker.update(progress, f"Processing removals: {processed_rows}/{total_rows}")

    sorted_concepts = dict(sorted(all_data.items()))
    
    # Process summaries using ThreadPoolExecutor for parallel processing
    progress_tracker.update(96, "Generating removal summaries...")
    summaries = []
    with ThreadPoolExecutor(max_workers=5) as executor:  # Limit concurrent API calls
        future_to_concept = {
            executor.submit(summarize_concept_removals, concept, names): concept 
            for concept, names in sorted_concepts.items()
        }
        
        for future in concurrent.futures.as_completed(future_to_concept):
            try:
                summary = future.result()
                summaries.append(summary)
                # Add a small delay between API calls to avoid rate limits
                time.sleep(0.1)
            except Exception as e:
                concept = future_to_concept[future]
                logging.error(f"Error processing concept {concept}: {str(e)}")
                # Create a basic summary for failed concepts
                names = sorted_concepts[concept]
                summaries.append(f"{concept}: Removed {len(names)} codes")

    # Sort summaries alphabetically
    summaries.sort(key=lambda x: x.split(':', 1)[0].lower())

    progress_tracker.update(98, "Writing removals report...")
    write_report_removals(output_file, summaries)
    
    with open(flagged_file, "w", encoding='utf-8') as f:
        f.write("Old Concept Name,Code System,Code,Reason\n")
        for entry in sorted(all_flagged_entries):
            f.write(f"{','.join(entry)}\n")
    
    progress_tracker.update(100, "Removals processing complete")

#code relocations summary
def code_relocations(file_path: str, removals_output_file: str):
    """
    Function to handle code relocations and append movement information to the removals output.
    """
    def stream_excel(file_path: str, sheet_name: str = None, chunk_size: int = 1000) -> Iterator[pd.DataFrame]:
        try:
            xlsx = openpyxl.load_workbook(file_path, read_only=True)
            if sheet_name is None:
                sheet_name = xlsx.sheetnames[0]  # Use first sheet if none specified
            sheet = xlsx[sheet_name]
            
            rows = sheet.iter_rows(values_only=True)
            header = next(rows)
            
            chunk = []
            for row in rows:
                chunk.append(row)
                if len(chunk) == chunk_size:
                    df = pd.DataFrame(chunk, columns=header)
                    yield df[['Code System', 'Code', 'Names', 'Old Concept Name', 'New Concept Name']]
                    chunk = []
            
            if chunk:
                df = pd.DataFrame(chunk, columns=header)
                yield df[['Code System', 'Code', 'Names', 'Old Concept Name', 'New Concept Name']]
        finally:
            xlsx.close()
            del xlsx
            gc.collect()

    def process_chunk_for_movements(chunk: pd.DataFrame) -> Tuple[Dict[str, Set[str]], Set[str], Set[str]]:
        movements = defaultdict(set)
        all_concepts = set()
        removed_concepts = set()
        code_to_concepts = defaultdict(set)
        code_to_new_concept = {}
        
        for _, row in chunk.iterrows():
            code_system = row['Code System']
            if code_system not in EXCLUDED_CODE_SYSTEMS:  # Apply same exclusions as other processes
                code, name = row['Code'], row['Names']
                old_concept, new_concept = row['Old Concept Name'], row['New Concept Name']
                
                if pd.notna(old_concept) and pd.isna(new_concept):
                    old_concept_clean = clean_text_removals(str(old_concept))
                    code_to_concepts[code].add(old_concept_clean)
                    all_concepts.add(old_concept_clean)
                    removed_concepts.add(old_concept_clean)
                elif pd.isna(old_concept) and pd.notna(new_concept):
                    new_concept_clean = clean_text_removals(str(new_concept))
                    code_to_new_concept[code] = new_concept_clean
        
        for code, old_concepts in code_to_concepts.items():
            if code in code_to_new_concept:
                new_concept = code_to_new_concept[code]
                for old_concept in old_concepts:
                    movements[old_concept].add(new_concept)
        
        return movements, all_concepts, removed_concepts

    def summarize_movements(concept: str, new_concepts: Set[str]) -> str:
        if not new_concepts:
            return "No codes from this concept were moved to new concepts."
        
        new_concepts_list = sorted(new_concepts)
        if len(new_concepts_list) > 10:
            new_concepts_text = ", ".join(new_concepts_list[:10]) + ", and other concepts"
        elif len(new_concepts_list) == 1:
            new_concepts_text = new_concepts_list[0]
        elif len(new_concepts_list) == 2:
            new_concepts_text = f"{new_concepts_list[0]} and {new_concepts_list[1]}"
        else:
            new_concepts_text = ", ".join(new_concepts_list[:-1]) + f", and {new_concepts_list[-1]}"
        
        return f"Codes in this concept were moved to {new_concepts_text}."

    def process_large_dataset_for_movements(file_path: str, output_file: str):
        sheet_name = get_sheet_name(file_path)
        data_stream = stream_excel(file_path, sheet_name)
        
        progress_tracker.update(0, "Processing relocations...")
        
        all_movements = defaultdict(set)
        all_concepts = set()
        removed_concepts = set()
        processed_chunks = 0
        total_chunks = get_total_rows_additions(file_path) // 1000 + 1  # Estimate chunks
        
        for chunk in data_stream:
            chunk_movements, chunk_concepts, chunk_removed = process_chunk_for_movements(chunk)
            for old_concept, new_concepts in chunk_movements.items():
                all_movements[old_concept].update(new_concepts)
            all_concepts.update(chunk_concepts)
            removed_concepts.update(chunk_removed)
            
            processed_chunks += 1
            progress = min(90, int((processed_chunks / total_chunks) * 100))
            progress_tracker.update(progress, f"Processing relocations: chunk {processed_chunks}/{total_chunks}")

        logging.info(f"Total concepts: {len(all_concepts)}")
        logging.info(f"Concepts with removals: {len(removed_concepts)}")
        logging.info(f"Concepts with movements: {len(all_movements)}")

        # Generate summaries for movements
        progress_tracker.update(95, "Generating relocation summaries...")
        movement_summaries = {concept: summarize_movements(concept, all_movements.get(concept, set())) 
                            for concept in removed_concepts}

        # Read and update the removals output file
        progress_tracker.update(98, "Updating removals report with relocations...")
        try:
            with open(output_file, "r", encoding='utf-8') as f:
                lines = f.readlines()
            
            new_lines = []
            processed_concepts = set()
            for line in lines:
                parts = line.split(":", 1)
                if len(parts) == 2:
                    concept = parts[0].strip()
                    if concept in movement_summaries:
                        # Only append movement summary if not already present
                        existing_text = parts[1].strip()
                        movement_text = movement_summaries[concept]
                        if "moved to" not in existing_text:
                            new_line = f"{concept}: {existing_text} {movement_text}\n"
                        else:
                            new_line = line
                        new_lines.append(new_line)
                        processed_concepts.add(concept)
                    else:
                        new_lines.append(line)
                else:
                    new_lines.append(line)
            
            # Append summaries for any concepts not found in the existing file
            unprocessed_concepts = removed_concepts - processed_concepts
            if unprocessed_concepts:
                new_lines.append("\nAdditional concepts with relocated codes:\n")
                for concept in sorted(unprocessed_concepts):
                    new_lines.append(f"{concept}: {movement_summaries[concept]}\n")
            
            # Write the updated content
            with open(output_file, "w", encoding='utf-8') as f:
                f.writelines(new_lines)
            
            progress_tracker.update(100, "Relocation processing complete")
            logging.info(f"Movement summaries inserted into {output_file}")
        
        except Exception as e:
            logging.error(f"Error updating removals file with movements: {str(e)}")
            progress_tracker.update(100, "Error updating relocations")
            raise

    # Main processing call
    sheet_name = get_sheet_name(file_path)
    process_large_dataset_for_movements(file_path, removals_output_file)

@app.route('/concept_summary', methods=['GET', 'POST'])
def concept_summary():
    if request.method == 'POST':
        selected_options = request.form.getlist('options')
        
        if 'file' not in request.files:
            error = "No file part"
            return render_template('concept_summary.html', error=error)
        
        file = request.files['file']
        
        if file.filename == '':
            error = "No selected file"
            return render_template('concept_summary.html', error=error)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            temp_dir = tempfile.mkdtemp()
            file_path = os.path.join(temp_dir, filename)
            file.save(file_path)

            standardized_file_path = standardize_column_names(file_path)

            try:
                output_files = []
                
                # Process Code Additions
                if 'additions' in selected_options:
                    additions_output = code_additions(file_path)
                    output_files.append(additions_output)
                
                # Process Code Removals
                if 'removals' in selected_options:
                    removals_output = code_removals(file_path)
                    output_files.append(removals_output)
                
                # Process Code Relocations
                if 'relocations' in selected_options:
                    # Ensure code_removals has been run
                    if 'removals' not in selected_options:
                        removals_output = code_removals(file_path)
                        output_files.append(removals_output)
                    code_relocations(file_path, removals_output)

                # Define the path for the final combined output file
                combined_output_path = os.path.join(temp_dir, 'combined_output.txt')
                
                # Combine the outputs from the selected options into a single file
                with open(combined_output_path, 'w') as combined_file:
                    for output_file in output_files:
                        with open(output_file, 'r') as f:
                            combined_file.write(f.read())
                            combined_file.write("\n")  # Add a newline between different outputs

                # Read the content of the combined file
                with open(combined_output_path, 'r') as f:
                    content = f.read()

                # Clean up files before sending response
                try:
                    shutil.rmtree(temp_dir)
                except PermissionError:
                    # If cleanup fails, schedule it for later
                    @after_this_request
                    def cleanup(response):
                        try:
                            shutil.rmtree(temp_dir)
                        except (PermissionError, FileNotFoundError):
                            pass  # If cleanup still fails, let the OS handle it
                        return response

                # Create response with the file content
                response = make_response(content)
                response.headers['Content-Type'] = 'text/plain'
                response.headers['Content-Disposition'] = f'attachment; filename=combined_output.txt'
                return response
                
            except Exception as e:
                # If any error occurs during processing, ensure cleanup
                try:
                    shutil.rmtree(temp_dir)
                except (PermissionError, FileNotFoundError):
                    pass  # If cleanup fails, let the OS handle it
                raise e
                
        else:
            error = "Invalid file type. Please upload an Excel file."
            return render_template('concept_summary.html', error=error)
    else:
        return render_template('concept_summary.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)