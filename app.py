import os
import re
import json
import sqlite3

# Third-Party Library Imports
from flask import Flask, render_template, request, send_file, redirect, url_for, session
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from werkzeug.utils import secure_filename
from rapidfuzz import process, fuzz

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

snomed_words = get_snomed_words()
print(f"Loaded {len(snomed_words)} unique words from SNOMED CT")

snomed_words = get_snomed_words()
print(f"Loaded {len(snomed_words)} unique words from SNOMED CT")

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

import openpyxl
from openpyxl import Workbook

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)