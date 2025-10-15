def map_data_manually(source_df, header_row=4, start_row=5):
    """
    Maps data from a source file to a template using a user-defined manual column map.
    """
    MANUAL_COLUMN_MAP = {
    'form_label':'Form Label',
    'item_group':'Item Group (if only one on form, recommend same as Form Label)'	,
    'item_group_repeating':'Item group Repeating'	,
    'item_order':'Item Order',
    'item_label':'Item Label'	,
    'item_name':'Item Name (provided by SDTM Programmer, if SDTM linked item)',
    'data_type':'Data type',
    'codelist':'Codelist - Choice Labels (If many, can use Codelist Tab)',
    'codelist_name':'Codelist Name (provided by SDTM programmer)',
    'required':'Required'}
    column_map=MANUAL_COLUMN_MAP
    try:
        print(f"Reading template headers from row {header_row}...")
        req_headers = ['Form Label',
                            'Item Group (if only one on form, recommend same as Form Label)','Item group Repeating',
                            'Item Order','Item Label',
                            'Item Name (provided by SDTM Programmer, if SDTM linked item)',
                            'Data type',
                            'Codelist - Choice Labels (If many, can use Codelist Tab)','Codelist Name (provided by SDTM programmer)',
                            'Required']
        template_headers = ['New or Copied from Study','Form Label','Form Name(provided by SDTM Programmer, if SDTM linked form)',
                            'Item Group (if only one on form, recommend same as Form Label)','Item group Repeating','Repeat Maximum, if known, else default =50',
                            'Display format of repeating item group (Grid, read only, form)','Default Data in repeating item group','Item Order','Item Label',
                            'Item Name (provided by SDTM Programmer, if SDTM linked item)','Progressively displayed?','Controlling Item (item name if known, else item label)',
                            'Controlling Item Value','','Data type','If text or number, Field Length','If number, Precision (decimal places)',
                            'Codelist - Choice Labels (If many, can use Codelist Tab)','Codelist Name (provided by SDTM programmer)',
                            'Choice Code (provided by SDTM programmer)','Codelist: Control Type','If Number, Range: Min Value - Max Value',
                            "If Date, Query Future Date",'Required','If Required, Open Query when Intentionally Left Blank (form, item)','Notes']																																																																																																																																																															
        
        template_file = pd.DataFrame(columns=template_headers)
        
        print(f"Required columns in Template: {req_headers}")

        # Read the entire source data file
        print(f"Reading data from '{source_df}'...")
        df_source = source_df

        # --- NEW: APPLYING YOUR MANUAL MAP ---
        print("\nApplying manual column map...")

        # 1. Check if all source columns in the map exist in the CSV file
        source_columns_in_map = list(column_map.keys())
        missing_source_cols = set(source_columns_in_map) - set(df_source.columns)
        if missing_source_cols:
            print(f"❌ Error: The following columns from your map were not found in the CSV: {missing_source_cols}")
            return

        # 2. Select only the columns from the source CSV that are in your map
        df_selected = df_source[source_columns_in_map]

        # 3. Rename the selected columns to match the template headers
        df_mapped = df_selected.rename(columns=column_map)
        print(f"Columns after mapping: {df_mapped.columns.tolist()}")
        template_file[req_headers]=df_mapped
        
        # print("\nSaving new file...")
        print("✅ Process completed successfully!")
        return template_file

    except FileNotFoundError as e:
        print(f"❌ Error: File not found. Please check the path: {e.filename}")
    except KeyError as e:
        print(f"❌ Error: A column name was not found. It might be a typo in your map or a wrong sheet name: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
        
def table_ai(combined_data):
    combined_data2 = [{'data':combined_data.to_json(orient='records')}]
    user_prompt_pr = f"""INPUT JSON: {combined_data2}
    Clean and return the structured JSON """
    
    messages_new = [
        {'role': 'system', 'content': system_prompt_pr},
        {'role': 'user', 'content': user_prompt_pr}
    ]
    with st.spinner('🤖 Sending request to AI model...'):
        try:
            response = client.chat.completions.create(
                model="o4-mini",  
                messages=messages_new,
                response_format={"type": "json_object"},
            )
            with st.spinner('📊 Processing AI response...'):
                try:
                    cleaned_data_json = json.loads(response.choices[0].message.content)
                except Exception as e:
                    cleaned_data_json = response.choices[0].message.content
                    
                if 'data' in cleaned_data_json or cleaned_data_json['data']:
                    # all_extracted_data=cleaned_data_json['data']
                    # st.write(pd.DataFrame(cleaned_data_json['data']))
                    return pd.DataFrame(cleaned_data_json['data'])
                elif cleaned_data_json:
                    return pd.DataFrame(cleaned_data_json)
                else:
                    st.warning(f"API returned empty data")# for table {table_idx+1} on page {i+1}.")
            
        except Exception as api_e:
            st.error(f"API error cleaning table: {api_e}")
        
def combine_rows(df3):
    fd = pd.DataFrame()
    df3[0]=df3[0].fillna(method='ffill')
    groups = df3[0].unique().tolist()
    for i in range(len(groups)):  # Changed from range(1, len(groups))
        try:
            # Get group
            group_df = df3[df3[0] == groups[i]]
            df1 = group_df.ffill()
            df2 = group_df.bfill()
            df = pd.concat([df1, df2])
            
            result = {}
            for col in df.columns:
                # print(df[col])
                values = df[col].dropna()
                unique_values = values.unique()
                
                if len(unique_values) == 0:
                    result[col] = np.nan
                elif len(unique_values) == 1:
                    result[col] = unique_values[0]
                else:
                    result[col] = ' '.join(str(v) for v in unique_values)                  
            # Append result
            fd = pd.concat([fd, pd.DataFrame([result])], ignore_index=True)
            
        except Exception as e:
            print(f"Error processing group {groups[i]}: {e}")
            # return pd.DataFrame()
    return fd.drop_duplicates().fillna('').reset_index(drop=True)



system_prompt_pr  = """You are a clinical trial table restructuring expert. Transform the provided Schedule of Activities JSON data.

INPUT ISSUES:
- Merged visits in single cells: "V2D-2\nV2D-1 V2D1" or "V16 V17 V18"
- Merged rows: cells contain "\n" separating two row values
- Incomplete headers: row 0 has parent headers only in first column of each group
- Sometimes row 0 might not have headers (continuation from previous table) - conserve order, do not change
- Misaligned timing/window data
- CRITICAL: Row values may be shifted up or down - values that should align with visit codes might be in wrong rows
- Null values need replacement

TRANSFORMATIONS (execute in order):

0. DETECT AND CORRECT VERTICAL MISALIGNMENT - DO THIS FIRST
   - Analyze patterns in rows with similar data types (timing values, X marks, numeric values)
   - Look for patterns where values appear shifted by 1-2 rows up or down
   - Common indicators of misalignment:
     * Timing values (numbers, ±X) appearing in wrong rows relative to "Timing of Visit" label
     * X marks appearing in rows above/below procedure names
     * Empty cells where X marks should be, with X marks in adjacent rows
     * Day numbers appearing in "Visit Window" row instead of "Timing of Visit" row
   
   Detection algorithm:
   a) Identify row labels in column 0: "Visit", "Timing of Visit (Days)", "Visit Window (Days)", procedure names
   b) Check if data in columns 1+ matches expected pattern for that label
   c) If mismatch detected, check rows immediately above/below for better pattern match
   d) Shift row values up/down to correct alignment
   
   Example of misalignment:
   INPUT (misaligned):
   Row 2: ["Timing of Visit (Days)", "", "", ""]
   Row 3: ["Visit Window (Days)", "1", "8", "15"]  <- Day values in wrong row
   Row 4: ["Procedure X", "±2", "±3", "±3"]  <- Window values in wrong row
   
   OUTPUT (corrected):
   Row 2: ["Timing of Visit (Days)", "1", "8", "15"]  <- Corrected
   Row 3: ["Visit Window (Days)", "±2", "±3", "±3"]  <- Corrected
   Row 4: ["Procedure X", "", "", ""]

   Pattern matching rules:
   - "Timing of Visit (Days)" row should contain: numbers (1, 8, 15, 22, etc.)
   - "Visit Window (Days)" row should contain: ±N format (±2, ±3, ±12, etc.) or empty
   - "Timing of Visit (Weeks)" row should contain: numbers or ranges (12, 13, 02-Oct, etc.)
   - Procedure rows should contain: X marks, Xp, Xh, Xo, or empty strings
   
   If pattern doesn't match, scan 1-2 rows above/below and realign values to correct rows.

1. UNMERGE ROWS - CRITICAL: REPLACE, DON'T DUPLICATE
   - IF a cell contains "\n", that row represents TWO rows merged
   - REMOVE the original merged row from output
   - REPLACE it with TWO separate rows
   - Split ALL cells in that row at "\n"
   
   Example:
   INPUT: 
   Row 3: ["Phase A", "V1\nV2", "Day 1\nDay 2", "X\nXp"]
   
   OUTPUT (Row 3 is REPLACED by two rows):
   Row 3: ["Phase A", "V1", "Day 1", "X"]
   Row 4: ["Phase A", "V2", "Day 2", "Xp"]

2. SPLIT MERGED VISITS - REPLACE, DON'T DUPLICATE
   - When ONE cell contains multiple visits (space or \n separated)
   - REMOVE that column from output
   - REPLACE with MULTIPLE columns (one per visit)
   - Distribute X marks (including Xp, Xh, Xo variants) and timing to corresponding new columns

3. RECONSTRUCT HEADERS (row 0) - ONLY IF HEADERS EXIST
   - If row 0 is blank/continuation, skip this step and preserve as-is
   - If row 0 contains parent headers spanning multiple columns:
     * Visit code format changes indicate new phase groups (V1→V2D-1→SxD1→V14)
     * Propagate parent header to ALL columns in its group with subscripts (_1, _2, _3)

4. PROPAGATE PHASE NAMES (column 0)
   - When column "0" is empty/null, fill with phase name from nearest row above
   - Keep phase names consistent

5. CLEAN TEXT
   - Remove ALL "\n" from final output
   - Fix broken words: "Withdraw al" → "Withdrawal"
   - Fix spellings, keep section numbers: "10.1.3", "8.1"
   - Replace None/null with ""
   - CRITICAL: Do NOT modify X mark variants - preserve exactly as-is:
     * "X" stays "X"
     * "Xp" stays "Xp" (not "X")
     * "Xh" stays "Xh" (not "X")
     * "Xo" stays "Xo" (not "X")
     * "Xa" stays "Xa" (not "X")

6. ALIGN DATA
   - After step 0 corrections, verify:
   - "Timing of Visit (Days)" row: contains day numbers (1, 8, 15, etc.)
   - "Visit Window (Days)" row: contains ±N values (±2, ±3, etc.)
   - X marks in procedure rows: aligned with correct visit columns

CRITICAL RULES:
- Execute step 0 (detect misalignment) BEFORE all other steps
- When values are shifted, move entire row of values up/down to match correct label
- When splitting rows/columns: DELETE original, REPLACE with split versions
- Count "\n" to detect merged rows
- Never reorder rows (except when correcting misalignment or replacing merged rows)
- PRESERVE ALL X MARK VARIANTS EXACTLY: X, Xp, Xh, Xo, Xa

MISALIGNMENT EXAMPLES:

Example 1 - Day values shifted down:
INPUT (wrong):
Row 1: ["Visit", "V1", "V2", "V3"]
Row 2: ["Timing of Visit (Days)", "", "", ""]
Row 3: ["Visit Window (Days)", "1", "8", "15"]

OUTPUT (corrected):
Row 1: ["Visit", "V1", "V2", "V3"]
Row 2: ["Timing of Visit (Days)", "1", "8", "15"]
Row 3: ["Visit Window (Days)", "", "", ""]

Example 2 - X marks shifted up:
INPUT (wrong):
Row 5: ["Medical History", "X", "X", ""]
Row 6: ["Concomitant Medication", "", "", "X"]

(If pattern analysis shows Medical History should not have X at V1, but Concomitant Medication should)

OUTPUT (corrected):
Row 5: ["Medical History", "", "", ""]
Row 6: ["Concomitant Medication", "X", "X", "X"]

OUTPUT:
Return ONLY this JSON (no markdown, no explanations):

{"data": [[row0_values], [row1_values], [row2_values], ...]}

Return only the JSON object."""

def extract_table_pages(pdf_file):
    """Extract pages containing Schedule of Activities tables"""

    # Patterns to find headings
    # schedule_pattern = re.compile(r"schedule of activities|Schedule of activities|Schedule of Activities", re.IGNORECASE)
    # intro_pattern = re.compile(r"Introduction")
    
    # Find start page
    schedule_start_page = None
    intro_start_page = None

    schedule_pattern = re.compile(r"schedule of activities|Schedule of Activities|Schedule of activities|Schedule Of Activities", re.IGNORECASE)
    intro_pattern = re.compile(r"Introduction", re.IGNORECASE)
    index_pattern = re.compile(r"Table of contents",re.IGNORECASE)

    pdf_document = fitz.open(pdf_file)
    page_texts = []
    for page_num in range(len(pdf_document)):
        page = pdf_document[page_num]
        
        # Try different extraction methods
        text = page.get_text("text", sort=True)
        if not index_pattern.search(text):
            # st.write(text)
            if schedule_pattern.search(text):
                schedule_start_page = page_num + 1
            if intro_pattern.search(text):
                intro_start_page = page_num + 1
            if schedule_start_page and intro_start_page:
                st.write("Start:",schedule_start_page," End:",intro_start_page)
                break
    
    if not schedule_start_page:
        pdf_document.close()
        return None
    else:
        end_page = intro_start_page if intro_start_page else len(pdf_document)
    
    table_settings = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "snap_tolerance": 300,
        "edge_min_length": 100,
    }
    
    consecutive_empty_pages = 0
    max_empty_pages = 2

    with pdfplumber.open(pdf_file) as pdf:
        for i in range(schedule_start_page - 1, len(pdf.pages)):  # 0-indexed
            page = pdf.pages[i]
            tables_on_page = page.extract_tables(table_settings=table_settings)
            if i==intro_start_page:
                # end_page=intro_start_page
                break
            elif tables_on_page and any(len(table) > 2 for table in tables_on_page):
                # Found tables with substance (more than just headers)
                end_page = i + 1  # 1-indexed
                consecutive_empty_pages = 0
            else:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_empty_pages or not intro_start_page:
                    break
    
    st.write(f"Tables detected from page {schedule_start_page} to page {end_page}")
    
    # Extract the identified range
    output_pdf = fitz.open()
    output_pdf.insert_pdf(pdf_document, from_page=schedule_start_page - 1, to_page=end_page - 1)
    
    if output_pdf.page_count > 0:
        extracted_pdf_path = "Schedule_of_Activities.pdf"
        output_pdf.save(extracted_pdf_path)
        pdf_document.close()
        
        st.write(f"Saved {output_pdf.page_count} pages to {extracted_pdf_path}")
        output_pdf.close()
        return extracted_pdf_path
    else:
        output_pdf.close()
        pdf_document.close()
        st.warning("No pages extracted for Schedule of Activities.")
        return None


def process_protocol_pdf_pdfplumber(extracted_pdf_path, system_prompt_pr) -> pd.DataFrame:
    """Process the extracted PDF to get tables and clean with API"""
    
    # Check if file exists
    if not extracted_pdf_path or not os.path.exists(extracted_pdf_path):
        st.error(f"PDF file not found at path: {extracted_pdf_path}")
        return pd.DataFrame()
    
    all_extracted_data = []
    
    table_settings = {
    #         "vertical_strategy": "lines", "horizontal_strategy": "lines","explicit_vertical_lines": [],
    # "explicit_horizontal_lines": [],"snap_tolerance": 300,
    # # "snap_x_tolerance": 6,
    # # "snap_y_tolerance": 5.16,
    # # "join_tolerance": 1,
    # # "join_x_tolerance": 5,
    # # "join_y_tolerance": 23,
    # # "edge_min_length": 25,
    # # "min_words_vertical": 3,
    # # "min_words_horizontal": 1,
    # # "intersection_tolerance": 1,
    # # "intersection_x_tolerance": 1,
    # # "intersection_y_tolerance": 5,
    # # "text_tolerance": 3,
    # # "text_x_tolerance": 5,
    # # "text_y_tolerance": 3,
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "explicit_vertical_lines": [],
        "explicit_horizontal_lines": [],
        "snap_tolerance": 300,
        "snap_x_tolerance": 6,
        "snap_y_tolerance": 5.16,
        "join_tolerance": 1,
        "join_x_tolerance": 2,
        "join_y_tolerance": 3,
        "edge_min_length": 100,
        "min_words_vertical": 3,
        "min_words_horizontal": 1,
        "intersection_tolerance": 1,
        "intersection_x_tolerance": 0.4,
        "intersection_y_tolerance": 2,
        "text_tolerance": 3,
        "text_x_tolerance": 5,
        "text_y_tolerance": 3,
    }
    
    st.write(f"Processing extracted PDF: {extracted_pdf_path}")
    
    try:
        with pdfplumber.open(extracted_pdf_path) as pdf:
            st.write(f"Opened PDF with {len(pdf.pages)} pages for table extraction.")
            
            progress_text = "Extracting tables from Protocol REF PDF..."
            my_bar = st.progress(0, text=progress_text)
            df = pd.DataFrame()
            df_ai = pd.DataFrame()
            combined_data = pd.DataFrame()
            for i in range(len(pdf.pages)):
                page = pdf.pages[i]
                st.write(f"Processing page {i+1}...")
                
                # Extract tables
                tables_on_page = page.extract_tables(table_settings=table_settings)
                
                if tables_on_page:
                    st.write(f"Found {len(tables_on_page)} tables on page {i+1}.")
                    raw_data = pd.DataFrame()
                    for table_idx, table_data in enumerate(tables_on_page):
                        if not table_data or len(table_data) < 2:
                            continue
                        # Convert to list of lists
                        raw_data = pd.concat((raw_data,pd.DataFrame(table_data)))
                    
                    # combined_data = combine_rows(raw_data)
                    combined_data = raw_data.copy()
                    st.write(combined_data)
                            
                if not combined_data.empty:
                    nd = table_ai(combined_data)
                    st.write('Post processed with AI')
                    st.write(nd)
                    df = pd.concat((df,combined_data)) 
                    df_ai = pd.concat((df_ai,nd))   
                # Update progress
                progress_percentage = (i + 1) / len(pdf.pages)
                my_bar.progress(
                    progress_percentage,
                    text=f"Extracting tables from Protocol REF PDF (page {i+1}/{len(pdf.pages)})..."
                )
            
            my_bar.empty()
            all_extracted_data = df_ai
            if not all_extracted_data.empty:
                # Convert to DataFrame
                pr_df = pd.DataFrame(all_extracted_data)
                # pr_df = df.copy()
                
                if not pr_df.empty:
                    st.write("With AI Post processing")
                    st.write(pr_df)
                    st.write("Without AI Post processing")
                    st.write(df)
                    # Set first row as header
                    pr_df.columns = pr_df.iloc[0]
                    pr_df = pr_df[1:].reset_index(drop=True)
                    
                    # Drop empty columns
                    pr_df = pr_df.dropna(axis=1, how='all')
                    
                    # pr_data = [{'data':pr_df.to_json(orient='records')}]
                    return pr_df
            else:
                st.warning("No tables extracted from the Protocol REF PDF.")
                return pd.DataFrame()
    
    except FileNotFoundError:
        st.error(f"File not found: {extracted_pdf_path}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error processing Protocol REF PDF: {e}")
        return pd.DataFrame()
