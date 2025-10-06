import streamlit as st
import re
import fitz
import pandas as pd
from docx import Document
from typing import List, Dict, Any
from openai import OpenAI
import json
import os
import pdfplumber
import numpy as np

# Ensure you have your OpenAI API key set up as an environment variable or use Streamlit secrets
api_key = st.secrets["api_key"]
# For this example, I'll use the key you provided, but using Streamlit secrets is recommended for deployment
client = OpenAI(api_key=api_key)

def map_data_manually(source_df, header_row=4, start_row=5):
    """
    Maps data from a source file to a template using a user-defined manual column map.
    """
    MANUAL_COLUMN_MAP = {
    'form_label':'Form Label',
    'form_type':'Form Type',
    'item_group':'Item Group (if only one on form, recommend same as Form Label)'	,
    'item_group_repeating':'Item group Repeating'	,
    'item_order':'Item Order',
    'item_label':'Item Label'	,
    'item_name':'Item Name (provided by SDTM Programmer, if SDTM linked item)',
    'data_type':'Data type',
    'codelist':'Codelist - Choice Labels (If many, can use Codelist Tab)',
    'codelist_name':'Codelist Name (provided by SDTM programmer)',
    'required':'Required',
    # 'items':''
    }
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
    

class DOCXCRFChunker:
    def __init__(self, max_chunk_size: int = 15000, overlap_size: int = 500):
        self.max_chunk_size = max_chunk_size
        self.overlap_size = overlap_size

        # CRF form detection patterns
        self.form_patterns = [
            r'sCRF\s+v\d+\.\d+',  # sCRF v1.0
            r'\[[\w_]+\]',        # [FORM_CODE]
            r'V\d+(?:,\s*V\d+)*', # V1, V2, V3 (visit schedules)
        ]

    def extract_and_chunk(self, docx_path: str):
        """Extract text from DOCX and create table-aware chunks"""

        doc = Document(docx_path)
        elements = self._extract_structured_content(doc)
        chunks = self._create_chunks(elements)

        return chunks

    def _extract_structured_content(self, doc: Document) -> List[Dict[str, Any]]:
        """Extract content while preserving structure"""

        elements = []

        for element in doc.element.body:
            if element.tag.endswith('p'):  # Paragraph
                para = self._get_paragraph_from_element(doc, element)
                if para and para.text.strip():
                    elements.append({
                        'type': 'paragraph',
                        'text': para.text,
                        'style': para.style.name if para.style else 'Normal',
                        'is_heading': self._is_heading(para),
                        'is_form_title': self._is_form_title(para.text),
                        'length': len(para.text)
                    })

            elif element.tag.endswith('tbl'):  # Table
                table = self._get_table_from_element(doc, element)
                if table:
                    table_text = self._extract_table_text(table)
                    elements.append({
                        'type': 'table',
                        'text': table_text,
                        'rows': len(table.rows),
                        'cols': len(table.columns) if table.rows else 0,
                        'length': len(table_text),
                        'is_crf_table': self._is_crf_table(table_text)
                    })

        return elements

    def _get_paragraph_from_element(self, doc, element):
        """Get paragraph object from XML element"""
        for para in doc.paragraphs:
            if para._element == element:
                return para
        return None

    def _get_table_from_element(self, doc, element):
        """Get table object from XML element"""
        for table in doc.tables:
            if table._element == element:
                return table
        return None

    def _is_heading(self, para) -> bool:
        """Check if paragraph is a heading"""
        if para.style:
            style_name = para.style.name.lower()
            return 'heading' in style_name or 'title' in style_name
        return False

    def _is_form_title(self, text: str) -> bool:
        """Check if text is a CRF form title"""
        for pattern in self.form_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True

        # Additional form title indicators
        form_indicators = [
            'collection of samples',
            'contraception',
            'c-ssrs',
            'patient health questionnaire',
            'weight history',
            'hand grip test',
            'mri scan consent'
        ]

        text_lower = text.lower()
        return any(indicator in text_lower for indicator in form_indicators)

    def _extract_table_text(self, table) -> str:
        """Extract text from table preserving structure"""
        table_text = []

        for row_idx, row in enumerate(table.rows):
            row_text = []
            for cell in row.cells:
                cell_text = cell.text.strip()
                if cell_text:
                    row_text.append(cell_text)

            if row_text:
                table_text.append(" | ".join(row_text))

        return "\n".join(table_text)

    def _is_crf_table(self, table_text: str) -> bool:
        """Check if table contains CRF content"""
        crf_indicators = [
            'yes', 'no', 'radio', 'checkbox',
            'required', 'optional', '*',
            'integration', 'argus', 'cosmos',
            'item label', 'data type', 'codelist'
        ]

        text_lower = table_text.lower()
        return sum(1 for indicator in crf_indicators if indicator in text_lower) >= 2

    def _create_chunks(self, elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create table-aware chunks from extracted elements"""

        chunks = []
        current_chunk = {
            'chunk_id': 1,
            'elements': [],
            'text': '',
            'length': 0,
            'form_context': '',
            'has_tables': False
        }

        for element in elements:
            # Check if this element starts a new form
            if (element['type'] == 'paragraph' and
                element.get('is_form_title', False)):

                # Finish current chunk if it has content
                if current_chunk['length'] > 0:
                    chunks.append(self._finalize_chunk(current_chunk))
                    current_chunk = self._create_new_chunk(len(chunks) + 1)

                # Set form context for new chunk
                current_chunk['form_context'] = element['text']

            # Check if adding this element exceeds chunk size
            if (current_chunk['length'] + element['length'] > self.max_chunk_size and
                current_chunk['length'] > 0):

                # Don't break in the middle of a table
                if element['type'] == 'table':
                    # Finish current chunk and start new one
                    chunks.append(self._finalize_chunk(current_chunk))
                    current_chunk = self._create_new_chunk(len(chunks) + 1)
                    current_chunk['form_context'] = chunks[-1]['form_context']

            # Add element to current chunk
            current_chunk['elements'].append(element)
            current_chunk['text'] += element['text'] + '\n\n'
            current_chunk['length'] += element['length']

            if element['type'] == 'table':
                current_chunk['has_tables'] = True

        # Add final chunk
        if current_chunk['length'] > 0:
            chunks.append(self._finalize_chunk(current_chunk))

        # Add overlap between chunks
        chunks = self._add_overlap(chunks)

        return chunks


    def _create_new_chunk(self, chunk_id: int) -> Dict[str, Any]:
        """Create a new empty chunk"""
        return {
            'chunk_id': chunk_id,
            'elements': [],
            'text': '',
            'length': 0,
            'form_context': '',
            'has_tables': False
        }

    def _finalize_chunk(self, chunk: Dict[str, Any]) -> Dict[str, Any]:
        """Finalize chunk with metadata"""

        # Add form context at the beginning if not already there
        if chunk['form_context'] and chunk['form_context'] not in chunk['text'][:200]:
            chunk['text'] = f"FORM CONTEXT: {chunk['form_context']}\n\n" + chunk['text']

        # Calculate statistics
        chunk['paragraph_count'] = sum(1 for el in chunk['elements'] if el['type'] == 'paragraph')
        chunk['table_count'] = sum(1 for el in chunk['elements'] if el['type'] == 'table')
        chunk['crf_table_count'] = sum(1 for el in chunk['elements']
                                     if el['type'] == 'table' and el.get('is_crf_table', False))

        return chunk


    def _add_overlap(self, chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Add overlap between consecutive chunks"""

        if len(chunks) <= 1:
            return chunks

        for i in range(1, len(chunks)):
            prev_chunk = chunks[i-1]
            current_chunk = chunks[i]

            # Get overlap text from previous chunk
            prev_text = prev_chunk['text']
            overlap_text = prev_text[-self.overlap_size:] if len(prev_text) > self.overlap_size else prev_text

            # Add overlap to current chunk with marker
            current_chunk['text'] = f"OVERLAP FROM PREVIOUS:\n{overlap_text}\n\nCURRENT CHUNK:\n" + current_chunk['text']
            current_chunk['has_overlap'] = True

        return chunks

    def get_chunk_summary(self, chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get summary statistics for chunks"""

        return {
            'total_chunks': len(chunks),
            'total_length': sum(chunk['length'] for chunk in chunks),
            'avg_chunk_size': sum(chunk['length'] for chunk in chunks) / len(chunks) if chunks else 0,
            'chunks_with_tables': sum(1 for chunk in chunks if chunk['has_tables']),
            'total_tables': sum(chunk.get('table_count', 0) for chunk in chunks),
            'total_crf_tables': sum(chunk.get('crf_table_count', 0) for chunk in chunks),
            'forms_identified': len(set(chunk['form_context'] for chunk in chunks if chunk['form_context']))
        }

    
# Usage example

def combine_chunks(chunks,max_tokens):
    i,j=0,0
    updated_chunks=['0']
    length=len(chunks)
    while i+1 in range(len(chunks)):
      if i==0:
        updated_chunks[j]=chunks[i].copy()
      else:
        count_len = updated_chunks[j]['length']+chunks[i]['length'] * 1.25
        print(count_len)
        if count_len <3500:
          updated_chunks[j] = {
             'chunk_id': f"{chunks[i].get('chunk_id')}-{updated_chunks[j].get('chunk_id')}",
             'elements': chunks[i].get('elements', []) + updated_chunks[j].get('elements', []),
             'text': chunks[i].get('text', '') + "\n\n" + updated_chunks[j].get('text', ''),
             'length': chunks[i].get('length', 0) + updated_chunks[j].get('length', 0),
             'form_context': chunks[i].get('form_context', '')+'\n\n'+ updated_chunks[j].get('form_context', ''), 
             'has_tables': chunks[i].get('has_tables', False) or updated_chunks[j].get('has_tables', False),
             'paragraph_count': chunks[i].get('paragraph_count', 0) + updated_chunks[j].get('paragraph_count', 0),
             'table_count': chunks[i].get('table_count', 0) + updated_chunks[j].get('table_count', 0),
             'crf_table_count': chunks[i].get('crf_table_count', 0) + updated_chunks[j].get('crf_table_count', 0),
             'has_overlap': chunks[i].get('has_overlap', False) or updated_chunks[j].get('has_overlap', False) # Indicate if either had overlap
          }
        else:
          j+=1
          updated_chunks.append('0')
          updated_chunks[j]=chunks[i].copy()
    
      i+=1
    return updated_chunks
def process_crf_docx(docx_path: str) -> List[Dict[str, Any]]:
    """Process a CRF DOCX file and return chunks"""

    chunker = DOCXCRFChunker(max_chunk_size=15000, overlap_size=500)
    chunks = chunker.extract_and_chunk(docx_path)
    chunks = combine_chunks(chunks,1500)

    # Print summary (optional, can be displayed in Streamlit)
    summary = chunker.get_chunk_summary(chunks)
    st.write(f"Created {summary['total_chunks']} chunks from Mock CRF.")
    st.write(f"Forms identified: {summary['forms_identified']}")

    return chunks


# Function to process the Protocol REF PDF and extract table data using pdfplumber
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

# System and User Prompts for OpenAI (CRF Extraction)
System_prompt="""You are a CRF (Case Report Form) extraction specialist. Your task is to extract structured information from clinical research document chunks and return it as valid JSON.
REQUIRED OUTPUT FORMAT: Return ONLY a valid JSON object with the structure below. Do not include any explanatory text before or after the JSON.
{
  "forms": [
    {
      "form_label": "string",
      "form_code": "string",
      "visits": "string",
      "form_type": "string",
      "item_group": "string",
      "item_group_repeating": "Y/N",
      "item_order": number,
      "item_label": "string",
      "item_name": "[SDTM Programmer]",
      "data_type": "string",
      "codelist": "string",
      "codelist_name": "string",
      "control_time": "",
      "required": "Y/N"
    }
  ],
  "chunk_metadata": {
    "chunk_id": "string",
    "forms_found": number,
    "items_extracted": number
  }
}

EXTRACTION RULES:
1. Form Label: Extract the main form title (e.g., "Collection of Samples for Laboratory (Lab_1)")
2. Form Code: Extract code in brackets (e.g., "[LAB_SMPL_TKN]")
3. Visits: Extract visit schedule (e.g., "V1, V4, V5, V6")
4. Form Type: "Non-repeating form" or "Repeating form"
5. Item Group: Logical sections like "Blood", "Urine", "Contraception"
6. Item Group Repeating: "Yes" if section can repeat, "No" otherwise
7. Item Order: Sequential number within form (1, 2, 3...)
8. Item Label: Exact question text
9. Item Name: Always "[SDTM Programmer]"
10. Data Type: "Radio Button", "Numeric", "Text", "Date", "Checkbox"
11. Codelist: Available options (e.g., "Yes/No", "4-point Scale")
12. Codelist Name: Logical name for options
13. Required: "Yes" if marked with asterisk (*), "No" otherwise

DATA TYPE MAPPING:
- Questions with radio options → "Radio Button"
- Numeric inputs (age, weight, scores) → "Numeric"
- Free text fields → "Text"
- Date fields → "Date"
- Multiple selections → "Checkbox"

Return valid JSON only. No other text."""


def user_prompt(text):
  prompt=f"""Extract CRF information from the following document chunk and return as JSON:

  CHUNK CONTENT:
  {text}

  Analyze this content and extract all CRF forms, item groups, and individual items following the specified JSON format. Pay special attention to:
  - Form titles and codes
  - Required fields marked with asterisks (*)
  - Item groups and their organization
  - Question text and response options
  - Data types based on field characteristics

  Return only valid JSON with no additional text."""

  return prompt

# Function to call OpenAI API for each chunk
def ai_extract(chunks, System_prompt):
    conversation = [{"role": "system", "content": System_prompt}]
    all_forms_data = []

    progress_text = "Operation in progress. Please wait."
    my_bar = st.progress(0, text=progress_text)

    for i, chunk in enumerate(chunks):
        user_msg = user_prompt(chunk['text'])
        messages = conversation + [{"role": "user", "content": user_msg}]

        try:
            response = client.chat.completions.create(
                model="o4-mini",  # Using gpt-4o-mini as it's generally available and cost-effective
                messages=messages,
                response_format={"type": "json_object"}
            )
            answer = json.loads(response.choices[0].message.content)

            if 'forms' in answer:
                all_forms_data.extend(answer['forms'])

            # Update progress bar
            progress_percentage = (i + 1) / len(chunks)
            my_bar.progress(progress_percentage, text=f"Processing chunk {i+1}/{len(chunks)}")

        except Exception as e:
            st.error(f"Error processing chunk {chunk['chunk_id']}: {e}")
            st.write("Attempting to continue with next chunk...")
            # You might want to log the problematic chunk or handle specific API errors

    my_bar.empty() # Clear progress bar on completion

    if all_forms_data:
        df = pd.DataFrame(all_forms_data)
        return df
    else:
        st.warning("No forms data extracted from the Mock CRF chunks.")
        return pd.DataFrame()

# import streamlit as st

# Initialize session state
if 'protocol_df' not in st.session_state:
    st.session_state.protocol_df = None
if 'crf_df' not in st.session_state:
    st.session_state.crf_df = None
if 'protocol_ready' not in st.session_state:
    st.session_state.protocol_ready = False
if 'crf_ready' not in st.session_state:
    st.session_state.crf_ready = False
if 'protocol_error' not in st.session_state:
    st.session_state.protocol_error = None
if 'crf_error' not in st.session_state:
    st.session_state.crf_error = None

# --- Streamlit App Layout ---
st.title("Clinical Document Processor")
st.write("Upload your Mock CRF (.docx) and Protocol REF (.pdf) documents to extract and process data.")

# File Uploaders
uploaded_crf_file = st.file_uploader("Upload Mock CRF (.docx)", type="docx")
uploaded_protocol_file = st.file_uploader("Upload Protocol REF (.pdf)", type="pdf")

# Process Button
if st.button("Process Documents", type="primary"):
    if uploaded_crf_file and uploaded_protocol_file:
        
        # Save uploaded files temporarily
        crf_path = "temp_crf.docx"
        protocol_path = "temp_protocol.pdf"
        
        with open(crf_path, "wb") as f:
            f.write(uploaded_crf_file.getbuffer())
        with open(protocol_path, "wb") as f:
            f.write(uploaded_protocol_file.getbuffer())
        
        st.success("Files uploaded successfully")
        
        # Reset states
        st.session_state.protocol_ready = False
        st.session_state.crf_ready = False
        st.session_state.protocol_error = None
        st.session_state.crf_error = None
        
        # Process Protocol (independent try-except)
        st.subheader("Protocol REF Processing")
        try:
            with st.spinner("Identifying the required tables..."):
                extracted_pdf_path = extract_table_pages(protocol_path)
            
            if extracted_pdf_path:
                protocol_progress = st.empty()
                protocol_df = process_protocol_pdf_pdfplumber(
                    extracted_pdf_path, 
                    system_prompt_pr
                )
                protocol_progress.empty()
                
                if not protocol_df.empty:
                    st.success(f"Extracted {len(protocol_df)} rows")
                    st.session_state.protocol_df = protocol_df
                    st.session_state.protocol_ready = True
                    
                    try:
                        st.write('Combined Processed Table')
                        st.dataframe(protocol_df)
                    except Exception as e:
                        dup1 = protocol_df.copy()
                        dup1.columns = [f"{c}_{i}" for i,c in enumerate(dup1.columns)]
                        st.write("Combined Processed Table")
                        st.dataframe(dup1)
                    
                    # Cleanup protocol temp files
                    if extracted_pdf_path and os.path.exists(extracted_pdf_path):
                        os.remove(extracted_pdf_path)
                else:
                    st.warning("No Protocol data extracted")
            else:
                st.error("Could not identify Schedule of Activities pages")
                
        except Exception as e:
            st.session_state.protocol_error = str(e)
            st.error(f"Protocol processing failed: {e}")
            st.info("CRF processing will continue independently...")
        
        # Process Mock CRF (independent try-except)
        st.subheader("Mock CRF Processing")
        try:
            with st.spinner("Chunking document..."):
                crf_chunks = process_crf_docx(crf_path)
                st.info(f"Created {len(crf_chunks)} chunks")
            
            with st.spinner("Extracting CRF data..."):
                crf_df = ai_extract(crf_chunks, System_prompt)
            
            if not crf_df.empty:
                st.success(f"Extracted {crf_df.shape[0]} items")
                source_data = crf_df
                final_crf=map_data_manually(source_data,
                    header_row=4,
                    start_row=5)
                st.session_state.crf_df = final_crf
                st.session_state.crf_ready = True
        
                try:
                    st.write('Extracted Values from Document')
                    st.dataframe(crf_df)
                    st.write('Final Mapped CRF Table')
                    st.dataframe(final_crf)
                except Exception as e:
                    dup2 = crf_df.copy()
                    dup2.columns = [f"{c}_{i}" for i,c in enumerate(dup2.columns)]
                    st.write('Extracted Values from Document')
                    st.dataframe(dup2)
                    st.write('Final Mapped CRF Table')
                    st.dataframe(final_crf)
            else:
                st.warning("No CRF data extracted")
                
        except Exception as e:
            st.session_state.crf_error = str(e)
            st.error(f"CRF processing failed: {e}")
        
        # Cleanup saved files
        for f in [crf_path, protocol_path]:
            if os.path.exists(f):
                os.remove(f)
        
        # Summary
        if st.session_state.protocol_ready or st.session_state.crf_ready:
            st.success("Processing complete! Download your results below.")
        else:
            st.error("Both processes failed. Please check your files and try again.")

# Download section - Protocol (appears as soon as protocol is ready)
if st.session_state.protocol_ready:
    st.subheader("Protocol Results Ready")
    st.download_button(
        "📥 Download Protocol Data",
        data=st.session_state.protocol_df.to_csv(index=False).encode('utf-8'),
        file_name='protocol_extraction.csv',
        mime='text/csv',
        type='primary',
        use_container_width=True
    )
elif st.session_state.protocol_error:
    st.error(f"Protocol processing error: {st.session_state.protocol_error}")

# Download section - CRF (appears as soon as crf is ready)
if st.session_state.crf_ready:
    st.subheader("CRF Results Ready")
    st.download_button(
        "📥 Download CRF Data",
        data=st.session_state.crf_df.to_csv(index=False).encode('utf-8'),
        file_name='crf_extraction.csv',
        mime='text/csv',
        type='primary',
        use_container_width=True
    )
elif st.session_state.crf_error:
    st.error(f"CRF processing error: {st.session_state.crf_error}")

# Reset button (only show if any processing was done)
if st.session_state.protocol_ready or st.session_state.crf_ready or st.session_state.protocol_error or st.session_state.crf_error:
    if st.button('🔄 Process New Documents', use_container_width=True):
        st.session_state.protocol_df = None
        st.session_state.crf_df = None
        st.session_state.protocol_ready = False
        st.session_state.crf_ready = False
        st.session_state.protocol_error = None
        st.session_state.crf_error = None
        
        # Clean up any temp files
        for f in ["temp_crf.docx", "temp_protocol.pdf", "Schedule_of_Activities.pdf"]:
            if os.path.exists(f):
                os.remove(f)
        st.rerun()
