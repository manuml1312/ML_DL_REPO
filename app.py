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

def table_ai(combined_data):
    combined_data2 = [{'data':combined_data.to_json(orient='records')}]
    user_prompt_pr = f"""INPUT JSON: {combined_data2}
    Clean and return the structured JSON """
    
    messages_new = [
        {'role': 'system', 'content': system_prompt_pr},
        {'role': 'user', 'content': user_prompt_pr}
    ]
    
    try:
        response = client.chat.completions.create(
            model="o4-mini",  
            messages=messages_new,
            response_format={"type": "json_object"},
        )
        st.write(response.choices[0].message.content)
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
system_prompt_pr = """You are a clinical trial data structuring specialist. Clean and restructure the provided Schedule of Activities table JSON.

INPUT: A messy JSON where:
- Multiple visit codes may be packed into single cells (e.g., "V2D-2\nV2D-1 V2D1")
- Phase names might contain merged visits (e.g., "V16 V17 V18 V19 V20 V21 V22 V23")
- Timing and window values may be in wrong positions
- Null/None values should be replaced with empty strings
- Headers are incomplete - row 0 contains parent headers that span multiple columns, but only the first column of each group has the header text

REQUIRED TRANSFORMATIONS:

1. **Reconstruct Split Headers**
   - Row 0 contains parent headers that apply to multiple columns beneath them
   - When you see a format change in visit codes (e.g., V1, V2D-1, V2D1, SxD1, V14), this indicates a new column group starting
   - The parent header from row 0 should be propagated to ALL columns in that group
   - Add subscripts (_1, _2, _3, etc.) to distinguish columns under the same parent header
   - Example: If "Screening Phase" appears in column 2, and columns 2-5 all have visit data before format changes to a new phase, then columns 2-5 should be "Screening Phase_1", "Screening Phase_2", etc.

2. **Split Merged Visit Columns**
   - When a cell contains multiple visits separated by spaces or newlines (e.g., "V2D-2\nV2D-1 V2D1" or "V16 V17 V18")
   - Create separate columns for EACH visit
   - Distribute timing, window, and X-mark data appropriately across the new columns
   - Visit code format changes indicate phase boundaries

3. **Propagate Phase Names**
   - When column "0" is null/empty, fill with the phase name from the most recent non-null row above
   - Examples: "Randomisation (V2) In-house visit", "Treatment Maintenance period Ambulatory visit"

4. **Clean Text**
   - Remove all "\n" characters from text
   - Fix spacing issues (e.g., "Withdraw al" → "Withdrawal")
   - Fix spelling mistakes
   - Keep protocol section references intact (e.g., "10.1.3", "8.1", "5.1, 5.2")
   - Replace None/null values with empty strings ""

5. **Align Timing Data**
   - Ensure "Timing of Visit (Days)" values align with their respective visit columns
   - Ensure "Visit Window (Days)" values (±2, ±3, +3) align correctly
   - Ensure "Timing of Visit (Weeks)" values align correctly

6. **Preserve Structure**
   - Maintain row order exactly as provided
   - Keep header rows (rows 0-1) at top
   - Keep all procedure rows in original sequence
   - Preserve all "X" marks in their correct positions

OUTPUT FORMAT:
Return a clean JSON object with the same structure as input, but with:
- Each visit in its own numbered column key
- Sometimes the starting rows might not contain the headers, it means that they are the continued part of the last table. So, do not reorder the rows,keep them as is for all.
- Parent headers from row 0 propagated with subscripts (_1, _2, _3) to all columns in that phase group
- Phase names repeated where nulls existed
- All text cleaned and properly formatted
- Timing/window values correctly aligned
- All X marks preserved in correct positions
- None/null replaced with ""

CRITICAL: 
- Visit code format changes (V1 → V2D-1 → SxD1 → V14) indicate new phase groups for header propagation
- When splitting merged visits, ensure X marks stay with the correct visit
- Timing values must match the correct visit
- The total number of columns increases to accommodate all individual visits

Return ONLY the cleaned JSON object, no explanations."""


def extract_table_pages(pdf_file):
    """Extract pages containing Schedule of Activities tables"""
    pdf_document = fitz.open(pdf_file)
    page_texts = []

    # Patterns to find headings
    schedule_pattern = re.compile(r"schedule of activities|Schedule of activities|Schedule of Activities", re.IGNORECASE)
    intro_pattern = re.compile(r"Introduction")
    
    # Find start page
    schedule_start_page = None
    intro_start_page = None
    
    # Extract text from all pages
    with pdfplumber.open(pdf_file) as pdf:
        for i in range(len(pdf.pages)):
            page = pdf.pages[i]
            try:
                text = str(page.extract_text()) 
                # st.write(text)
            except Exception as e:
                text = str(page.extract_text_lines())
                # st.write(text)
            page_texts.append(text)
    
    schedule_pattern = re.compile(r"schedule of activities|Schedule of Activities|Schedule of activities|Schedule Of Activities", re.IGNORECASE)
    intro_pattern = re.compile(r"Introduction", re.IGNORECASE)
    
    schedule_start_page = None
    intro_start_page = None
    
    for i in range(1, len(page_texts)):
        text = page_texts[i]
        if schedule_pattern.search(text):
            schedule_start_page = i + 1
        if intro_pattern.search(text):
            intro_start_page = i + 1
        if schedule_start_page and intro_start_page:
            st.write("Start:",schedule_start_page," End:",intro_start_page)
            break
    
    if not schedule_start_page:
        pdf_document.close()
        return None
    else:
        end_page = intro_start_page if intro_start_page else len(pdf.pages)
    
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
            "vertical_strategy": "lines", "horizontal_strategy": "lines","explicit_vertical_lines": [],
    "explicit_horizontal_lines": [],"snap_tolerance": 300,
    "snap_x_tolerance": 6,
    "snap_y_tolerance": 5.16,
    "join_tolerance": 1,
    "join_x_tolerance": 2,
    "join_y_tolerance": 25,
    "edge_min_length": 25,
    "min_words_vertical": 3,
    "min_words_horizontal": 1,
    "intersection_tolerance": 1,
    "intersection_x_tolerance": 1,
    "intersection_y_tolerance": 5,
    "text_tolerance": 3,
    "text_x_tolerance": 5,
    "text_y_tolerance": 3,
        # "vertical_strategy": "lines",
        # "horizontal_strategy": "lines",
        # "explicit_vertical_lines": [],
        # "explicit_horizontal_lines": [],
        # "snap_tolerance": 300,
        # "snap_x_tolerance": 6,
        # "snap_y_tolerance": 4 , #5.16,
        # "join_tolerance": 1,
        # "join_x_tolerance": 2,
        # "join_y_tolerance": 3,
        # "edge_min_length": 100,
        # "min_words_vertical": 3,
        # "min_words_horizontal": 1,
        # "intersection_tolerance": 1,
        # "intersection_x_tolerance": 0.4,
        # "intersection_y_tolerance": 2,
        # "text_tolerance": 3,
        # "text_x_tolerance": 5,
        # "text_y_tolerance": 3,
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
                    
                    combined_data = combine_rows(raw_data)
                    st.write(combined_data)
                            
                if not combined_data.empty:
                    # nd = table_ai(combined_data)
                    # st.write('Post processed with AI')
                    # st.write(nd)
                    df = pd.concat((df,combined_data)) 
                    # df_ai = pd.concat((df_ai,nd))   
                # Update progress
                progress_percentage = (i + 1) / len(pdf.pages)
                my_bar.progress(
                    progress_percentage,
                    text=f"Extracting tables from Protocol REF PDF (page {i+1}/{len(pdf.pages)})..."
                )
            
            my_bar.empty()
            all_extracted_data = df
            if not all_extracted_data.empty:
                # Convert to DataFrame
                # pr_df = pd.DataFrame(all_extracted_data)
                pr_df = df.copy()
                
                if not pr_df.empty:
                    st.write(pr_df)
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
                        st.dataframe(protocol_df)
                    except Exception as e:
                        dup1 = protocol_df.copy()
                        dup1.columns = [f"{c}_{i}" for i,c in enumerate(dup1.columns)]
                        st.write("Table with and without ai postprocessing")
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
                st.session_state.crf_df = crf_df
                st.session_state.crf_ready = True
                
                try:
                    st.dataframe(crf_df)
                except Exception as e:
                    dup2 = crf_df.copy()
                    dup2.columns = [f"{c}_{i}" for i,c in enumerate(dup2.columns)]
                    st.dataframe(dup2)
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
# # Initialize session state
# if 'protocol_df' not in st.session_state:
#     st.session_state.protocol_df = None
# if 'crf_df' not in st.session_state:
#     st.session_state.crf_df = None
# if 'processing_done' not in st.session_state:
#     st.session_state.processing_done = False

# # --- Streamlit App Layout ---
# st.title("Clinical Document Processor")
# st.write("Upload your Mock CRF (.docx) and Protocol REF (.pdf) documents to extract and process data.")

# # File Uploaders
# uploaded_crf_file = st.file_uploader("Upload Mock CRF (.docx)", type="docx")
# uploaded_protocol_file = st.file_uploader("Upload Protocol REF (.pdf)", type="pdf")

# # Process Button
# if st.button("Process Documents", type="primary"):
#     if uploaded_crf_file and uploaded_protocol_file:
        
#         # Save uploaded files temporarily
#         crf_path = "temp_crf.docx"
#         protocol_path = "temp_protocol.pdf"
        
#         with open(crf_path, "wb") as f:
#             f.write(uploaded_crf_file.getbuffer())
#         with open(protocol_path, "wb") as f:
#             f.write(uploaded_protocol_file.getbuffer())
        
#         st.success("Files uploaded successfully")
        
#         # Process Protocol
#         st.subheader("Protocol REF Processing")
        
#         with st.spinner("Identifying the required tables..."):
#             extracted_pdf_path = extract_table_pages(protocol_path)
        
#         if extracted_pdf_path:
#             protocol_progress = st.empty()
#             protocol_df = process_protocol_pdf_pdfplumber(
#                 extracted_pdf_path, 
#                 system_prompt_pr
#             )
#             protocol_progress.empty()
            
#             if not protocol_df.empty:
#                 st.success(f"Extracted {len(protocol_df)} rows")
#                 st.session_state.protocol_df = protocol_df
#                 try:
#                     st.dataframe(protocol_df)
#                 except Exception as e:
#                     dup1 = protocol_df.copy()
#                     dup1.columns = [f"{c}_{i}" for i,c in enumerate(dup1.columns)]
#                     st.write("Table with and without ai postprocessing")
#                     st.dataframe(dup1)
#             else:
#                 st.warning("No Protocol data extracted")
#         else:
#             st.error("Could not identify Schedule of Activities pages")
            
#         # Process Mock CRF
#         st.subheader("Mock CRF Processing")
#         with st.spinner("Chunking document..."):
#             crf_chunks = process_crf_docx(crf_path)
#             crf_df = ai_extract(crf_chunks, System_prompt)
#             st.info(f"Created {len(crf_chunks)} chunks")
        
#         if not crf_df.empty:
#             st.success(f"Extracted {crf_df.shape[0]} items")
#             st.session_state.crf_df = crf_df
#             try:
#                 st.dataframe(crf_df)
#             except Exception as e:
#                 dup2 = crf_df.copy()
#                 dup2.columns = [f"{c}_{i}" for i,c in enumerate(dup2.columns)]
#                 st.dataframe(dup2)
        
#         # Mark processing as done
#         st.session_state.processing_done = True
        
#         # Cleanup
#         for f in [crf_path, protocol_path]:
#             if os.path.exists(f):
#                 os.remove(f)
#         if extracted_pdf_path and os.path.exists(extracted_pdf_path):
#             os.remove(extracted_pdf_path)
        
#         st.success("Processing complete!")

# # Download section (persists across reruns)
# if st.session_state.processing_done:
#     st.subheader("Download Results")
    
#     col1, col2, col3 = st.columns(3)
    
#     with col1:
#         if st.session_state.protocol_df is not None:
#             st.download_button(
#                 "Download Protocol Data",
#                 data=st.session_state.protocol_df.to_csv(index=False).encode('utf-8'),
#                 file_name='protocol_extraction.csv',
#                 mime='text/csv',
#                 type='primary'
#             )
    
#     with col2:
#         if st.session_state.crf_df is not None:
#             st.download_button(
#                 "Download CRF Data",
#                 data=st.session_state.crf_df.to_csv(index=False).encode('utf-8'),
#                 file_name='crf_extraction.csv',
#                 mime='text/csv',
#                 type='primary'
#             )
    
#     with col3:
#         if st.button('Process New Documents'):
#             st.session_state.protocol_df = None
#             st.session_state.crf_df = None
#             st.session_state.processing_done = False
#             # Clean up any temp files
#             for f in ["temp_crf.docx", "temp_protocol.pdf", "Schedule_of_Activities.pdf"]:
#                 if os.path.exists(f):
#                     os.remove(f)
#             st.rerun()
# --- Streamlit App Layout ---

# st.title("Clinical Document Processor")

# st.write("Upload your Mock CRF (.docx) and Protocol REF (.pdf) documents to extract and process data.")

# # File Uploaders
# uploaded_crf_file = st.file_uploader("Upload Mock CRF (.docx)", type="docx")
# uploaded_protocol_file = st.file_uploader("Upload Protocol REF (.pdf)", type="pdf")

# # st.info("Document with other Content: Used to extract tables from a document combined with other text and tables.")
# # st.info("Document with only Tables: Use when there are only tables in the document and no other pages.")

# # option = st.selectbox(
# #     "Select the one with respect to the Protocol Document",
# #     ("Document with other Content","Document with only Tables"),
# # )
# # Process Button
# # In your main Streamlit code, replace the protocol processing section:
# _em = st.empty()
# if st.button("Process Documents", type="primary"):
#     if uploaded_crf_file and uploaded_protocol_file:
        
#         # Save uploaded files temporarily
#         crf_path = "temp_crf.docx"
#         protocol_path = "temp_protocol.pdf"
        
#         with open(crf_path, "wb") as f:
#             f.write(uploaded_crf_file.getbuffer())
#         with open(protocol_path, "wb") as f:
#             f.write(uploaded_protocol_file.getbuffer())
        
#         st.success("Files uploaded successfully")
        
#         # Process Protocol - CORRECTED FLOW
#         st.subheader("Protocol REF Processing")
        
#         # Step 1: Extract table pages
#         with st.spinner("Identifying the required tables..."):
#             # Pass both the path AND the file object to extract_table_pages
#             # if option=='Document with other Content':
#             extracted_pdf_path = extract_table_pages(protocol_path)
#             # else:
#             # extracted_pdf_path = protocol_path
        
#         if extracted_pdf_path:
#             # Step 2: Process the extracted tables
#             protocol_progress = st.empty()
#             protocol_df = process_protocol_pdf_pdfplumber(
#                 extracted_pdf_path, 
#                 system_prompt_pr
#             )
#             protocol_progress.empty()
            
#             if not protocol_df.empty:
#                 st.success(f"Extracted {len(protocol_df)} rows")
#                 try:
#                     st.dataframe(protocol_df)
#                 except Exception as e:
#                     dup1 = protocol_df.copy()
#                     dup1.columns = [f"{c}_{i}" for i,c in enumerate(dup1.columns)]
#                     st.write("Table with and without ai postprocessing")
#                     # table_ai(dup1)
#                     st.dataframe(dup1)
                    
#                 st.download_button(
#                     "Download Protocol Data",
#                     data=protocol_df.to_csv(index=False).encode('utf-8'),
#                     file_name='protocol_extraction.csv',
#                     mime='text/csv',
#                     type = 'primary'
#                 )
#             else:
#                 st.warning("No Protocol data extracted")
            
#             # if protocol_errors:
#             #     with st.expander("View Errors"):
#             #         for error in protocol_errors:
#             #             st.error(error)
#         else:
#             st.error("Could not identify Schedule of Activities pages")
            
#         # Process Mock CRF
#         st.subheader("Mock CRF Processing")
#         with st.spinner("Chunking document..."):
#             chunker = DOCXCRFChunker(max_chunk_size=15000, overlap_size=500)
#             crf_chunks = process_crf_docx(crf_path)
#             crf_df = ai_extract(crf_chunks,System_prompt)
#             st.info(f"Created {len(crf_chunks)} chunks")
        
#         if not crf_df.empty:
#             st.success(f"Extracted {crf_df.shape[0]} items")
#             try:
#                 st.dataframe(crf_df)
#             except Exception as e:
#                 dup2 = crf_df.copy()
#                 dup2.columns = [f"{c}_{i}" for i,c in enumerate(dup2.columns)]
#                 st.dataframe(dup2)
                
#             st.download_button(
#                 "Download CRF Data",
#                 data=crf_df.to_csv(index=False).encode('utf-8'),
#                 file_name='crf_extraction.csv',
#                 mime='text/csv',
#                 type = 'primary'
#             )
            
#         # Cleanup
#         for f in [crf_path, protocol_path]:
#             if os.path.exists(f):
#                 os.remove(f)
#         if extracted_pdf_path and os.path.exists(extracted_pdf_path):
#             os.remove(extracted_pdf_path)
        
#         st.success("Processing complete!")
#                 # Clean up temporary files
#         if st.button('Clear Files History'):
#             if os.path.exists(crf_path):
#                 os.remove(crf_path)
#             if os.path.exists(protocol_path):
#                  os.remove(protocol_path)
#             if os.path.exists("Schedule_of_Activities.pdf"):
#                  os.remove("Schedule_of_Activities.pdf")
############################################################# old session states
#         st.success("Processing complete.")
# if st.button("Process Documents"):
#     if uploaded_crf_file is not None or uploaded_protocol_file is not None:
#         st.info("Processing documents...")

#         # Save uploaded files temporarily
#         crf_filename = "uploaded_crf.docx"
#         protocol_filename = "uploaded_protocol.pdf"

#         st.subheader("Protocol REF Processing Results (Table Extraction)")
#         # Process Protocol REF using pdfplumber
#         if option=="Document with other Content":
#             extracted_pdf = extract_table_pages(protocol_filename,uploaded_protocol_file)
#             if extracted_pdf:
#                 file=''
#                 protocol_df = process_protocol_pdf_pdfplumber(extracted_pdf,system_prompt_pr)
#                 st.write(protocol_df)
#         else:
#             protocol_df = process_protocol_pdf_pdfplumber(uploaded_protocol_file,system_prompt_pr)
#             st.write(protocol_df)

#         if protocol_df:
#             st.write("Extracted and Cleaned Table Data from Protocol REF:")
#             try:
#                 st.dataframe(protocol_df)
#             except Exception as e:
#                 dup=protocol_df.copy()
#                 dup.columns = [f"{c}_{i}" for i, c in enumerate(dup.columns)]
#                 st.dataframe(dup)
#         # else:
#         #     st.write("Output Not Available")


#             # Provide download link for Protocol data
#             @st.cache_data
#             def convert_df_to_excel(df):
#                 return df.to_csv(index=False)

#             protocol_excel_data = convert_df_to_excel(protocol_df)
#             st.download_button(
#                 label="Download Protocol REF Table Data as Excel",
#                 data=protocol_excel_data,
#                 file_name='protocol_ref_table_data.csv',
#                 mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
#             )
#         else:
#             st.warning("No tables extracted or processed successfully from the Protocol REF.")

        
#         with open(crf_filename, "wb") as f:
#             f.write(uploaded_crf_file.getbuffer())

#         with open(protocol_filename, "wb") as f:
#             f.write(uploaded_protocol_file.getbuffer())

#         st.success("Files uploaded successfully.")

#         # Process Mock CRF
#         st.subheader("Mock CRF Processing Results")
#         crf_chunks = process_crf_docx(crf_filename)

#         if crf_chunks:
#             crf_extraction_df = ai_extract(crf_chunks, System_prompt)
#             if crf_extraction_df:
#                 st.write("Extracted CRF Data:")
#                 st.dataframe(crf_extraction_df)

#                 # Provide download link for CRF data
#                 @st.cache_data
#                 def convert_df_to_excel(df):
#                     return df.to_csv(index=False)

#                 crf_excel_data = convert_df_to_excel(crf_extraction_df)
#                 st.download_button(
#                     label="Download Extracted CRF Data as Excel",
#                     data=crf_excel_data,
#                     file_name='extracted_crf_data.csv',
#                     mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
#                 )
#         else:
#              st.warning("No chunks generated from the Mock CRF.")

#         # Clean up temporary files
#         if st.button('Clear Files History'):
#             if os.path.exists(crf_filename):
#                 os.remove(crf_filename)
#             if os.path.exists(protocol_filename):
#                  os.remove(protocol_filename)
#             if os.path.exists("Schedule_of_Activities.pdf"):
#                  os.remove("Schedule_of_Activities.pdf")

#         st.success("Processing complete.")

#     else:
#         st.warning("Please upload both Mock CRF and Protocol REF documents to start processing.")
