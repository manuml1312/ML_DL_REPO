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


# Ensure you have your OpenAI API key set up as an environment variable or use Streamlit secrets
api_key = st.secrets["api_key"]
# For this example, I'll use the key you provided, but using Streamlit secrets is recommended for deployment
client = OpenAI(api_key=api_key)


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

    def extract_and_chunk(self, docx_path: str) -> List[Dict[str, Any]]:
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
def process_crf_docx(docx_path: str) -> List[Dict[str, Any]]:
    """Process a CRF DOCX file and return chunks"""

    chunker = DOCXCRFChunker(max_chunk_size=15000, overlap_size=500)
    chunks = chunker.extract_and_chunk(docx_path)

    # Print summary (optional, can be displayed in Streamlit)
    summary = chunker.get_chunk_summary(chunks)
    st.write(f"Created {summary['total_chunks']} chunks from Mock CRF.")
    st.write(f"Forms identified: {summary['forms_identified']}")

    return chunks


# Function to process the Protocol REF PDF and extract table data using pdfplumber
system_prompt_pr = """You are a clinical trial data structuring specialist. Clean and restructure the provided Schedule of Activities table JSON.

INPUT: A messy JSON where:
- Multiple visit codes are packed into single cells (e.g., "V2D-2\nV2D-1 V2D1")
- Phase names contain merged visits (e.g., "V16 V17 V18 V19 V20 V21 V22 V23")
- Timing and window values may be in wrong positions
- Null values where phase names should repeat

REQUIRED TRANSFORMATIONS:

1. **Split Merged Visit Columns**
   - When a cell contains multiple visits separated by spaces or newlines (e.g., "V2D-2\nV2D-1 V2D1" or "V16 V17 V18")
   - Create separate columns for EACH visit
   - Distribute timing, window, and X-mark data appropriately across the new columns

2. **Propagate Phase Names**
   - When column "0" is null, fill with the phase name from the most recent non-null row above
   - Examples: "Randomisation (V2) In-house visit", "Treatment Maintenance period Ambulatory visit"

3. **Clean Text**
   - Remove all "\n" characters from text
   - Fix spacing issues (e.g., "Withdraw al" → "Withdrawal")
   - Keep protocol section references intact (e.g., "10.1.3", "8.1", "5.1, 5.2")

4. **Align Timing Data**
   - Ensure "Timing of Visit (Days)" values align with their respective visit columns
   - Ensure "Visit Window (Days)" values (±2, ±3, +3) align correctly
   - Ensure "Timing of Visit (Weeks)" values align correctly

5. **Preserve Structure**
   - Maintain row order exactly as provided
   - Keep header rows (rows 0-1) at top
   - Keep all procedure rows in original sequence
   - Preserve all "X" marks in their correct positions

OUTPUT FORMAT:
Return a clean JSON object with the same structure as input, but with:
- Each visit in its own numbered column key
- Phase names repeated where nulls existed
- All text cleaned and properly formatted
- Timing/window values correctly aligned
- All X marks preserved in correct positions

CRITICAL: When splitting merged visits, ensure that:
- X marks stay with the correct visit
- Timing values match the correct visit
- The total number of columns increases to accommodate all individual visits

Return ONLY the cleaned JSON object, no explanations."""


def process_protocol_pdf_pdfplumber(pdf_path: str,system_prompt_pr: str) -> pd.DataFrame:
    """
    Processes the Protocol REF PDF to extract tables using pdfplumber and cleans data with API.
    """
    pdf_document = fitz.open(pdf_path)

    with pdfplumber.open(path_pdf) as pdf:
      for i in range(len(pdf.pages)):
        page = pdf.pages[i]
        text=page.extract_text()
        page_texts.append(text)
          
        
    
    # Regex to find the headings, looking for the exact phrases
    schedule_pattern = re.compile(r"schedule of activities|Schedule of activities", re.IGNORECASE)
    intro_pattern = re.compile(r"introduction", re.IGNORECASE)
    
    # Start searching from page 2 (index 1) to skip initial sections
    start_search_index = 1
    
    schedule_start_page = None
    intro_start_page = None
    
    for i in range(start_search_index, len(page_texts)):
        text = page_texts[i]
        if schedule_start_page is None:
            if schedule_pattern.search(text):
              schedule_start_page = i + 1
        if intro_start_page is None:
            if intro_pattern.search(text):
              intro_start_page = i + 1
    
    # Determine the range of pages for the Schedule of Activities section
    start_page = schedule_start_page
    if end_page is not None:
      end_page = len(pdf.pages)
    
    output_pdf = fitz.open()
    extracted_pdf_path = None # Initialize extracted_pdf_path
    
    if start_page and end_page:
        st.write(f"Found 'Schedule of Activities' starting on page: {start_page}")
        st.write(f"Will extract pages from {start_page} up to page {end_page} or until tables stop.")
    
        # Extract potential schedule pages into a temporary PDF
        temp_schedule_pdf = fitz.open()
    
        temp_schedule_pdf.insert_pdf(pdf_document, from_page=start_page-1, to_page=end_page+1)
    
        if temp_schedule_pdf.page_count > 0:
            extracted_pdf_path = "Schedule_of_Activities.pdf"
            temp_schedule_pdf.save(extracted_pdf_path)
            temp_schedule_pdf.close()
            st.write(f"Saved potential schedule section to {extracted_pdf_path}")
        else:
            st.warning("Could not extract any pages for the Schedule of Activities section based on the identified range.")
            pdf_document.close()
            return pd.DataFrame()
    else:
        st.warning("Could not find the 'Schedule of Activities' section heading.")
        pdf_document.close()
        return pd.DataFrame()
    
    pdf_document.close()
    
  # Use Camelot to read tables from the extracted PDF
    all_extracted_data = []

    table_settings = {
        "vertical_strategy": "lines", "horizontal_strategy": "lines","explicit_vertical_lines": [],
        "explicit_horizontal_lines": [],"snap_tolerance": 300,
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

    if extracted_pdf_path:
        st.write(f"Processing extracted PDF: {extracted_pdf_path}")
        try:
            with pdfplumber.open(extracted_pdf_path) as pdf:
                st.write(f"Opened PDF with {len(pdf.pages)} pages for table extraction.")

                progress_text = "Extracting tables from Protocol REF PDF..."
                my_bar = st.progress(0, text=progress_text)

                for i in range(len(pdf.pages)):
                    page = pdf.pages[i]
                    st.write(f"Processing page {i+1}...")

                    # Extract table using the specified settings
                    tables_on_page = page.extract_tables(table_settings=table_settings)

                    if tables_on_page:
                        st.write(f"Found {len(tables_on_page)} tables on page {i+1}.")
                        for table_data in tables_on_page:
                            # Convert extracted table data to a list of lists
                            raw_data = [list(row) for row in table_data]

                            # Convert to JSON string for API input
                            raw_json = json.dumps({"data": raw_data})

                            user_prompt_pr = f"""INPUT JSON: {raw_json}

                            Clean and return the structured JSON."""

                            messages_new = [{'role':'system','content':system_prompt_pr}]
                            messages_new.append({'role':'user','content':user_prompt_pr})

                            try:
                                response = client.chat.completions.create(
                                    model="o4-mini",
                                    messages=messages_new,
                                    response_format={"type": "json_object"}
                                )
                                cleaned_data_json = json.loads(response.choices[0].message.content)

                                if 'data' in cleaned_data_json and cleaned_data_json['data']:
                                    # Extend the main list with rows from this table
                                    all_extracted_data.extend(cleaned_data_json['data'])
                                else:
                                    st.warning(f"API returned empty or invalid data for a table on page {i+1}.")

                            except Exception as api_e:
                                st.error(f"API error cleaning table data from page {i+1}: {api_e}")

                    # Update progress bar
                    progress_percentage = (i + 1) / len(pdf.pages)
                    my_bar.progress(progress_percentage, text=f"Extracting tables from Protocol REF PDF (page {i+1}/{len(pdf.pages)})...")
                my_bar.empty() # Clear progress bar on completion

                if all_extracted_data:
                    # Convert the list of lists into a DataFrame
                    pr_df = pd.DataFrame(all_extracted_data)

                    # Assuming the first row is the header and setting it
                    if not pr_df.empty:
                        pr_df.columns = pr_df.iloc[0]
                        pr_df = pr_df[1:].reset_index(drop=True)

                    # Drop columns that are entirely NaN after processing
                    pr_df = pr_df.dropna(axis=1, how='all')
                    return pr_df
                else:
                    st.warning("No tables found or extracted successfully from the Protocol REF PDF using pdfplumber.")
                    return pd.DataFrame()

        except Exception as e:
            st.error(f"Error processing Protocol REF PDF with pdfplumber: {e}")
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


# --- Streamlit App Layout ---

st.title("Clinical Document Processor")

st.write("Upload your Mock CRF (.docx) and Protocol REF (.pdf) documents to extract and process data.")

# File Uploaders
uploaded_crf_file = st.file_uploader("Upload Mock CRF (.docx)", type="docx")
uploaded_protocol_file = st.file_uploader("Upload Protocol REF (.pdf)", type="pdf")

option = st.selectbox(
    "Select the one with respect to the Protocol Document",
    ("Document with other Content","Document with only Tables"),
)

# Process Button
if st.button("Process Documents"):
    if uploaded_crf_file is not None and uploaded_protocol_file is not None:
        st.info("Processing documents...")

        # Save uploaded files temporarily
        crf_filename = "uploaded_crf.docx"
        protocol_filename = "uploaded_protocol.pdf"

        with open(crf_filename, "wb") as f:
            f.write(uploaded_crf_file.getbuffer())

        with open(protocol_filename, "wb") as f:
            f.write(uploaded_protocol_file.getbuffer())

        st.success("Files uploaded successfully.")

        # Process Mock CRF
        st.subheader("Mock CRF Processing Results")
        crf_chunks = process_crf_docx(crf_filename)

        if crf_chunks:
            crf_extraction_df = ai_extract(crf_chunks, System_prompt)
            if not crf_extraction_df.empty:
                st.write("Extracted CRF Data:")
                st.dataframe(crf_extraction_df)

                # Provide download link for CRF data
                @st.cache_data
                def convert_df_to_excel(df):
                    return df.to_csv(index=False)

                crf_excel_data = convert_df_to_excel(crf_extraction_df)
                st.download_button(
                    label="Download Extracted CRF Data as Excel",
                    data=crf_excel_data,
                    file_name='extracted_crf_data.csv',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
        else:
             st.warning("No chunks generated from the Mock CRF.")


        st.subheader("Protocol REF Processing Results (Table Extraction)")
        # Process Protocol REF using pdfplumber
        protocol_df = process_protocol_pdf_pdfplumber(protocol_filename,system_prompt_pr)

        if not protocol_df.empty:
            st.write("Extracted and Cleaned Table Data from Protocol REF:")
            try:
                st.dataframe(protocol_df)
            except Exception as e:
                dup=protocol_df.copy()
                dup.columns = [f"{c}_{i}" for i, c in enumerate(dup.columns)]
                st.dataframe(dup)


            # Provide download link for Protocol data
            @st.cache_data
            def convert_df_to_excel(df):
                return df.to_csv(index=False)

            protocol_excel_data = convert_df_to_excel(protocol_df.fillna('',inplace=True))
            st.download_button(
                label="Download Protocol REF Table Data as Excel",
                data=protocol_excel_data,
                file_name='protocol_ref_table_data.csv',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        else:
            st.warning("No tables extracted or processed successfully from the Protocol REF.")

        # Clean up temporary files
        if os.path.exists(crf_filename):
            os.remove(crf_filename)
        if os.path.exists(protocol_filename):
             os.remove(protocol_filename)
        if os.path.exists("Schedule_of_Activities.pdf"):
             os.remove("Schedule_of_Activities.pdf")

        st.success("Processing complete.")

    else:
        st.warning("Please upload both Mock CRF and Protocol REF documents to start processing.")
