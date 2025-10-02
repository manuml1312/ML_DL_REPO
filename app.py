import streamlit as st
import pandas as pd
from docx import Document
from typing import List, Dict, Any
from openai import OpenAI
import json
import os
import pdfplumber
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue

# API setup
api_key = st.secrets["api_key"]
client = OpenAI(api_key=api_key)


class DOCXCRFChunker:
def __init__(self, total_items, description):
        self.lock = threading.Lock()
        self.completed = 0
        self.total = total_items
        self.progress_bar = st.progress(0, text=f"{description} (0/{total_items})")
    
    def update(self, increment=1):
        with self.lock:
            self.completed += increment
            progress = self.completed / self.total
            self.progress_bar.progress(
                progress, 
                text=f"Processing ({self.completed}/{self.total})"
            )
    
    def complete(self):
        self.progress_bar.empty()


class DOCXCRFChunker:
    def __init__(self, max_chunk_size: int = 15000, overlap_size: int = 500):
        self.max_chunk_size = max_chunk_size
        self.overlap_size = overlap_size
        self.form_patterns = [
            r'sCRF\s+v\d+\.\d+',
            r'\[[\w_]+\]',
            r'V\d+(?:,\s*V\d+)*',
        ]

    def extract_and_chunk(self, docx_path: str) -> List[Dict[str, Any]]:
        doc = Document(docx_path)
        elements = self._extract_structured_content(doc)
        chunks = self._create_chunks(elements)
        return chunks

    def _extract_structured_content(self, doc: Document) -> List[Dict[str, Any]]:
        elements = []
        for element in doc.element.body:
            if element.tag.endswith('p'):
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
            elif element.tag.endswith('tbl'):
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
        for para in doc.paragraphs:
            if para._element == element:
                return para
        return None

    def _get_table_from_element(self, doc, element):
        for table in doc.tables:
            if table._element == element:
                return table
        return None

    def _is_heading(self, para) -> bool:
        if para.style:
            style_name = para.style.name.lower()
            return 'heading' in style_name or 'title' in style_name
        return False

    def _is_form_title(self, text: str) -> bool:
        for pattern in self.form_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        
        form_indicators = [
            'collection of samples', 'contraception', 'c-ssrs',
            'patient health questionnaire', 'weight history',
            'hand grip test', 'mri scan consent'
        ]
        text_lower = text.lower()
        return any(indicator in text_lower for indicator in form_indicators)

    def _extract_table_text(self, table) -> str:
        table_text = []
        for row in table.rows:
            row_text = [cell.text.strip() for cell in row.cells if cell.text.strip()]
            if row_text:
                table_text.append(" | ".join(row_text))
        return "\n".join(table_text)

    def _is_crf_table(self, table_text: str) -> bool:
        crf_indicators = [
            'yes', 'no', 'radio', 'checkbox', 'required', 'optional', '*',
            'integration', 'argus', 'cosmos', 'item label', 'data type', 'codelist'
        ]
        text_lower = table_text.lower()
        return sum(1 for indicator in crf_indicators if indicator in text_lower) >= 2

    def _create_chunks(self, elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        chunks = []
        current_chunk = self._create_new_chunk(1)

        for element in elements:
            if (element['type'] == 'paragraph' and element.get('is_form_title', False)):
                if current_chunk['length'] > 0:
                    chunks.append(self._finalize_chunk(current_chunk))
                    current_chunk = self._create_new_chunk(len(chunks) + 1)
                current_chunk['form_context'] = element['text']

            if (current_chunk['length'] + element['length'] > self.max_chunk_size and 
                current_chunk['length'] > 0):
                if element['type'] == 'table':
                    chunks.append(self._finalize_chunk(current_chunk))
                    current_chunk = self._create_new_chunk(len(chunks) + 1)
                    current_chunk['form_context'] = chunks[-1]['form_context'] if chunks else ''

            current_chunk['elements'].append(element)
            current_chunk['text'] += element['text'] + '\n\n'
            current_chunk['length'] += element['length']
            if element['type'] == 'table':
                current_chunk['has_tables'] = True

        if current_chunk['length'] > 0:
            chunks.append(self._finalize_chunk(current_chunk))

        return self._add_overlap(chunks)

    def _create_new_chunk(self, chunk_id: int) -> Dict[str, Any]:
        return {
            'chunk_id': chunk_id,
            'elements': [],
            'text': '',
            'length': 0,
            'form_context': '',
            'has_tables': False
        }

    def _finalize_chunk(self, chunk: Dict[str, Any]) -> Dict[str, Any]:
        if chunk['form_context'] and chunk['form_context'] not in chunk['text'][:200]:
            chunk['text'] = f"FORM CONTEXT: {chunk['form_context']}\n\n" + chunk['text']
        
        chunk['paragraph_count'] = sum(1 for el in chunk['elements'] if el['type'] == 'paragraph')
        chunk['table_count'] = sum(1 for el in chunk['elements'] if el['type'] == 'table')
        chunk['crf_table_count'] = sum(1 for el in chunk['elements'] 
                                      if el['type'] == 'table' and el.get('is_crf_table', False))
        return chunk

    def _add_overlap(self, chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if len(chunks) <= 1:
            return chunks
        
        for i in range(1, len(chunks)):
            prev_text = chunks[i-1]['text']
            overlap_text = prev_text[-self.overlap_size:] if len(prev_text) > self.overlap_size else prev_text
            chunks[i]['text'] = f"OVERLAP FROM PREVIOUS:\n{overlap_text}\n\nCURRENT CHUNK:\n" + chunks[i]['text']
            chunks[i]['has_overlap'] = True
        
        return chunks


# Optimized Prompts
SYSTEM_PROMPT_CRF = """You are a CRF extraction specialist. Extract structured information from clinical research documents and return ONLY valid JSON.

OUTPUT FORMAT:
{
  "forms": [{
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
  }],
  "chunk_metadata": {
    "chunk_id": "string",
    "forms_found": number,
    "items_extracted": number
  }
}

EXTRACTION RULES:
1. Form Label: Main form title (e.g., "Collection of Samples for Laboratory (Lab_1)")
2. Form Code: Code in brackets (e.g., "[LAB_SMPL_TKN]")
3. Visits: Visit schedule (e.g., "V1, V4, V5, V6")
4. Form Type: "Non-repeating form" or "Repeating form"
5. Item Group: Sections like "Blood", "Urine", "Contraception"
6. Item Group Repeating: "Y" if repeating, "N" otherwise
7. Item Order: Sequential number (1, 2, 3...)
8. Item Label: Exact question text
9. Item Name: Always "[SDTM Programmer]"
10. Data Type: Radio Button/Numeric/Text/Date/Checkbox
11. Codelist: Available options (e.g., "Yes/No")
12. Codelist Name: Logical name for options
13. Required: "Y" if asterisk (*) present, "N" otherwise

Return ONLY valid JSON, no explanations."""

SYSTEM_PROMPT_PROTOCOL = """You are a clinical trial data structuring specialist. Clean and restructure Schedule of Activities table JSON.

INPUT: Messy JSON with multiple visit codes packed into single cells, merged phase names, misaligned timing data.

REQUIRED TRANSFORMATIONS:
1. Split Merged Visit Columns - Create separate columns for each visit (e.g., "V2D-2\nV2D-1 V2D1" → 3 columns)
2. Propagate Phase Names - Fill null values with phase name from row above
3. Clean Text - Remove "\n", fix spacing, keep protocol section references
4. Align Timing Data - Ensure days/weeks/windows align with correct visits
5. Preserve Structure - Maintain row order, keep headers, preserve all "X" marks

OUTPUT: Clean JSON with each visit in own column, phase names filled, text cleaned, timing aligned, X marks preserved.

CRITICAL: When splitting visits, X marks and timing values must stay with correct visit.

Return ONLY cleaned JSON, no explanations."""


def process_crf_chunk(chunk, chunk_idx):
    """Process a single CRF chunk - no Streamlit calls"""
    try:
        user_prompt = f"""Extract CRF information from this document chunk and return JSON:

CHUNK CONTENT:
{chunk['text']}

Analyze and extract all CRF forms, item groups, and items following the specified JSON format."""

        response = client.chat.completions.create(
            model="o4-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT_CRF},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
        )
        
        answer = json.loads(response.choices[0].message.content)
        return {'success': True, 'data': answer.get('forms', []), 'chunk_idx': chunk_idx}
    
    except Exception as e:
        return {'success': False, 'error': str(e), 'chunk_idx': chunk_idx}


def process_protocol_table(page_idx, table_idx, table_data):
    """Process a single protocol table - no Streamlit calls"""
    try:
        raw_data = [list(row) for row in table_data]
        raw_json = json.dumps({"data": raw_data})
        
        user_prompt = f"""INPUT JSON: {raw_json}

Clean and return the structured JSON."""

        response = client.chat.completions.create(
            model="o4-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT_PROTOCOL},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},        )
        
        cleaned_data = json.loads(response.choices[0].message.content)
        return {'success': True, 'data': cleaned_data.get('data', []), 'page_idx': page_idx}
    
    except Exception as e:
        return {'success': False, 'error': str(e), 'page_idx': page_idx}


def process_crf_parallel(chunks, progress_placeholder):
    """Process CRF chunks in parallel"""
    all_forms_data = []
    errors = []
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(process_crf_chunk, chunk, i): i 
            for i, chunk in enumerate(chunks)
        }
        
        completed = 0
        for future in as_completed(futures):
            result = future.result()
            completed += 1
            
            # Update progress in main thread
            progress_placeholder.progress(
                completed / len(chunks),
                text=f"Processing CRF chunks ({completed}/{len(chunks)})"
            )
            
            if result['success']:
                all_forms_data.extend(result['data'])
            else:
                errors.append(f"Chunk {result['chunk_idx']}: {result['error']}")
    
    return pd.DataFrame(all_forms_data) if all_forms_data else pd.DataFrame(), errors


def process_protocol_parallel(pdf_path, progress_placeholder):
    """Process protocol PDF tables in parallel"""
    table_settings = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
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
    
    all_extracted_data = []
    errors = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # Collect all tables
            tables_to_process = []
            for page_idx, page in enumerate(pdf.pages):
                tables_on_page = page.extract_tables(table_settings=table_settings)
                if tables_on_page:
                    for table_idx, table_data in enumerate(tables_on_page):
                        tables_to_process.append((page_idx, table_idx, table_data))
            
            if not tables_to_process:
                return pd.DataFrame(), ["No tables found in PDF"]
            
            # Process in parallel
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(process_protocol_table, *args): i 
                    for i, args in enumerate(tables_to_process)
                }
                
                completed = 0
                for future in as_completed(futures):
                    result = future.result()
                    completed += 1
                    
                    # Update progress in main thread
                    progress_placeholder.progress(
                        completed / len(tables_to_process),
                        text=f"Processing Protocol tables ({completed}/{len(tables_to_process)})"
                    )
                    
                    if result['success']:
                        all_extracted_data.extend(result['data'])
                    else:
                        errors.append(f"Page {result['page_idx']}: {result['error']}")
            
            if all_extracted_data:
                pr_df = pd.DataFrame(all_extracted_data)
                
                if not pr_df.empty:
                    pr_df.columns = pr_df.iloc[0]
                    pr_df = pr_df[1:].reset_index(drop=True)
                
                new_cols = ['Common Forms', 'Unscheduled', 'Is Form Dynamic?', 
                           'Form Dynamic Criteria', 'Additional Programming Instructions']
                for col in new_cols:
                    if col not in pr_df.columns:
                        pr_df[col] = ''
                
                pr_df = pr_df.dropna(axis=1, how='all')
                return pr_df, errors
            
            return pd.DataFrame(), errors
    
    except Exception as e:
        return pd.DataFrame(), [f"PDF processing error: {str(e)}"]


# Streamlit App
st.set_page_config(page_title="Clinical Document Processor", layout="wide")
st.title("Clinical Document Processor")
st.write("Upload your Mock CRF (.docx) and Protocol REF (.pdf) to extract data.")

col1, col2 = st.columns(2)

with col1:
    uploaded_crf_file = st.file_uploader("Upload Mock CRF (.docx)", type="docx")

with col2:
    uploaded_protocol_file = st.file_uploader("Upload Protocol REF (.pdf)", type="pdf")

if st.button("Process Documents", type="primary"):
    if uploaded_crf_file and uploaded_protocol_file:
        
        # Save files
        crf_path = "temp_crf.docx"
        protocol_path = "temp_protocol.pdf"
        
        with open(crf_path, "wb") as f:
            f.write(uploaded_crf_file.getbuffer())
        with open(protocol_path, "wb") as f:
            f.write(uploaded_protocol_file.getbuffer())
        
        st.success("Files uploaded successfully")
        
        # Create columns for parallel display
        col_crf, col_protocol = st.columns(2)
        
        with col_crf:
            st.subheader("Mock CRF Processing")
            with st.spinner("Chunking document..."):
                chunker = DOCXCRFChunker()
                crf_chunks = chunker.extract_and_chunk(crf_path)
                st.info(f"Created {len(crf_chunks)} chunks")
        
        with col_protocol:
            st.subheader("Protocol REF Processing")
        
        # Create progress placeholders
        with col_crf:
            crf_progress = st.empty()
        with col_protocol:
            protocol_progress = st.empty()
        
        # Process both simultaneously using threads
        crf_result = [None, None]  # [dataframe, errors]
        protocol_result = [None, None]
        
        def process_crf_wrapper():
            df, errors = process_crf_parallel(crf_chunks, crf_progress)
            crf_result[0] = df
            crf_result[1] = errors
        
        def process_protocol_wrapper():
            df, errors = process_protocol_parallel(protocol_path, protocol_progress)
            protocol_result[0] = df
            protocol_result[1] = errors
        
        # Start both threads
        thread_crf = threading.Thread(target=process_crf_wrapper)
        thread_protocol = threading.Thread(target=process_protocol_wrapper)
        
        thread_crf.start()
        thread_protocol.start()
        
        # Wait for completion
        thread_crf.join()
        thread_protocol.join()
        
        # Clear progress bars
        crf_progress.empty()
        protocol_progress.empty()
        
        # Display results
        with col_crf:
            if crf_result[0] is not None and not crf_result[0].empty:
                df1=crf_result[0]
                df1.columns = [f"{c}_{i}" for i, c in enumerate(df1.columns)]
                st.success(f"Extracted {len(crf_result[0])} items")
                st.dataframe(crf_result[0], use_container_width=True)
                crf_result[0]=crf_result[0].replace(None,'')
                
                st.download_button(
                    "Download CRF Data",
                    data=crf_result[0].to_csv(index=False).encode('utf-8'),
                    file_name='crf_extraction.csv',
                    mime='text/csv'
                )
            else:
                st.warning("No CRF data extracted")
            
            if crf_result[1]:
                with st.expander("View Errors"):
                    for error in crf_result[1]:
                        st.error(error)
        
        with col_protocol:
            if protocol_result[0] is not None and not protocol_result[0].empty:
                df2 =  protocol_result[0]
                df2.columns = [f"{c}_{i}" for i, c in enumerate(df2.columns)]
                st.success(f"Extracted {len(protocol_result[0])} rows")
                st.dataframe(protocol_result[0], use_container_width=True)
                protocol_result[0]=protocol_result[0].replace(None,'')
                
                st.download_button(
                    "Download Protocol Data",
                    data=protocol_result[0].to_csv(index=False).encode('utf-8'),
                    file_name='protocol_extraction.csv',
                    mime='text/csv'
                )
            else:
                st.warning("No Protocol data extracted")
            
            if protocol_result[1]:
                with st.expander("View Errors"):
                    for error in protocol_result[1]:
                        st.error(error)
        
        # Cleanup
        for f in [crf_path, protocol_path]:
            if os.path.exists(f):
                os.remove(f)
        
        st.success("Processing complete!")
    
    else:
        st.error("Please upload both documents")
