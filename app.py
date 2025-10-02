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
    # [Keep all your existing DOCXCRFChunker code exactly as is]
    pass


# Optimized Prompts
SYSTEM_PROMPT_CRF = """[Your existing CRF prompt]"""
SYSTEM_PROMPT_PROTOCOL = """[Your existing Protocol prompt]"""


def process_crf_chunk(chunk, chunk_idx):
    """Process a single CRF chunk - no Streamlit calls"""
    try:
        user_prompt = f"""Extract CRF information from this document chunk and return JSON:

CHUNK CONTENT:
{chunk['text']}

Analyze and extract all CRF forms, item groups, and items following the specified JSON format."""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT_CRF},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.1
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
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT_PROTOCOL},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.1
        )
        
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
                chunker = DOCXCRFChunker(max_chunk_size=15000, overlap_size=500)
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
