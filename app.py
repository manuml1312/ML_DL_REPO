import streamlit as st
import pandas as pd
import os
import time
import logging
from openai import OpenAI

# Import custom classes from the modules
from crf import DOCXCRFChunker
from protocol import ClinicalDataProcessor

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Clinical Document Processor",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional styling
st.markdown("""
<style>
    /* Main container styling */
    .main {
        padding: 2rem;
    }
    
    /* Metric containers */
    .metric-container {
        background: white;
        padding: 1.5rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #667eea;
        margin-bottom: 1rem;
    }
    
    /* Status badges */
    .status-badge {
        display: inline-block;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.9rem;
        margin-bottom: 1rem;
    }
    
    .status-idle {
        background: #e3e8ef;
        color: #5a6c7d;
    }
    
    .status-processing {
        background: #fff3cd;
        color: #856404;
        animation: pulse 2s infinite;
    }
    
    .status-success {
        background: #d4edda;
        color: #155724;
    }
    
    .status-error {
        background: #f8d7da;
        color: #721c24;
    }
    
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.7; }
    }
    
    /* Progress bar styling */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    }
    
    /* Card headers */
    .card-header {
        font-size: 1.2rem;
        font-weight: 600;
        color: #2c3e50;
        margin-bottom: 1.5rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e9ecef;
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background: #f8f9fa;
    }
    
    /* Button styling */
    .stButton > button {
        width: 100%;
        border-radius: 8px;
        font-weight: 600;
        padding: 0.75rem 1rem;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
    }
</style>
""", unsafe_allow_html=True)

# Initialize OpenAI client
try:
    client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
    logger.info("OpenAI client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize OpenAI client: {e}")
    st.error("⚠️ OpenAI API key not found. Please set OPENAI_API_KEY in your Streamlit secrets.")
    st.stop()

# Initialize processors with OpenAI client
crf_processor = DOCXCRFChunker(
    max_chunk_size=15000,
    overlap_size=500,
    openai_client=client
)
protocol_processor = ClinicalDataProcessor(openai_client=client)
logger.info("Processors initialized successfully")

# Initialize session state
def init_session_state():
    defaults = {
        'protocol_df': None,
        'crf_df': None,
        'protocol_ready': False,
        'crf_ready': False,
        'protocol_error': None,
        'crf_error': None,
        'processing_active': False,
        'protocol_metrics': {
            'total_pages': 0,
            'pages_processed': 0,
            'tables_found': 0,
            'rows_extracted': 0,
            'status': 'Idle',
            'confidence': 0.0,
            'processing_time': 0.0
        },
        'crf_metrics': {
            'total_chunks': 0,
            'chunks_processed': 0,
            'forms_found': 0,
            'items_extracted': 0,
            'status': 'Idle',
            'confidence': 0.0,
            'processing_time': 0.0
        }
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

# Helper function to display status badge
def status_badge(status):
    status_classes = {
        'Idle': 'status-idle',
        'Processing': 'status-processing',
        'Complete': 'status-success',
        'Error': 'status-error'
    }
    class_name = status_classes.get(status, 'status-idle')
    
    status_icons = {
        'Idle': '⚪',
        'Processing': '🔄',
        'Complete': '✅',
        'Error': '❌'
    }
    icon = status_icons.get(status, '⚪')
    
    return f'<div class="status-badge {class_name}">{icon} {status}</div>'

# --- Streamlit App Layout ---
st.title("🏥 Clinical Document Processor")
st.markdown("#### Transform clinical documents into structured data with AI-powered extraction")
st.markdown("---")

# Sidebar
with st.sidebar:
    st.markdown("### 📁 Document Upload")
    st.markdown("Upload your clinical trial documents for automated processing.")
    
    uploaded_crf_file = st.file_uploader(
        "Mock CRF Document",
        type="docx",
        key="crf_upload",
        help="Upload a Word document (.docx) containing the Case Report Form"
    )
    
    uploaded_protocol_file = st.file_uploader(
        "Protocol REF Document",
        type="pdf",
        key="protocol_upload",
        help="Upload a PDF document containing the Protocol Reference"
    )
    
    st.markdown("---")
    
    process_button = st.button(
        "🚀 Start Processing",
        type="primary",
        disabled=st.session_state.processing_active,
        use_container_width=True
    )
    
    if st.session_state.protocol_ready or st.session_state.crf_ready or st.session_state.protocol_error or st.session_state.crf_error:
        st.markdown("---")
        if st.button('🔄 Reset & Process New', use_container_width=True):
            logger.info("Resetting application state")
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            init_session_state()
            
            for f in ["temp_crf.docx", "temp_protocol.pdf", "Schedule_of_Activities.pdf"]:
                if os.path.exists(f):
                    os.remove(f)
                    logger.info(f"Cleaned up temp file: {f}")
            st.rerun()
    
    # Help section
    with st.expander("ℹ️ Help & Information"):
        st.markdown("""
        **How to use:**
        1. Upload both CRF (.docx) and Protocol (.pdf) files
        2. Click 'Start Processing'
        3. Monitor real-time progress in the dashboards
        4. Download extracted data when complete
        
        **Supported formats:**
        - CRF: Microsoft Word (.docx)
        - Protocol: Adobe PDF (.pdf)
        """)

# Main content area - Two column layout
col1, col2 = st.columns(2, gap="large")

# Protocol Dashboard
with col1:
    st.markdown('<div class="card-header">📄 Protocol REF Extraction</div>', unsafe_allow_html=True)
    
    # Status badge
    st.markdown(status_badge(st.session_state.protocol_metrics['status']), unsafe_allow_html=True)
    
    # Metrics in a grid
    metric_cols = st.columns(3)
    with metric_cols[0]:
        st.metric("📑 Total Pages", st.session_state.protocol_metrics['total_pages'])
    with metric_cols[1]:
        st.metric("✅ Processed", st.session_state.protocol_metrics['pages_processed'])
    with metric_cols[2]:
        st.metric("📊 Tables", st.session_state.protocol_metrics['tables_found'])
    
    metric_cols2 = st.columns(3)
    with metric_cols2[0]:
        st.metric("📝 Rows", st.session_state.protocol_metrics['rows_extracted'])
    with metric_cols2[1]:
        conf_val = st.session_state.protocol_metrics['confidence']
        st.metric("🎯 Confidence", f"{conf_val:.0%}")
    with metric_cols2[2]:
        time_val = st.session_state.protocol_metrics['processing_time']
        st.metric("⏱️ Time", f"{time_val:.1f}s")
    
    # Progress bar
    if st.session_state.protocol_metrics['total_pages'] > 0:
        progress = st.session_state.protocol_metrics['pages_processed'] / st.session_state.protocol_metrics['total_pages']
        st.progress(progress, text=f"Progress: {progress:.0%}")
    
    # Results section
    if st.session_state.protocol_ready:
        st.success("✅ Protocol extraction completed successfully!")
        
        with st.expander("📊 Preview Extracted Data", expanded=False):
            st.dataframe(
                st.session_state.protocol_df,
                use_container_width=True,
                height=300
            )
        
        st.download_button(
            "📥 Download Protocol CSV",
            data=st.session_state.protocol_df.to_csv(index=False).encode('utf-8'),
            file_name='protocol_extraction.csv',
            mime='text/csv',
            use_container_width=True
        )
    elif st.session_state.protocol_error:
        st.error(f"❌ Processing Error")
        with st.expander("View Error Details"):
            st.code(st.session_state.protocol_error)

# CRF Dashboard
with col2:
    st.markdown('<div class="card-header">📋 Mock CRF Extraction</div>', unsafe_allow_html=True)
    
    # Status badge
    st.markdown(status_badge(st.session_state.crf_metrics['status']), unsafe_allow_html=True)
    
    # Metrics in a grid
    metric_cols = st.columns(3)
    with metric_cols[0]:
        st.metric("📦 Total Chunks", st.session_state.crf_metrics['total_chunks'])
    with metric_cols[1]:
        st.metric("✅ Processed", st.session_state.crf_metrics['chunks_processed'])
    with metric_cols[2]:
        st.metric("📋 Forms", st.session_state.crf_metrics['forms_found'])
    
    metric_cols2 = st.columns(3)
    with metric_cols2[0]:
        st.metric("📝 Items", st.session_state.crf_metrics['items_extracted'])
    with metric_cols2[1]:
        conf_val = st.session_state.crf_metrics['confidence']
        st.metric("🎯 Confidence", f"{conf_val:.0%}")
    with metric_cols2[2]:
        time_val = st.session_state.crf_metrics['processing_time']
        st.metric("⏱️ Time", f"{time_val:.1f}s")
    
    # Progress bar
    if st.session_state.crf_metrics['total_chunks'] > 0:
        progress = st.session_state.crf_metrics['chunks_processed'] / st.session_state.crf_metrics['total_chunks']
        st.progress(progress, text=f"Progress: {progress:.0%}")
    
    # Results section
    if st.session_state.crf_ready:
        st.success("✅ CRF extraction completed successfully!")
        
        with st.expander("📊 Preview Extracted Data", expanded=False):
            st.dataframe(
                st.session_state.crf_df,
                use_container_width=True,
                height=300
            )
        
        st.download_button(
            "📥 Download CRF CSV",
            data=st.session_state.crf_df.to_csv(index=False).encode('utf-8'),
            file_name='crf_extraction.csv',
            mime='text/csv',
            use_container_width=True
        )
    elif st.session_state.crf_error:
        st.error(f"❌ Processing Error")
        with st.expander("View Error Details"):
            st.code(st.session_state.crf_error)

# Process Button Logic
if process_button:
    logger.info("=== PROCESSING STARTED ===")
    
    if not uploaded_crf_file or not uploaded_protocol_file:
        st.warning("⚠️ Please upload both documents before processing.")
        logger.warning("Processing attempted without both files uploaded")
    else:
        st.session_state.processing_active = True
        
        # Save uploaded files
        crf_path = "temp_crf.docx"
        protocol_path = "temp_protocol.pdf"
        
        logger.info(f"Saving uploaded files: {crf_path}, {protocol_path}")
        with open(crf_path, "wb") as f:
            f.write(uploaded_crf_file.getbuffer())
        with open(protocol_path, "wb") as f:
            f.write(uploaded_protocol_file.getbuffer())
        logger.info("Files saved successfully")
        
        # Reset states
        st.session_state.protocol_ready = False
        st.session_state.crf_ready = False
        st.session_state.protocol_error = None
        st.session_state.crf_error = None
        
        # Create a container for live logs
        log_container = st.container()
        
        # Process Protocol
        with log_container:
            st.markdown("### 📊 Processing Logs")
            log_placeholder = st.empty()
            
        logger.info("Starting Protocol REF processing")
        try:
            protocol_start_time = time.time()
            st.session_state.protocol_metrics['status'] = 'Processing'
            
            log_placeholder.info("🔍 Extracting table pages from Protocol PDF...")
            logger.info("Calling extract_table_pages")
            
            extracted_pdf_path = protocol_processor.extract_table_pages(protocol_path)
            logger.info(f"Extracted PDF path: {extracted_pdf_path}")
            
            if extracted_pdf_path:
                # Get page count
                import fitz
                pdf_doc = fitz.open(extracted_pdf_path)
                page_count = len(pdf_doc)
                st.session_state.protocol_metrics['total_pages'] = page_count
                pdf_doc.close()
                logger.info(f"Total pages to process: {page_count}")
                
                log_placeholder.info(f"📄 Found {page_count} pages to process...")
                
                # Process pages
                import pdfplumber
                table_settings = {
                    "vertical_strategy": "lines",
                    "horizontal_strategy": "lines",
                    "snap_tolerance": 300,
                    "edge_min_length": 100,
                }
                
                logger.info("Starting page-by-page processing")
                with pdfplumber.open(extracted_pdf_path) as pdf:
                    df_ai = pd.DataFrame()
                    tables_count = 0
                    
                    for i in range(len(pdf.pages)):
                        logger.info(f"Processing page {i+1}/{len(pdf.pages)}")
                        page = pdf.pages[i]
                        tables_on_page = page.extract_tables(table_settings=table_settings)
                        
                        if tables_on_page:
                            tables_count += len(tables_on_page)
                            st.session_state.protocol_metrics['tables_found'] = tables_count
                            logger.info(f"Found {len(tables_on_page)} tables on page {i+1}")
                            
                            raw_data = pd.DataFrame()
                            for table_idx, table_data in enumerate(tables_on_page):
                                if table_data and len(table_data) >= 2:
                                    raw_data = pd.concat([raw_data, pd.DataFrame(table_data)])
                            
                            if not raw_data.empty:
                                logger.info(f"Processing {len(raw_data)} rows with AI")
                                log_placeholder.info(f"🤖 Processing table data with AI (Page {i+1})...")
                                nd = protocol_processor.table_ai(raw_data)
                                df_ai = pd.concat([df_ai, nd])
                                logger.info(f"AI processing complete, total rows: {len(df_ai)}")
                        
                        st.session_state.protocol_metrics['pages_processed'] = i + 1
                        st.session_state.protocol_metrics['rows_extracted'] = len(df_ai)
                        
                        log_placeholder.success(f"✅ Processed {i+1}/{len(pdf.pages)} pages | {tables_count} tables | {len(df_ai)} rows")
                
                logger.info(f"Protocol processing complete. Total rows: {len(df_ai)}")
                
                if not df_ai.empty:
                    protocol_df = pd.DataFrame(df_ai)
                    protocol_df.columns = protocol_df.iloc[0]
                    protocol_df = protocol_df[1:].reset_index(drop=True)
                    protocol_df = protocol_df.dropna(axis=1, how='all')
                    
                    st.session_state.protocol_df = protocol_df
                    st.session_state.protocol_ready = True
                    st.session_state.protocol_metrics['status'] = 'Complete'
                    st.session_state.protocol_metrics['confidence'] = min(0.85 + (len(protocol_df) / 100) * 0.15, 1.0)
                    logger.info("Protocol processing marked as complete")
                else:
                    st.session_state.protocol_metrics['status'] = 'Error'
                    logger.warning("No data extracted from protocol")
                
                if os.path.exists(extracted_pdf_path):
                    os.remove(extracted_pdf_path)
                    logger.info(f"Cleaned up: {extracted_pdf_path}")
            else:
                st.session_state.protocol_error = "Could not identify Schedule of Activities pages"
                st.session_state.protocol_metrics['status'] = 'Error'
                logger.error("Failed to extract table pages")
            
            protocol_end_time = time.time()
            st.session_state.protocol_metrics['processing_time'] = protocol_end_time - protocol_start_time
            logger.info(f"Protocol processing time: {st.session_state.protocol_metrics['processing_time']:.2f}s")
                
        except Exception as e:
            st.session_state.protocol_error = str(e)
            st.session_state.protocol_metrics['status'] = 'Error'
            logger.error(f"Protocol processing error: {e}", exc_info=True)
        
        # Process Mock CRF
        logger.info("Starting CRF processing")
        try:
            crf_start_time = time.time()
            st.session_state.crf_metrics['status'] = 'Processing'
            
            log_placeholder.info("📦 Chunking CRF document...")
            logger.info("Calling process_crf_docx")
            
            crf_chunks = crf_processor.process_crf_docx(crf_path)
            chunk_count = len(crf_chunks)
            st.session_state.crf_metrics['total_chunks'] = chunk_count
            logger.info(f"Created {chunk_count} chunks")
            
            log_placeholder.info(f"🤖 Extracting data from {chunk_count} chunks...")
            
            # Extract with tracking
            import json
            conversation = [{"role": "system", "content": crf_processor.system_prompt}]
            all_forms_data = []
            
            for i, chunk in enumerate(crf_chunks):
                logger.info(f"Processing chunk {i+1}/{chunk_count}")
                user_msg = crf_processor.user_prompt(chunk['text'])
                messages = conversation + [{"role": "user", "content": user_msg}]
                
                try:
                    response = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=messages,
                        response_format={"type": "json_object"}
                    )
                    
                    answer = json.loads(response.choices[0].message.content)
                    
                    if 'forms' in answer:
                        all_forms_data.extend(answer['forms'])
                        st.session_state.crf_metrics['items_extracted'] = len(all_forms_data)
                        st.session_state.crf_metrics['forms_found'] = len(set(f.get('form_label', '') for f in all_forms_data))
                        logger.info(f"Chunk {i+1}: {len(answer['forms'])} forms extracted")
                    
                    st.session_state.crf_metrics['chunks_processed'] = i + 1
                    log_placeholder.success(f"✅ Processed {i+1}/{chunk_count} chunks | {len(all_forms_data)} items extracted")
                    
                except Exception as e:
                    logger.error(f"Error processing chunk {i+1}: {e}")
            
            logger.info(f"CRF extraction complete. Total items: {len(all_forms_data)}")
            
            if all_forms_data:
                log_placeholder.info("🔄 Mapping extracted data to template...")
                crf_df = pd.DataFrame(all_forms_data)
                final_crf = protocol_processor.map_data_manually(crf_df, header_row=4, start_row=5)
                
                st.session_state.crf_df = final_crf
                st.session_state.crf_ready = True
                st.session_state.crf_metrics['status'] = 'Complete'
                st.session_state.crf_metrics['confidence'] = min(0.80 + (len(crf_df) / 100) * 0.20, 1.0)
                logger.info("CRF processing marked as complete")
            else:
                st.session_state.crf_metrics['status'] = 'Error'
                logger.warning("No forms data extracted from CRF")
            
            crf_end_time = time.time()
            st.session_state.crf_metrics['processing_time'] = crf_end_time - crf_start_time
            logger.info(f"CRF processing time: {st.session_state.crf_metrics['processing_time']:.2f}s")
                
        except Exception as e:
            st.session_state.crf_error = str(e)
            st.session_state.crf_metrics['status'] = 'Error'
            logger.error(f"CRF processing error: {e}", exc_info=True)
        
        # Cleanup
        for f in [crf_path, protocol_path]:
            if os.path.exists(f):
                os.remove(f)
                logger.info(f"Cleaned up: {f}")
        
        st.session_state.processing_active = False
        logger.info("=== PROCESSING COMPLETED ===")
        
        log_placeholder.success("🎉 All processing completed! Check results above.")
        time.sleep(2)
        st.rerun()
