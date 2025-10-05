```mermaid
flowchart TD
    A[Start Streamlit App] --> B[Import Libraries]
    B --> C[Initialize Session State]
    C --> D[Display UI: Title, File Uploaders]
    D --> E[User Uploads DOCX & PDF]
    E --> F[User Clicks "Process Documents"]
    
    F --> G1[Save Uploaded Files Temporarily]
    G1 --> G2[Reset Processing States]
    G2 --> H[Process Protocol REF (.pdf)]
    G2 --> I[Process Mock CRF (.docx)]
    H --> H1[Extract Schedule of Activities Pages (fitz, pdfplumber)]
    H1 --> H2[Extract & Combine Table Data (pdfplumber, pandas)]
    H2 --> H3[Clean Table Data with OpenAI API]
    H3 --> H4[Display Table Data & Enable Download]
    H4 --> J[Cleanup Temp Files]
    
    I --> I1[Chunk Document (docx, DOCXCRFChunker)]
    I1 --> I2[Extract CRF Info with OpenAI API]
    I2 --> I3[Display CRF Data & Enable Download]
    I3 --> J
    
    J --> K[Show Download Buttons]
    K --> L[Option to Reset & Rerun]
    
    subgraph Libraries & Technologies
        B1[streamlit]
        B2[fitz (PyMuPDF)]
        B3[pdfplumber]
        B4[pandas]
        B5[docx]
        B6[numpy]
        B7[OpenAI API]
        B8[json]
        B9[os]
    end
    B --> B1
    B --> B2
    B --> B3
    B --> B4
    B --> B5
    B --> B6
    B --> B7
    B --> B8
    B --> B9

    style A fill:#c0e6ff,stroke:#333,stroke-width:2px
    style F fill:#ffd3c0,stroke:#333,stroke-width:2px
    style H fill:#ffe7b3
    style I fill:#ffe7b3
    style K fill:#c0ffc0
    style L fill:#e6e6e6
    style B fill:#f0f0f0
    style B1 fill:#b3e6ff
    style B2 fill:#b3e6ff
    style B3 fill:#b3e6ff
    style B4 fill:#b3e6ff
    style B5 fill:#b3e6ff
    style B6 fill:#b3e6ff
    style B7 fill:#b3e6ff
    style B8 fill:#b3e6ff
    style B9 fill:#b3e6ff
```
**High-Level Flow Explanation:**
- The app uses **Streamlit** for the UI and user interaction.
- Users upload two files: a DOCX (Mock CRF) and a PDF (Protocol REF).
- The PDF is processed to extract relevant pages and tables with **PyMuPDF (fitz)** and **pdfplumber**; data is cleaned using **OpenAI API**.
- The DOCX is chunked and parsed with a custom class and **python-docx**, then sent to **OpenAI API** for structured extraction.
- **pandas** is used for data manipulation throughout.
- Final results are shown and can be downloaded.
