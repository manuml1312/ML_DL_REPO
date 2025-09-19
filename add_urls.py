import csv

# Base URL for the files
BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pub/litarch/"

# Read the complete database file
enhanced_data = []
with open('Arogyan_AI_Medical_Database_Complete.csv', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Create the full URL by combining base URL with file path
        file_path = row['File']
        full_url = BASE_URL + file_path
        
        # Add URL to the record
        enhanced_record = {
            'Priority Level': row['Priority Level'],
            'Category': row['Category'],
            'Topic/Resource Title': row['Topic/Resource Title'],
            'Relevance to Arogyan AI': row['Relevance to Arogyan AI'],
            'File': row['File'],
            'Download URL': full_url,
            'Publisher': row['Publisher'],
            'Publication Year': row['Publication Year'],
            'Accession ID': row['Accession ID'],
            'Last Updated': row['Last Updated']
        }
        enhanced_data.append(enhanced_record)

print(f"Added URLs to {len(enhanced_data)} records")

# Write the final file with URLs
fieldnames = [
    'Priority Level', 'Category', 'Topic/Resource Title', 
    'Relevance to Arogyan AI', 'File', 'Download URL', 'Publisher', 
    'Publication Year', 'Accession ID', 'Last Updated'
]

with open('Arogyan_AI_Medical_Database_Final.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    
    for record in enhanced_data:
        writer.writerow(record)

print("Final file with URLs saved as: Arogyan_AI_Medical_Database_Final.csv")

# Show sample URLs for verification
print("\n=== SAMPLE URLs FOR VERIFICATION ===")
for i, record in enumerate(enhanced_data[:10], 1):
    print(f"{i:2d}. {record['Topic/Resource Title'][:50]}...")
    print(f"     File: {record['File']}")
    print(f"     URL:  {record['Download URL']}")
    print()

# Create a URL-specific summary for high priority topics
print("=== TOP 10 HIGH PRIORITY TOPICS WITH DOWNLOAD URLS ===")
high_priority = [r for r in enhanced_data if r['Priority Level'] == 'High']

for i, record in enumerate(high_priority[:10], 1):
    print(f"{i:2d}. {record['Topic/Resource Title'][:60]}...")
    print(f"     Category: {record['Category']}")
    print(f"     Publisher: {record['Publisher'][:40]}...")
    print(f"     Year: {record['Publication Year']}")
    print(f"     Download: {record['Download URL']}")
    print()

print(f"\nSUMMARY:")
print(f"- Total records with URLs: {len(enhanced_data)}")
print(f"- High priority downloads: {len([r for r in enhanced_data if r['Priority Level'] == 'High'])}")
print(f"- All URLs follow pattern: {BASE_URL}[file_path]")
print(f"- Files are in .tar.gz format and can be downloaded directly")