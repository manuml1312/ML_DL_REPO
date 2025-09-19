import csv
import os

# Read the final database file
data = []
with open('Arogyan_AI_Medical_Database_Final.csv', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        data.append(row)

print(f"Loaded {len(data)} records to organize by folder structure")

def extract_folder_info(file_path):
    """Extract folder structure information from file path"""
    # Split the path: e.g., "ab/b0/cer253_NBK579797.tar.gz" -> ["ab", "b0", "cer253_NBK579797.tar.gz"]
    parts = file_path.split('/')
    
    if len(parts) >= 3:
        primary_folder = parts[0]  # e.g., "ab"
        secondary_folder = parts[1]  # e.g., "b0"
        filename = parts[2]  # e.g., "cer253_NBK579797.tar.gz"
        
        return {
            'primary': primary_folder,
            'secondary': secondary_folder,
            'filename': filename,
            'full_path': file_path
        }
    else:
        # Handle edge cases
        return {
            'primary': parts[0] if len(parts) > 0 else '',
            'secondary': parts[1] if len(parts) > 1 else '',
            'filename': parts[-1] if len(parts) > 0 else '',
            'full_path': file_path
        }

# Add folder information to each record
for record in data:
    folder_info = extract_folder_info(record['File'])
    record.update(folder_info)

# Sort by folder structure:
# 1. Primary folder (00, 01, 02, ..., 0a, 0b, ..., a0, a1, ..., ff)
# 2. Secondary folder within primary
# 3. Filename within secondary folder

def folder_sort_key(record):
    """Create sort key for proper folder ordering"""
    primary = record['primary'].lower()
    secondary = record['secondary'].lower()
    filename = record['filename'].lower()
    
    # Convert hex-like folder names to proper sorting
    # This handles the progression: 00, 01, ..., 09, 0a, 0b, ..., 0f, 10, 11, etc.
    
    return (primary, secondary, filename)

# Sort the data
data.sort(key=folder_sort_key)

print("Data sorted by folder structure")

# Create the organized output
fieldnames = [
    'Priority Level', 'Category', 'Topic/Resource Title', 
    'Relevance to Arogyan AI', 'File', 'Download URL', 'Publisher', 
    'Publication Year', 'Accession ID', 'Last Updated'
]

# Write the organized file
with open('Arogyan_AI_Medical_Database_Organized.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    
    for record in data:
        # Write only the original fields, not the helper fields
        output_record = {field: record[field] for field in fieldnames}
        writer.writerow(output_record)

print("Organized file saved as: Arogyan_AI_Medical_Database_Organized.csv")

# Create a folder structure summary
folder_summary = {}
current_primary = None

for record in data:
    primary = record['primary']
    secondary = record['secondary']
    
    if primary not in folder_summary:
        folder_summary[primary] = {}
    
    if secondary not in folder_summary[primary]:
        folder_summary[primary][secondary] = []
    
    folder_summary[primary][secondary].append({
        'title': record['Topic/Resource Title'],
        'priority': record['Priority Level'],
        'file': record['filename']
    })

# Display folder structure organization
print("\n=== FOLDER STRUCTURE ORGANIZATION ===")
folder_count = 0
total_files = 0

for primary_folder in sorted(folder_summary.keys()):
    secondary_folders = folder_summary[primary_folder]
    folder_file_count = sum(len(files) for files in secondary_folders.values())
    
    print(f"\n📁 {primary_folder}/ ({len(secondary_folders)} subfolders, {folder_file_count} files)")
    
    # Show first few secondary folders for each primary folder
    for i, secondary_folder in enumerate(sorted(secondary_folders.keys())):
        if i < 3:  # Show only first 3 secondary folders
            files = secondary_folders[secondary_folder]
            high_priority_count = len([f for f in files if f['priority'] == 'High'])
            print(f"  📂 {secondary_folder}/ ({len(files)} files, {high_priority_count} high priority)")
            
            # Show first high priority file if available
            high_priority_files = [f for f in files if f['priority'] == 'High']
            if high_priority_files:
                print(f"      🔹 {high_priority_files[0]['title'][:60]}...")
        elif i == 3:
            remaining = len(secondary_folders) - 3
            print(f"  📂 ... and {remaining} more subfolders")
            break
    
    folder_count += len(secondary_folders)
    total_files += folder_file_count
    
    if len(folder_summary) > 10 and primary_folder == sorted(folder_summary.keys())[9]:
        remaining_primary = len(folder_summary) - 10
        print(f"\n📁 ... and {remaining_primary} more primary folders")
        break

print(f"\n=== ORGANIZATION SUMMARY ===")
print(f"📊 Total primary folders: {len(folder_summary)}")
print(f"📊 Total secondary folders: {folder_count}")
print(f"📊 Total files: {total_files}")
print(f"📊 Files now organized by hierarchical folder structure")

# Show sample of organized structure
print("\n=== SAMPLE OF ORGANIZED STRUCTURE ===")
for i, record in enumerate(data[:10], 1):
    print(f"{i:2d}. 📁 {record['primary']}/{record['secondary']}/{record['filename']}")
    print(f"     📄 {record['Topic/Resource Title'][:70]}...")
    print(f"     🎯 Priority: {record['Priority Level']} | Category: {record['Category']}")
    print()

print("✅ Database successfully organized by FTP folder structure!")