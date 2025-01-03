import pandas as pd
import os

# Load the CSV file
input_file = os.path.join(
    r'C:\Users\koage\OneDrive\デスクトップ',
    'Internship Asgn',
    'github',
    'dataWebScrapingIndeed',
    'first_100_result.csv'
)

df = pd.read_csv(input_file)

# Check if the "EmailCrawler" column exists and is not empty
if 'EmailCrawler' not in df.columns:
    print("The 'EmailCrawler' column is missing from the CSV.")
    exit()

# Check the first few rows of the EmailCrawler column
print("Preview of 'EmailCrawler' column before processing:")
print(df['EmailCrawler'].head())

# Total number of websites
total_websites = df['Website'].notna().sum()

# Function to clean emails (remove "None" and empty strings)
def clean_emails(row):
    emails = str(row).split(',')  # Split emails by commas
    # Remove 'None' (case-insensitive) and empty strings, and strip whitespace
    cleaned_emails = [email.strip() for email in emails if email.strip().lower() != 'none' and email.strip()]
    return ",".join(cleaned_emails)

# Clean the "EmailCrawler" column
df['EmailCrawler'] = df['EmailCrawler'].fillna('').apply(clean_emails)

# Count websites with no emails
websites_with_no_emails = df[df['EmailCrawler'] == '']['Website'].notna().sum()

# Function to split emails into chunks of 20
def split_emails(row):
    emails = row.split(',')  # Split cleaned emails by commas
    chunks = [",".join(emails[i:i + 20]) for i in range(0, len(emails), 20)]
    return chunks

# Split the "EmailCrawler" column into chunks of 20
email_chunks = df['EmailCrawler'].apply(split_emails)

# Create new email columns dynamically
max_columns = email_chunks.apply(len).max()
for i in range(max_columns):
    column_name = f"EmailCrawler {i + 1}"
    df[column_name] = email_chunks.apply(lambda x: x[i] if i < len(x) else '')

# Remove the old "EmailCrawler" column
df.drop(columns=["EmailCrawler"], inplace=True)

# Reorder columns to match the desired structure
columns_order = (
    ["Name", "Address", "Phone", "Email", "Website"] +
    ["ABN", "Profession"] +
    [
        "InternshipOrInternships", 
        "ApprenticeshipOrApprenticeships", 
        "TraineeshipOrTraineeships", 
        "WorkPlacement", 
        "JobOrJobs", 
        "CareerOrCareers", 
        "HROrHumanResources", 
        "Employment"
    ] +
    [f"EmailCrawler {i + 1}" for i in range(max_columns)]
)

df = df[columns_order]

# Save the updated DataFrame to a new CSV file
output_file = os.path.join(
    r'C:\Users\koage\OneDrive\デスクトップ',
    'Internship Asgn',
    'github',
    'dataWebScrapingIndeed',
    'cleaned_first_100_result.csv'
)
os.makedirs(os.path.dirname(output_file), exist_ok=True)

df.to_csv(output_file, index=False)

# Print summary of findings
print(f"Emails cleaned and saved to {output_file}")
print(f"Total websites: {total_websites}")
print(f"Websites with no emails: {websites_with_no_emails}")
