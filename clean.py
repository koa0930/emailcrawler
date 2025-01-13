import pandas as pd
import os

# Fetch the file path from the environment
file_path = os.getenv("FILE_PATH")
if not file_path:
    raise ValueError("FILE_PATH environment variable is not set.")

# Load the CSV file
df = pd.read_csv(file_path)

# Check if the "EmailCrawler" column exists
if 'EmailCrawler' not in df.columns:
    print("The 'EmailCrawler' column is missing from the CSV.")
    exit()

# Clean the "EmailCrawler" column
df['EmailCrawler'] = df['EmailCrawler'].fillna('').apply(
    lambda row: ",".join([email.strip() for email in row.split(',') if email.strip().lower() != 'none' and email.strip()])
)

# Count websites with no emails
websites_with_no_emails = df[df['EmailCrawler'] == '']['Website'].notna().sum()

# Split emails into chunks of 20 and create new columns
email_chunks = df['EmailCrawler'].apply(lambda row: [",".join(row.split(',')[i:i + 20]) for i in range(0, len(row.split(',')), 20)])
max_columns = email_chunks.apply(len).max()
for i in range(max_columns):
    column_name = f"EmailCrawler {i + 1}"
    df[column_name] = email_chunks.apply(lambda x: x[i] if i < len(x) else '')

# Drop the original "EmailCrawler" column
df.drop(columns=["EmailCrawler"], inplace=True)

# Save the updated CSV back to the same file
df.to_csv(file_path, index=False)

print(f"Processed file saved to {file_path}")
print(f"Websites with no emails: {websites_with_no_emails}")
