import pandas as pd
import os

# Fetch file path from the environment
file_path = os.getenv("FILE_PATH")
if not file_path:
    raise ValueError("FILE_PATH environment variable is not set.")

# Load the CSV file
df = pd.read_csv(file_path)

# Function to generate URLs
def generate_url(email):
    # Always construct the URL based on the email domain and include "www."
    domain = email.split('@')[-1]
    url = f"https://www.{domain}"
    return url

# Process the "Email" column
def process_emails(df):
    results = []
    success_count = 0  # Counter for successful email-to-URL conversions
    excluded_domains = ['gmail.com', 'outlook.com', 'yahoo.com', 'icloud.com', 'hotmail.com', 'bigpond.com', 'netspace.net.au']

    for email, website in zip(df['Email'], df['Website']):
        # If the Website column already has a value, skip
        if pd.notna(website) and isinstance(website, str):
            results.append(website)
        # If email is invalid or from excluded domains, return empty
        elif pd.isna(email) or not isinstance(email, str) or any(domain in email for domain in excluded_domains):
            results.append(None)
        # Generate a URL for valid emails
        else:
            results.append(generate_url(email))
            success_count += 1  # Increment counter
    return results, success_count

# Ensure the "Website" column exists (for cases where it's not in the input file)
if 'Website' not in df.columns:
    df['Website'] = None

# Apply the function and get the success count
df['Website'], success_count = process_emails(df)

# Save the updated DataFrame to the same file
df.to_csv(file_path, index=False)

print(f"Processed file saved to {file_path}")
print(f"Number of emails successfully converted to URLs: {success_count}")
