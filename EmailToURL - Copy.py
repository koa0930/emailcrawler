import pandas as pd
import os

# Define the file path
file_path = r'C:\Users\koage\OneDrive\デスクトップ\Internship Asgn\github\dataWebScrapingIndeed\email_scrapy\data\ndis_providers_part1.csv'

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
    for email, website in zip(df['Email'], df['Website']):
        # If the Website column already has a value, skip
        if pd.notna(website) and isinstance(website, str):
            results.append(website)
        # If email is invalid or from excluded domains, return empty
        elif pd.isna(email) or not isinstance(email, str) or any(domain in email for domain in ['gmail.com', 'outlook.com', 'yahoo.com','icloud.com']):
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

# Save the updated DataFrame to a new CSV file
output_path = os.path.join(os.path.dirname(file_path), 'ndis_providers_part1.csv')
df.to_csv(output_path, index=False)

print(f"Processed file saved to {output_path}")
print(f"Number of emails successfully converted to URLs: {success_count}")
