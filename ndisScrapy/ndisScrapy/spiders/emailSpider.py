import scrapy
import pandas as pd
import re
import validators
from urllib.parse import urljoin
import os


class CombinedSpider(scrapy.Spider):
    name = "combined_spider"

    custom_settings = {
        'DEPTH_LIMIT': 2,
        'CONCURRENT_REQUESTS': 8,
        'DOWNLOAD_DELAY': 0.25,
        'RETRY_TIMES': 3,
        'FEED_EXPORT_ENCODING': 'utf-8',
        'LOG_LEVEL': 'INFO',
        'COOKIES_ENABLED': False,
        'USER_AGENT': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
    }

    CAREER_CATEGORIES = {
        "Internship": ["internship", "internships", "work with us internship"],
        "Apprenticeship": ["apprenticeship", "apprenticeships"],
        "Traineeship": ["traineeship", "traineeships"],
        "WorkExperience": ["work experience", "experience with us"],
        "JobOrJobs": ["job", "jobs", "work with us", "career opportunities"],
        "CareerOrCareers": ["career", "careers", "profession", "vocation", "occupation"],
        "HROrHumanResources": ["HR", "human resources", "talent acquisition", "people operations"]
    }

    def __init__(self, *args, **kwargs):
        super(CombinedSpider, self).__init__(*args, **kwargs)
        self.results = None
        self.data = None

    def start_requests(self):
        input_file = os.path.join(
            r'C:\Users\koage\OneDrive\デスクトップ',
            'Internship Asgn',
            'github',
            'dataWebScrapingIndeed',
            'email_scrapy',
            'data',
            'first_100.csv'
        )
        if not os.path.exists(input_file):
            self.logger.error(f"Input file not found: {input_file}")
            return

        self.data = pd.read_csv(input_file)
        self.data.columns = self.data.columns.str.strip()
        self.results = self.data.copy()
        for category in self.CAREER_CATEGORIES:
            self.results[category] = ''  # Add a column for each career category
        self.results['EmailCrawler'] = ''

        for index, row in self.data.iterrows():
            url = row.get('Website', None)
            if not isinstance(url, str) or not url.startswith(('http://', 'https://')):
                url = f'http://{url}' if isinstance(url, str) else None

            if not url or not validators.url(url):
                self.logger.info(f"Skipping row {index}: Invalid or missing website.")
                for category in self.CAREER_CATEGORIES:
                    self.results.at[index, category] = 'NO'
                continue

            yield scrapy.Request(
                url=url,
                callback=self.parse_fast,
                meta={"index": index, "website": url},
                errback=self.error_handling,
                dont_filter=True,
            )

    def parse_fast(self, response):
        """Fast parser to extract emails and career category presence."""
        index = response.meta['index']
        website = response.meta['website']

        # Check for career category keywords
        page_text = response.text.lower()
        for category, keywords in self.CAREER_CATEGORIES.items():
            if any(keyword in page_text for keyword in keywords):
                self.results.at[index, category] = 'YES'
            else:
                self.results.at[index, category] = 'NO'

        # Extract emails
        emails_found = self.extract_emails(response)
        if emails_found:
            self.assign_emails(index, emails_found)
        else:
            self.logger.info(f"No emails found by fast spider for {website}. Retrying with Playwright.")
            yield scrapy.Request(
                url=website,
                callback=self.parse_detailed,
                meta={**response.meta, "playwright": True},
                errback=self.error_handling,
                dont_filter=True,
            )

    def parse_detailed(self, response):
        """Detailed parser logic with Playwright."""
        index = response.meta['index']
        website = response.meta['website']

        emails_found = self.extract_emails(response)
        if emails_found:
            self.assign_emails(index, emails_found)
        else:
            self.logger.info(f"No emails found by detailed spider for {website}.")

    def assign_emails(self, index, emails):
        """Assign emails to the results data structure."""
        website = self.results.at[index, 'Website']
        valid_emails = [email for email in emails if '@' in email]
        if not valid_emails:
            self.logger.info(f"No valid emails found for {website}.")
            return

        current_emails = self.results.at[index, 'EmailCrawler']
        current_emails = current_emails if pd.notna(current_emails) else ''
        combined_emails = list(set(current_emails.split(", ") + valid_emails))
        combined_emails = [email.strip() for email in combined_emails if email.strip()]
        self.results.at[index, 'EmailCrawler'] = ", ".join(combined_emails)

        self.logger.info(f"Updated EmailCrawler for {website}: {self.results.at[index, 'EmailCrawler']}")

    def extract_emails(self, response):
        """Extract emails using regex from response."""
        # Extract emails from both text and mailto links
        text_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', response.text)
        href_emails = re.findall(r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', response.text)
        emails = set(text_emails + href_emails)

        # Log found emails
        if emails:
            self.logger.info(f"Found emails: {emails}")

        return emails

    def error_handling(self, failure):
        """Handle errors."""
        index = failure.request.meta['index']
        for category in self.CAREER_CATEGORIES:
            self.results.at[index, category] = 'NO'
        self.logger.error(f"Request failed: {failure.request.url}. Error: {failure.value}")

    def closed(self, reason):
        """Save results to CSV on spider close."""
        output_file = os.path.join(
            r'C:\Users\koage\OneDrive\デスクトップ',
            'Internship Asgn',
            'github',
            'dataWebScrapingIndeed',
            'first_100_result.csv'
        )
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        if self.results is not None and not self.results.empty:
            try:
                self.results.to_csv(output_file, index=False, encoding='utf-8')
                self.logger.info(f"Results saved to {output_file}")
            except Exception as e:
                self.logger.error(f"Error saving results: {e}")
        else:
            self.logger.warning("No results to save.")