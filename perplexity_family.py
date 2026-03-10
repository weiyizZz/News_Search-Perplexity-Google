import pandas as pd
import requests
import json
import time
from typing import List, Dict, Any
import os
from datetime import datetime


class PerplexityNewsSearcher:
    def __init__(self, api_key: str):
        """
        Initialize the Perplexity API client

        Args:
            api_key (str): Your Perplexity API key
        """
        self.api_key = api_key
        self.base_url = "https://api.perplexity.ai/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

    def create_prompt(self, company_name: str, company_website: str) -> str:
        """
        Create the search prompt for a specific company

        Args:
            company_name (str): Name of the company
            company_website (str): Company's website URL

        Returns:
            str: Formatted prompt for Perplexity API
        """
        prompt = f"""Find the 3 most recent news-style articles about the Belgian company {company_name}
        You may include results from:
        Reputable news websites, or
        {company_name}'s official website {company_website}, only if the link goes directly to some news-related content.
        The article must meet all of the following criteria:
        1. Articles MUST explicitly mention '[COMPANY NAME]' by name (or a documented, verifiable alternative/alias for THIS Belgian company) in the body or title. Do not include articles that only reference other key words, broader or generic term.
        2. The URL is unique to the article — do not return homepage URLs or listing pages (e.g., /news, /stories, /press, etc.). However, if the company's official website only has a single news page with dated news or announcements relevant to the company, you may include it.
        3. If no news-style articles or company news/announcements meeting the above criteria exist, you may include a third-party financial data or business information page that explicitly mentions the company by its registered name and provides financial/accounting or legal information, as long as the page is individually accessible and up to date.
        4. If no real news-style article exists, or if the only available article is more than 2 years old, return the best-matching official company news, blog, or announcement page instead, even if it's dated or non-traditional. However, criteria 1 must be followed.
        5. It must be freely accessible (no login or subscription required).
        6. All URLs must be article-specific (not listing or homepages), unless the only available company news is found on a dedicated company news/newsroom page.
        Return the result as a JSON array of objects with fields "url", "summary", "date", and "category". Provide no other output.
        The Category column should specify if the result is:
        a "news article" (from a reputable news site),
        a "company news page" (official news/update/announcement page from the company),
        a "business data page" (from a third-party business directory or financial info site),
        or any other relevant, clearly described category.
        If no result is found that satisfies the criteria, return a single object with all four fields set to null.
        DO NOT include any other text, explanation, or message—only the JSON output."""

        return prompt

    def search_company_news(self, company_name: str, company_website: str, max_retries: int = 3) -> List[
        Dict[str, Any]]:
        """
        Search for news about a specific company using Perplexity API

        Args:
            company_name (str): Name of the company
            company_website (str): Company's website URL
            max_retries (int): Maximum number of retry attempts

        Returns:
            List[Dict]: List of news articles found
        """
        prompt = self.create_prompt(company_name, company_website)

        payload = {
            "model": "sonar-pro",  # Use Perplexity's sonar-pro model
            "messages": [
                {
                    "role": "system",
                    "content": "You are a helpful assistant that searches for news articles and returns results in JSON format only."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.3,
            "max_tokens": 2000
        }

        for attempt in range(max_retries):
            try:
                print(f"Searching for news about {company_name} (attempt {attempt + 1}/{max_retries})...")

                response = requests.post(
                    self.base_url,
                    headers=self.headers,
                    json=payload,
                    timeout=30
                )

                if response.status_code == 200:
                    result = response.json()
                    content = result['choices'][0]['message']['content']

                    # Try to parse JSON from the response
                    try:
                        # Look for JSON array in the response
                        start_idx = content.find('[')
                        end_idx = content.rfind(']') + 1

                        if start_idx != -1 and end_idx != 0:
                            json_str = content[start_idx:end_idx]
                            news_data = json.loads(json_str)

                            # Validate that we have the expected structure
                            if isinstance(news_data, list):
                                for item in news_data:
                                    if not all(key in item for key in ['url', 'summary', 'date', 'category']):
                                        print(f"Warning: Missing required fields in result for {company_name}")

                                print(f"Successfully found {len(news_data)} articles for {company_name}")
                                return news_data
                            else:
                                raise ValueError("Response is not a JSON array")

                    except (json.JSONDecodeError, ValueError) as e:
                        print(f"Error parsing JSON response for {company_name}: {e}")
                        print(f"Raw response: {content[:500]}...")  # First 500 chars for debugging

                        # Return empty result with error info
                        return [{
                            "url": "ERROR",
                            "summary": f"Failed to parse API response: {str(e)}",
                            "date": datetime.now().strftime("%Y-%m-%d"),
                            "category": "error"
                        }]

                else:
                    print(f"API request failed with status {response.status_code}: {response.text}")

            except requests.exceptions.RequestException as e:
                print(f"Network error for {company_name} (attempt {attempt + 1}): {e}")

            # Wait before retrying
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff

        # If all retries failed, return error result
        print(f"Failed to get results for {company_name} after {max_retries} attempts")
        return [{
            "url": "ERROR",
            "summary": f"Failed to retrieve data after {max_retries} attempts",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "category": "error"
        }]

    def process_companies(self, input_file: str, output_file: str, max_companies: int = None) -> None:
        """
        Process companies from Excel file and search for news

        Args:
            input_file (str): Path to input Excel file
            output_file (str): Path to output Excel file
            max_companies (int): Maximum number of companies to process (None for all)
        """
        try:
            # Read the Excel file
            print(f"Reading companies from {input_file}...")
            df = pd.read_excel(input_file)

            # Validate required columns
            required_columns = ['companies', 'website']
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")

            # Limit number of companies if specified
            if max_companies:
                df = df.head(max_companies)
                print(f"Processing first {len(df)} companies...")

            # Prepare result dataframe
            result_columns = ['companies', 'website']
            for i in range(1, 4):  # 3 articles per company
                result_columns.extend([
                    f'perplexity_url_{i}',
                    f'perplexity_summary_{i}',
                    f'perplexity_date_{i}',
                    f'perplexity_category_{i}'
                ])

            results = []

            # Process each company
            for idx, row in df.iterrows():
                company_name = row['companies']
                company_website = row['website']

                print(f"\n--- Processing company {idx + 1}/{len(df)}: {company_name} ---")

                # Search for news
                news_articles = self.search_company_news(company_name, company_website)

                # Prepare result row
                result_row = {
                    'companies': company_name,
                    'website': company_website
                }

                # Add up to 3 articles
                for i in range(3):
                    if i < len(news_articles):
                        article = news_articles[i]
                        result_row[f'perplexity_url_{i + 1}'] = article.get('url', '')
                        result_row[f'perplexity_summary_{i + 1}'] = article.get('summary', '')
                        result_row[f'perplexity_date_{i + 1}'] = article.get('date', '')
                        result_row[f'perplexity_category_{i + 1}'] = article.get('category', '')
                    else:
                        # Fill empty fields if fewer than 3 articles found
                        result_row[f'perplexity_url_{i + 1}'] = ''
                        result_row[f'perplexity_summary_{i + 1}'] = ''
                        result_row[f'perplexity_date_{i + 1}'] = ''
                        result_row[f'perplexity_category_{i + 1}'] = ''

                results.append(result_row)

                # Add delay between requests to be respectful to the API
                time.sleep(1)

            # Create results dataframe and save to Excel
            results_df = pd.DataFrame(results)
            results_df.to_excel(output_file, index=False)

            print(f"\n✅ Processing complete! Results saved to {output_file}")
            print(f"Processed {len(results)} companies")

        except FileNotFoundError:
            print(f"❌ Error: Input file '{input_file}' not found")
        except Exception as e:
            print(f"❌ Error processing companies: {e}")


def main():
    # Define your Google API key and CSE ID directly here
    API_KEY = "YOUR_API_KEY_HERE"
    SEARCH_ENGINE_ID = "YOUR_CSE_ID_HERE"

    # File paths (adjust as needed)
    INPUT_FILE = "./company_list.xlsx"   # Must contain a column "Full name Latin Alphabet"
    OUTPUT_FILE = "./family_companies_perplexitynews.xlsx"

    # Optional: if your Excel has a specific sheet name, set it here; otherwise None uses the first sheet
    SHEET_NAME = None

    # Create searcher instance
    searcher = GoogleNewsSearcher(API_KEY, SEARCH_ENGINE_ID)

    # Process the file (only first 100 rows by default)
    searcher.process_excel_file(
        input_file=INPUT_FILE,
        output_file=OUTPUT_FILE,
        sheet_name=SHEET_NAME,
        name_column="Full name Latin Alphabet",
        delay=1.0,
        limit_rows=100,
        num_results=3
    )



if __name__ == "__main__":
    main()