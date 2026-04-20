import pandas as pd
import requests
import time
from typing import List
import json


class GoogleNewsSearcher:
    def __init__(self, api_key: str, search_engine_id: str):
        """
        Initialize the Google News Searcher

        Args:
            api_key: Your Google Custom Search API key
            search_engine_id: Your Custom Search Engine ID
        """
        self.api_key = api_key
        self.search_engine_id = search_engine_id
        self.base_url = "https://www.googleapis.com/customsearch/v1"

    def search_company_news(self, company_name: str, website: str, num_results: int = 3) -> List[str]:
        """
        Search for company news using Google Custom Search API

        Args:
            company_name: Name of the company
            website: Website of the company
            num_results: Number of results to return (default: 3)

        Returns:
            List of URLs from search results
        """
        # Construct search query
        query = f'"{company_name}" {website} news'

        params = {
            'key': self.api_key,
            'cx': self.search_engine_id,
            'q': query,
            'num': num_results
        }

        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()

            data = response.json()

            # Extract URLs from search results
            urls = []
            if 'items' in data:
                for item in data['items'][:num_results]:
                    urls.append(item.get('link', ''))

            # Pad with empty strings if we don't have enough results
            while len(urls) < num_results:
                urls.append('')

            return urls

        except requests.exceptions.RequestException as e:
            print(f"Error searching for {company_name}: {e}")
            return [''] * num_results
        except json.JSONDecodeError as e:
            print(f"Error parsing response for {company_name}: {e}")
            return [''] * num_results

    def process_excel_file(self, input_file: str, output_file: str, delay: float = 1.0):
        """
        Process Excel file and add news URLs

        Args:
            input_file: Path to input Excel file
            output_file: Path to output Excel file
            delay: Delay between API calls in seconds (to respect rate limits)
        """
        try:
            # Read the Excel file
            df = pd.read_excel(input_file, sheet_name='company_list')

            # Check if required columns exist
            required_columns = ['companies', 'website']
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                print(f"Error: Missing required columns: {missing_columns}")
                print(f"Available columns: {list(df.columns)}")
                return

            # Initialize new columns for URLs
            df['google_url_1'] = ''
            df['google_url_2'] = ''
            df['google_url_3'] = ''

            # Process each row
            total_rows = len(df)
            for index, row in df.iterrows():
                company_name = str(row['companies']).strip().lower()
                website = str(row['website']).strip()

                # Use "Belgium" as fallback if website is empty
                if pd.isna(row['website']) or not website:
                    website = "Belgium"
                    print(f"  Using 'Belgium' as fallback for {company_name} (empty website)")

                print(f"Processing {index + 1}/{total_rows}: {company_name}")

                # Search for news
                urls = self.search_company_news(company_name, website)

                # Update dataframe
                df.at[index, 'google_url_1'] = urls[0]
                df.at[index, 'google_url_2'] = urls[1]
                df.at[index, 'google_url_3'] = urls[2]

                # Add delay to respect API rate limits
                if index < total_rows - 1:  # Don't delay after the last item
                    time.sleep(delay)

            # Save the updated dataframe
            df.to_excel(output_file, index=False)
            print(f"Results saved to {output_file}")

        except FileNotFoundError:
            print(f"Error: Input file '{input_file}' not found")
        except Exception as e:
            print(f"Error processing file: {e}")


# Example usage
def main():
    # Replace with your actual API credentials
    API_KEY = "AIzaSyDySepB538rGFUw6Qi8Vx_iRbCWG_ovLkU"
    SEARCH_ENGINE_ID = "d6c3caa9009bf40c8"

    # File paths
    INPUT_FILE = "./family/family_owned_businesses.xlsx"
    OUTPUT_FILE = "./family/family_companies_with_news_v1.xlsx"

    # Create searcher instance
    searcher = GoogleNewsSearcher(API_KEY, SEARCH_ENGINE_ID)

    # Process the file
    searcher.process_excel_file(INPUT_FILE, OUTPUT_FILE, delay=1.0)


if __name__ == "__main__":
    main()
