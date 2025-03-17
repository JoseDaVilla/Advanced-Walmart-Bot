# Walmart Leasing Space Checker

This script automates the process of finding suitable retail spaces available at Walmart locations based on specific criteria.

## 🔍 What It Does

1. **Scrapes Walmart Leasing Page**: Extracts information about all available properties.
2. **Applies Filters**:
   - Spaces under 1000 sqft
   - Locations with more than 10,000 Google reviews
   - No mobile phone repair stores present
3. **Sends Email Notifications**: When matching properties are found.
4. **Runs on a Schedule**: Can be configured to check daily.

## ✨ Features

- **Parallel Processing**: Uses multiple browser instances to scrape properties faster.
- **API Integration**: Leverages Google Places API to check reviews and nearby businesses.
- **Smart Filtering**: Optimized to minimize API calls by filtering square footage first.
- **Fault Tolerance**: Handles errors gracefully and continues processing.
- **Detailed Logging**: Tracks progress and helps troubleshoot issues.

## 🛠️ Installation

1. **Clone this repository**:
   ```bash
   git clone https://github.com/yourusername/walmart-leasing-checker.git
   cd walmart-leasing-checker
   ```

2. **Install the required packages**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure settings**:
   - Update email settings in `walmart_leasing_checker_parallel.py`
   - Set your Google API key
   - Adjust criteria as needed

## 🚀 Usage

### Basic Usage

```bash
python walmart_leasing_checker_parallel.py
```

### Options

- `--test`: Run in test mode with sample data
- `--quick`: Process only the first 300 properties (faster)
- `--schedule`: Run once and then schedule daily execution
- `--parallel N`: Use N parallel browser workers (default: 6)
- `--help`: Display help information

### Example

```bash
python walmart_leasing_checker_parallel.py --parallel 8 --schedule
```

## 📋 Requirements

- Python 3.7+
- Chrome browser installed
- Internet connection
- Google Places API key
- SMTP-enabled email account

## ⚙️ How It Works

1. **Initial Setup**: The script loads the Walmart leasing page and counts available properties.
2. **Parallel Processing**: It divides properties among multiple workers for faster processing.
3. **Data Extraction**: Each worker extracts property details and space information.
4. **Filtering**: Properties with spaces under 1000 sqft are identified.
5. **API Verification**: For qualifying properties, it checks Google reviews and nearby businesses.
6. **Notification**: Matching properties are saved to JSON and sent via email.

## 🙏 Acknowledgements

- Selenium for web automation
- BeautifulSoup for HTML parsing
- Google Places API for location data
