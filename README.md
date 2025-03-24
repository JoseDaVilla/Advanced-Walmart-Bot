# Advanced Walmart Leasing Space Checker

This tool scrapes the Walmart leasing website for available rental spaces under 1000 sqft, checks Google Maps for reviews and competing mobile stores, and sends email notifications about matching properties.

## Features

- **Parallel processing** for faster scraping of Walmart leasing properties
- **Direct Google Maps search** to check review counts and nearby mobile phone repair shops
- **Email notifications** for properties that match criteria
- **Scheduling** for daily automated runs
- **Data versioning** to preserve historical findings

## Requirements

- Python 3.6+
- Chrome browser
- ChromeDriver (installed automatically via webdriver-manager)

## Installation

1. Clone this repository
2. Install required packages:

```bash
pip install -r requirements.txt
```

3. Update configuration in `config.py` if needed

## Usage

Run the script with:

```bash
python main.py [options]
```

### Options:

- `--test`: Run in test mode with sample data
- `--schedule`: Run once and then schedule daily execution at 8:00 AM
- `--quick`: Process only a limited number of properties (faster)
- `--workers N`: Use N parallel browser workers (default: 15)
- `--api N`: Use N parallel API/location check workers (default: 8)

### Performance Tips:

- For faster scraping, increase the number of workers with `--workers 20`
- For modern computers with good internet connections, 20-25 workers is usually optimal
- If you experience crashes or errors, reduce the number of workers to 10-15
- You can check progress in the log file `walmart_leasing_parallel.log`

## How It Works

1. **Scraping Phase**: The tool scrapes the Walmart leasing website to find properties with available spaces under 1000 sqft.
2. **Verification Phase**: It then checks Google Maps to:
   - Verify the property has over 10,000 reviews
   - Ensure no mobile phone repair stores are nearby (within 100 meters)
3. **Notification Phase**: Properties meeting all criteria are emailed to the configured recipient.

## Project Structure

- `main.py`: Main entry point
- `config.py`: Configuration settings
- `scraper.py`: Walmart leasing site scraper
- `location_checker.py`: Google Maps verification
- `email_notifier.py`: Email notification system
- `data_manager.py`: Data persistence
- `selenium_utils.py`: Browser automation utilities

## Output

Matching properties are saved in the `json_data` directory:
- `matching_properties.json`: Current matches
- `matching_properties_YYYYMMDD_HHMMSS.json`: Versioned historical data

## License

This project is for educational purposes only. Use responsibly and in accordance with website terms of service.
