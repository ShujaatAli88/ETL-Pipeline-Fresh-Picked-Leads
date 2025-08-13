# ETL Fresh Picked Leads

Automated ETL pipeline for scraping, downloading, and uploading FreshPickedLeads.com data to Google BigQuery, with robust logging and error monitoring via Sentry.

---

## Features

- **Automated Web Scraping:** Uses Selenium to log in, set date ranges, fetch, and download leads as CSV files.
- **BigQuery Integration:** Uploads all downloaded CSVs to Google BigQuery, with schema autodetection and ingestion date tracking.
- **Robust Logging:** Centralized logging to both console and file using Loguru.
- **Error Monitoring:** Integrated with Sentry for real-time error tracking and alerting.
- **Custom Exception Handling:** Easily extendable for project-specific error types and reporting.

---

## Requirements

- Python 3.8+
- Google Cloud Service Account with BigQuery permissions
- Chrome browser installed
- ChromeDriver (auto-managed by `webdriver_manager`)
- Environment variables set in `.env` file

---

## Setup

1. **Clone the repository:**
	```sh
	git clone https://github.com/yourusername/etl-fresh-picked-leads.git
	cd etl-fresh-picked-leads
	```

2. **Create and activate a virtual environment:**
	```sh
	python -m venv .venv
	.venv\Scripts\activate  # On Windows
	# Or
	source .venv/bin/activate  # On Mac/Linux
	```

3. **Install dependencies:**
	```sh
	pip install -r requirements.txt
	```

4. **Set up your `.env` file:**
	```
	PROJECT_ID=your-gcp-project-id
	DATASET_ID=your_bigquery_dataset
	EMAIL=your_freshpickedleads_email
	PASSWORD=your_freshpickedleads_password
	SENTRY_DSN=your_sentry_dsn
	```

5. **Add your Google Cloud service account key:**
	- Download the JSON key from Google Cloud Console.
	- Place it in the project root (e.g., `wholesaling-data-warehouse-cd2929689ac2.json`).

---

## Usage

### Run the full ETL pipeline

```sh
python main.py
```

This will:
- Log in to FreshPickedLeads.com
- Download the latest leads as CSV files
- Upload all CSVs to BigQuery
- Log all actions and errors
- Report exceptions to Sentry

### Run only the BigQuery uploader

```sh
python big_uery_handler.py
```

---

## Project Structure

```
.
├── big_uery_handler.py      # BigQuery upload logic
├── config.py                # Loads environment variables
├── custom_exceptions.py     # Custom exception classes
├── exception_logger.py      # Logs and reports exceptions to Sentry
├── fresh_picked_leads.py    # Selenium scraping and download logic
├── log_handler.py           # Centralized logger setup
├── main.py                  # Main entry point for the ETL pipeline
├── requirements.txt         # Python dependencies
├── .env                     # Environment variables (not committed)
├── logs/                    # Log files
└── wholesaling-data-warehouse-cd2929689ac2.json  # GCP credentials
```

---

## Logging

- All logs are written to `logs/latest.log` and the console.
- Logging is managed by Loguru and initialized in `log_handler.py`.

---

## Error Monitoring

- Sentry is initialized in `main.py` using the DSN from `.env`.
- All unhandled exceptions are automatically reported.
- Use `log_exception(message, error)` from `exception_logger.py` to manually log and report handled exceptions.

---

## Custom Exceptions

- Define and raise custom exceptions in `custom_exceptions.py`.
- Catch and report them using `log_exception` for full traceability.

---

## Environment Variables

All sensitive configuration is managed via `.env` and loaded by `config.py`.

---

## Contributing

Pull requests and issues are welcome! Please open an issue to discuss your ideas or report bugs.

---

## Author

Shujaat ALI

---

**Happy automating!**
