import time
import os
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from log_handler import logger
from datetime import datetime, timedelta
from big_uery_handler import BigQueryUploader
from config import Config
from exception_logger import log_exception


# Suppress selenium and webdriver_manager logs
logging.getLogger("selenium").setLevel(logging.CRITICAL)
logging.getLogger("webdriver_manager").setLevel(logging.CRITICAL)


class FreshPickedLeadsBot:
    def __init__(self, headless: bool = False):
        self.headless = headless
        self.driver = None
        self.url = "https://freshpickedleads.com/login?redirect=%2Fapp%2Fleads"
        self._setup_driver()

    def _setup_driver(self):
        logger.info("Setting Up the Selenium Driver.")
        try:
            chrome_options = Options()
            if self.headless:
                chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")

            # ✅ Set download path to the directory where this script lives
            download_dir = os.path.dirname(os.path.abspath(__file__))
            logger.info(f"Setting Chrome download directory to: {download_dir}")

            prefs = {
                "download.default_directory": download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "profile.default_content_settings.popups": 0,
                "profile.default_content_setting_values.automatic_downloads": 1,
            }
            chrome_options.add_experimental_option("prefs", prefs)

            # Suppress ChromeDriver output
            service = Service(ChromeDriverManager().install(), log_output=os.devnull)

            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.download_path = download_dir  # Save it for use elsewhere
            logger.info("Chrome WebDriver initialized.")
        except Exception as error:
            logger.error(f"Error While Setting Up The Selenium Driver: {error}")

    def open_login_page(self):
        logger.info(f"Navigating to: {self.url}")
        try:
            self.driver.get(self.url)
            time.sleep(2)  # wait for full load

            if "login" in self.driver.current_url.lower():
                logger.info("Login page loaded successfully.")
                return True
            else:
                logger.warning("Unexpected page loaded.")
                return False
        except Exception as error:
            sentry_message = f"Error While Navigating To The Login Page: {error}"
            logger.error(sentry_message)
            log_exception(sentry_message,error)
            return False

    def fill_login_form(self):
        logger.info("🔐 Locating & Filling in login form...")

        try:
            # Wait for email field
            email_input = WebDriverWait(self.driver, 15).until(
                EC.visibility_of_element_located(
                    (By.XPATH, "(//input[contains(@name,'email')])[1]")
                )
            )
            if email_input:
                logger.info("Email Input Field Located.")
            else:
                logger.error(f"Error Locating Email Input Field:{email_input}")

            password_input = WebDriverWait(self.driver, 15).until(
                EC.visibility_of_element_located(
                    (By.XPATH, "(//input[contains(@name,'password')])[1]")
                )
            )
            if password_input:
                logger.info("Password Input Field Located.")
            else:
                logger.error(f"Error Locating Password Input Field:{email_input}")

            password = Config.PASSWORD
            email = Config.EMAIL

            email_input.send_keys(email)
            logger.info("Email Input Field Successfully Filled.")
            time.sleep(3)
            password_input.send_keys(password)
            logger.info("Password input Filed Successfully Filled.")

        except Exception as e:
            sentry_message = f"❌ Failed to log in: {e}"
            logger.error(sentry_message)
            log_exception(sentry_message,e)

    def perform_login(self):
        logger.info("Attempting to Perform Login To Fresh Leads Pick.")
        try:
            login_button = WebDriverWait(self.driver, 15).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//button[contains(@class,'Login__Button')]")
                )
            )
            if login_button:
                logger.info("Login Button located and ready to be clicked.")
            else:
                logger.error(f"Login Button not ready to be clicked:{login_button}")
            login_button.click()
            logger.info("Login Button Clicked Successfully.")
            time.sleep(10)
        except Exception as error:
            logger.error(f"Error While Attempting to Perform Login:{error}")

    def get_custom_dates(self):
        logger.info("Getting Today and yesterday dates.")
        try:
            today = datetime.today()
            yesterday = today - timedelta(days=1)

            yesterday_formatted = yesterday.strftime("%m%d")  # e.g., "0805"
            today_day_only = today.strftime("%d")  # e.g., "06"
            logger.info(
                f"Today Date:{today_day_only} and Yesterday :{yesterday_formatted}"
            )
            return yesterday_formatted, today_day_only
        except Exception as error:
            logger.error(f"Error while Getting Today and Yesterday Dates:{error}")
            return False

    def set_date_range(self, yesterday, today):
        wait = WebDriverWait(self.driver, 10)

        # Target start date input
        start_input = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//input[contains(@name,'start')]"))
        )
        if start_input:
            logger.info("Start Date Field Found.")
            start_input.send_keys(yesterday)
            logger.info("Start Date Filled.")
        else:
            logger.error(f"Start Date Field Not Found:{start_input}")
        time.sleep(4)

        # Target end date input
        end_input = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//input[contains(@name,'end')]"))
        )
        if end_input:
            logger.info("End Date Field Found.")
            end_input.send_keys(today)
            logger.info("End Date Filled.")
        else:
            logger.error(f"End Date Filed Not Found:{end_input}")

        logger.info("✅ Date range set")

    def fetch_leads(self):
        logger.info("Trying To locate the fetch Leads Button.")
        try:
            fetch_button_xpath = "//button[contains(text(),'Fetch Leads')]"
            fetch_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, fetch_button_xpath))
            )
            if fetch_button:
                logger.info("Fetch button Found.")
                fetch_button.click()
                logger.info("Fetch Leads Button Clicked.")
            else:
                logger.error(f"Fetch Button not Found:{fetch_button}")
        except Exception as error:
            logger.error(f"Error While Fetching Leads:{error}")

    def dowload_leads(self):
        logger.info("Downloading The Fetched Leads.")
        try:
            download_button_xpath = "//button[contains(text(),'Download')]"
            if download_button_xpath:
                logger.info("Download Button Found Clciking it Now.")
                download_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, download_button_xpath))
                )
                download_button.click()
                logger.info("Download Button Clicked.")
                logger.info("SUCCESSFULLY DOWNLOADED ALL 04 CSV FILES.")
            else:
                logger.error(f"Failed to click Download Button:{download_button}")
        except Exception as error:
            logger.error(f"Error While Downloading The Fetched Leads:{error}")

    def quit(self):
        if self.driver:
            time.sleep(10)
            self.driver.quit()
            logger.info("Browser closed.")


def main():
    STATUS = True
    bot = FreshPickedLeadsBot(headless=False)  # Set to True to run headless
    try:
        login_page_status = bot.open_login_page()
        if not login_page_status:
            logger.error("Login Page Did Not Open. Closing The Scraper.")
            STATUS = False
            return STATUS

        bot.fill_login_form()
        bot.perform_login()
        yesterday, today = bot.get_custom_dates()
        # yesterday = "0808"
        # today = "09"
        bot.set_date_range(yesterday, today)
        bot.fetch_leads()
        time.sleep(2)
        bot.dowload_leads()

    finally:
        bot.quit()

    # ✅ Step 2: Upload to BigQuery after File Downloading Done...
    logger.info(
        "📦 Starting BigQuery upload process after Downloading The CSV Files..."
    )

    download_path = os.path.dirname(os.path.abspath(__file__))
    credentials_path = os.path.join(
        download_path, "wholesaling-data-warehouse-cd2929689ac2.json"
    )

    uploader = BigQueryUploader(
        project_id=Config.PROJECT_ID,
        dataset_id=Config.DATASET_ID,
        download_path=download_path,
        credentials_path=credentials_path,
    )

    uploader.upload_all_csvs()

    logger.info("✅ FreshPickedLeads automation completed end-to-end.")
