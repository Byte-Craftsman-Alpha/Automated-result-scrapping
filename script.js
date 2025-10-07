from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import sqlite3
import json
import time
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from typing import Dict, List, Optional, Tuple
import hashlib
from contextlib import contextmanager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraping.log'),
        logging.StreamHandler()
    ]
)

class OptimizedScraper:
    def __init__(self, headless=True, db_path='student_results.db', max_workers=4):
        self.db_path = db_path
        self.headless = headless
        self.max_workers = max_workers
        self.lock = threading.Lock()
        self.create_database()
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0
        }
        
    def create_database(self):
        """Create all necessary database tables with proper schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Students table
        cursor.execute('''CREATE TABLE IF NOT EXISTS STUDENTS (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            NAME VARCHAR(100), 
            STUDENT_TYPE VARCHAR(20), 
            FATHER_NAME VARCHAR(100),
            ROLL_NO VARCHAR(20) UNIQUE,
            MOTHER_NAME VARCHAR(100),
            ENROLLMENT_NO VARCHAR(20),
            UNIVERSITY_COLLEGE_NAME VARCHAR(200),
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')

        # Subject results table with proper foreign key
        cursor.execute('''CREATE TABLE IF NOT EXISTS SUBJECT_RESULT (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            STUDENT_ID INTEGER,
            ROLL_NO VARCHAR(20),
            COURSE_CODE VARCHAR(20),
            COURSE_TITLE VARCHAR(200),
            EXTERNAL_THEORY INTEGER,
            INTERNAL_THEORY INTEGER,
            INTERNAL_PRACTICAL INTEGER,
            EXTERNAL_PRACTICAL INTEGER,
            OBTAINED_MARKS INTEGER,
            TOTAL_CREDIT INTEGER,
            GRADE_AWARDED VARCHAR(10),
            GRADE_POINT REAL,
            SEMESTER INTEGER,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (STUDENT_ID) REFERENCES STUDENTS(ID)
        )''')
        
        # Scraping status table for tracking
        cursor.execute('''CREATE TABLE IF NOT EXISTS SCRAPING_STATUS (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            ROLL_NO VARCHAR(20) UNIQUE,
            STATUS VARCHAR(20),
            ERROR_MESSAGE TEXT,
            ATTEMPTS INTEGER DEFAULT 0,
            LAST_ATTEMPT TIMESTAMP,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_roll_no ON STUDENTS(ROLL_NO)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_student_id ON SUBJECT_RESULT(STUDENT_ID)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON SCRAPING_STATUS(STATUS)')
        
        conn.commit()
        conn.close()
    
    @contextmanager
    def get_db_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def get_driver(self) -> webdriver.Chrome:
        """Initialize Chrome driver with optimized settings"""
        options = Options()
        if self.headless:
            options.add_argument('--headless')
        
        # Performance optimizations
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-features=VizDisplayCompositor')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        
        # Disable images for faster loading
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)
        
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(20)
        driver.implicitly_wait(5)
        return driver
    
    def convert_grade_to_point(self, grade: str) -> float:
        """Convert letter grade to grade points"""
        grade_map = {
            'A+': 10.0, 'A': 9.0, 'B+': 8.0, 'B': 7.0,
            'C+': 6.0, 'C': 5.0, 'D': 4.0, 'F': 0.0
        }
        return grade_map.get(grade.upper(), 0.0)
    
    def clean_value(self, value: str) -> Optional[int]:
        """Clean and convert string values to integers, handling '---' as NULL"""
        if value is None or value == '---' or value.strip() == '':
            return None
        try:
            return int(value.strip())
        except (ValueError, AttributeError):
            return None
    
    def validate_result_data(self, result_data: Dict) -> bool:
        """Validate the scraped result data"""
        if not result_data:
            return False
        
        student = result_data.get('student', {})
        subjects = result_data.get('result', [])
        
        # Check if essential student data exists
        if not student.get('roll_no') or not student.get('name'):
            return False
        
        # Check if at least one subject exists
        if not subjects or len(subjects) == 0:
            return False
        
        return True
    
    def insert_student_data(self, roll_no: str, result_data: Dict, semester: int = 2) -> bool:
        """Insert or update student and subject data with proper error handling"""
        if not self.validate_result_data(result_data):
            logging.error(f"Invalid result data for {roll_no}")
            self.update_scraping_status(roll_no, 'invalid_data', 'Result data validation failed')
            return False
        
        with self.lock:
            with self.get_db_connection() as conn:
                cursor = conn.cursor()
                
                try:
                    # Begin transaction
                    conn.execute('BEGIN TRANSACTION')
                    
                    student = result_data.get('student', {})
                    subjects = result_data.get('result', [])
                    
                    # Check if student exists
                    cursor.execute('SELECT ID FROM STUDENTS WHERE ROLL_NO = ?', (roll_no,))
                    existing = cursor.fetchone()
                    
                    if existing:
                        student_id = existing['ID']
                        # Update student information
                        cursor.execute('''
                            UPDATE STUDENTS SET 
                                NAME = ?, STUDENT_TYPE = ?, FATHER_NAME = ?,
                                MOTHER_NAME = ?, ENROLLMENT_NO = ?, 
                                UNIVERSITY_COLLEGE_NAME = ?, UPDATED_AT = CURRENT_TIMESTAMP
                            WHERE ID = ?
                        ''', (
                            student.get('name'),
                            student.get('student_type'),
                            student.get('father_name'),
                            student.get('mother_name'),
                            student.get('enrollment_no'),
                            student.get('university_college_name'),
                            student_id
                        ))
                        logging.info(f"Updated existing student: {roll_no}")
                    else:
                        # Insert new student
                        cursor.execute('''
                            INSERT INTO STUDENTS (
                                NAME, STUDENT_TYPE, FATHER_NAME, ROLL_NO,
                                MOTHER_NAME, ENROLLMENT_NO, UNIVERSITY_COLLEGE_NAME
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            student.get('name'),
                            student.get('student_type'),
                            student.get('father_name'),
                            roll_no,
                            student.get('mother_name'),
                            student.get('enrollment_no'),
                            student.get('university_college_name')
                        ))
                        student_id = cursor.lastrowid
                        logging.info(f"Inserted new student: {roll_no}")
                    
                    # Delete existing subject results for this semester
                    cursor.execute('''
                        DELETE FROM SUBJECT_RESULT 
                        WHERE STUDENT_ID = ? AND SEMESTER = ?
                    ''', (student_id, semester))
                    
                    # Insert subject results
                    subjects_inserted = 0
                    for subject in subjects:
                        if not subject or not subject.get('course_code'):
                            continue
                        
                        # Clean and prepare data
                        external_theory = self.clean_value(subject.get('ext_theory'))
                        internal_theory = self.clean_value(subject.get('int_theory'))
                        internal_practical = self.clean_value(subject.get('int_pract'))
                        external_practical = self.clean_value(subject.get('ext_pract'))
                        obtained_marks = self.clean_value(subject.get('obt_marks'))
                        total_credit = self.clean_value(subject.get('tot_credit'))
                        grade_awarded = subject.get('grade_awarded', '').strip()
                        grade_point = self.convert_grade_to_point(grade_awarded)
                        
                        cursor.execute('''
                            INSERT INTO SUBJECT_RESULT (
                                STUDENT_ID, ROLL_NO, COURSE_CODE, COURSE_TITLE,
                                EXTERNAL_THEORY, INTERNAL_THEORY, INTERNAL_PRACTICAL,
                                EXTERNAL_PRACTICAL, OBTAINED_MARKS, TOTAL_CREDIT,
                                GRADE_AWARDED, GRADE_POINT, SEMESTER
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            student_id, roll_no,
                            subject.get('course_code'),
                            subject.get('course_title'),
                            external_theory, internal_theory,
                            internal_practical, external_practical,
                            obtained_marks, total_credit,
                            grade_awarded, grade_point, semester
                        ))
                        subjects_inserted += 1
                    
                    # Update scraping status
                    cursor.execute('''
                        INSERT OR REPLACE INTO SCRAPING_STATUS (
                            ROLL_NO, STATUS, ERROR_MESSAGE, ATTEMPTS, LAST_ATTEMPT
                        ) VALUES (?, 'success', NULL, 
                                  COALESCE((SELECT ATTEMPTS + 1 FROM SCRAPING_STATUS WHERE ROLL_NO = ?), 1),
                                  CURRENT_TIMESTAMP)
                    ''', (roll_no, roll_no))
                    
                    # Commit transaction
                    conn.commit()
                    
                    logging.info(f"Successfully stored data for {roll_no}: {subjects_inserted} subjects")
                    self.stats['success'] += 1
                    return True
                    
                except Exception as e:
                    # Rollback on error
                    conn.rollback()
                    logging.error(f"Database error for {roll_no}: {e}")
                    self.update_scraping_status(roll_no, 'error', str(e))
                    self.stats['failed'] += 1
                    return False
    
    def update_scraping_status(self, roll_no: str, status: str, error_msg: Optional[str] = None):
        """Update the scraping status for a roll number"""
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO SCRAPING_STATUS (
                    ROLL_NO, STATUS, ERROR_MESSAGE, ATTEMPTS, LAST_ATTEMPT
                ) VALUES (?, ?, ?, 
                          COALESCE((SELECT ATTEMPTS + 1 FROM SCRAPING_STATUS WHERE ROLL_NO = ?), 1),
                          CURRENT_TIMESTAMP)
            ''', (roll_no, status, error_msg, roll_no))
            conn.commit()
    
    def check_already_scraped(self, roll_no: str) -> bool:
        """Check if a roll number has already been successfully scraped"""
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT STATUS FROM SCRAPING_STATUS WHERE ROLL_NO = ? AND STATUS = 'success'
            ''', (roll_no,))
            return cursor.fetchone() is not None
    
    def scrape_single_result(self, driver: webdriver.Chrome, roll_no: str, dob: str, semester: str = "2") -> Optional[Dict]:
        """Scrape result for a single student with improved error handling"""
        url = r'https://ddugorakhpur.com/result2023/searchresult_new.aspx'
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                driver.get(url)
                wait = WebDriverWait(driver, 10)
                
                # Select semester
                semester_dropdown = wait.until(
                    EC.presence_of_element_located((By.ID, "ddlsem"))
                )
                Select(semester_dropdown).select_by_value(semester)
                
                # Enter roll number
                roll_input = wait.until(
                    EC.presence_of_element_located((By.ID, "txtRollno"))
                )
                roll_input.clear()
                roll_input.send_keys(roll_no)
                
                # Enter DOB
                dob_input = driver.find_element(By.ID, "txtDob")
                dob_input.clear()
                dob_input.send_keys(dob)
                
                # Click submit
                submit_btn = driver.find_element(By.ID, "btnSearch")
                driver.execute_script("arguments[0].click();", submit_btn)
                
                # Wait for results to load
                time.sleep(2)
                
                # Check for error messages
                try:
                    error_element = driver.find_element(By.ID, "lblMsg")
                    if error_element and error_element.text:
                        logging.warning(f"Error message for {roll_no}: {error_element.text}")
                        return None
                except NoSuchElementException:
                    pass
                
                # Enhanced JavaScript for data extraction
                scrap_result_js = """
                function getResult() {
                    try {
                        var result = [];
                        var tables = document.querySelectorAll('table');
                        if (tables.length < 4) return [];
                        
                        var table = tables[3];
                        var subjectRows = table.querySelectorAll('tr');
                        
                        for (var i = 1; i < subjectRows.length; i++) {
                            var cells = subjectRows[i].querySelectorAll('td');
                            if (cells.length >= 9) {
                                var subjectResult = {
                                    course_code: cells[0].innerText.trim(),
                                    course_title: cells[1].innerText.trim(),
                                    ext_theory: cells[2].innerText.trim(),
                                    int_theory: cells[3].innerText.trim(),
                                    int_pract: cells[4].innerText.trim(),
                                    ext_pract: cells[5].innerText.trim(),
                                    obt_marks: cells[6].innerText.trim(),
                                    tot_credit: cells[7].innerText.trim(),
                                    grade_awarded: cells[8].innerText.trim()
                                };
                                result.push(subjectResult);
                            }
                        }
                        return result;
                    } catch (e) {
                        console.error('Error in getResult:', e);
                        return [];
                    }
                }

                function getStudentDetails() {
                    try {
                        var tables = document.querySelectorAll('table');
                        if (tables.length < 3) return {};
                        
                        var table = tables[2];
                        var cells = table.querySelectorAll("td span");
                        
                        if (cells.length >= 7) {
                            return {
                                name: cells[0].innerText.trim(),
                                student_type: cells[1].innerText.trim(),
                                father_name: cells[2].innerText.trim(),
                                roll_no: cells[3].innerText.trim(),
                                mother_name: cells[4].innerText.trim(),
                                enrollment_no: cells[5].innerText.trim(),
                                university_college_name: cells[6].innerText.trim()
                            };
                        }
                        return {};
                    } catch (e) {
                        console.error('Error in getStudentDetails:', e);
                        return {};
                    }
                }

                function scrap() {
                    return {
                        result: getResult(),
                        student: getStudentDetails()
                    };
                }
                
                return scrap();
                """
                
                result = driver.execute_script(scrap_result_js)
                
                if result and result.get('student') and result.get('result'):
                    # Store the result with semester information
                    if self.insert_student_data(roll_no, result, int(semester)):
                        return result
                    else:
                        logging.warning(f"Failed to store data for {roll_no}")
                else:
                    logging.warning(f"No valid result found for {roll_no}, attempt {attempt + 1}")
                    
            except TimeoutException:
                logging.error(f"Timeout for {roll_no}, attempt {attempt + 1}")
            except Exception as e:
                logging.error(f"Error scraping {roll_no}, attempt {attempt + 1}: {e}")
            
            if attempt < max_retries - 1:
                time.sleep(2)  # Brief pause between retries
        
        return None
    
    def scrape_batch(self, students: List[Tuple[str, str]], semester: str = "2", skip_existing: bool = True):
        """Scrape results for a batch of students using parallel processing"""
        self.stats['total'] = len(students)
        
        def worker(student_data: Tuple[str, str]) -> bool:
            roll_no, dob = student_data
            
            # Skip if already scraped
            if skip_existing and self.check_already_scraped(roll_no):
                logging.info(f"Skipping {roll_no} - already scraped")
                self.stats['skipped'] += 1
                return True
            
            driver = None
            try:
                driver = self.get_driver()
                result = self.scrape_single_result(driver, roll_no, dob, semester)
                return result is not None
            except Exception as e:
                logging.error(f"Worker error for {roll_no}: {e}")
                self.stats['failed'] += 1
                return False
            finally:
                if driver:
                    driver.quit()
        
        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(worker, student): student for student in students}
            
            for future in as_completed(futures):
                student = futures[future]
                try:
                    success = future.result()
                    if success:
                        logging.info(f"Completed: {student[0]}")
                except Exception as e:
                    logging.error(f"Future error for {student[0]}: {e}")
        
        # Print statistics
        self.print_statistics()
    
    def print_statistics(self):
        """Print scraping statistics"""
        print("\n" + "="*50)
        print("SCRAPING STATISTICS")
        print("="*50)
        print(f"Total Students: {self.stats['total']}")
        print(f"Successfully Scraped: {self.stats['success']}")
        print(f"Failed: {self.stats['failed']}")
        print(f"Skipped (Already Exists): {self.stats['skipped']}")
        print(f"Success Rate: {(self.stats['success']/max(self.stats['total'], 1))*100:.2f}%")
        print("="*50)
    
    def get_failed_records(self) -> List[str]:
        """Get list of roll numbers that failed to scrape"""
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT ROLL_NO FROM SCRAPING_STATUS 
                WHERE STATUS IN ('error', 'invalid_data')
            ''')
            return [row['ROLL_NO'] for row in cursor.fetchall()]
    
    def export_to_csv(self, output_file: str = 'results.csv'):
        """Export results to CSV file"""
        import csv
        
        with self.get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get all student results with subjects
            cursor.execute('''
                SELECT 
                    s.ROLL_NO, s.NAME, s.FATHER_NAME, s.MOTHER_NAME,
                    s.ENROLLMENT_NO, s.STUDENT_TYPE, s.UNIVERSITY_COLLEGE_NAME,
                    sr.COURSE_CODE, sr.COURSE_TITLE, sr.EXTERNAL_THEORY,
                    sr.INTERNAL_THEORY, sr.INTERNAL_PRACTICAL, sr.EXTERNAL_PRACTICAL,
                    sr.OBTAINED_MARKS, sr.TOTAL_CREDIT, sr.GRADE_AWARDED, sr.GRADE_POINT
                FROM STUDENTS s
                JOIN SUBJECT_RESULT sr ON s.ID = sr.STUDENT_ID
                ORDER BY s.ROLL_NO, sr.COURSE_CODE
            ''')
            
            rows = cursor.fetchall()
            
            if rows:
                with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = [description[0] for description in cursor.description]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    writer.writeheader()
                    for row in rows:
                        writer.writerow(dict(row))
                
                logging.info(f"Exported {len(rows)} records to {output_file}")
            else:
                logging.warning("No records to export")


# Example usage
if __name__ == "__main__":
    # Initialize scraper
    scraper = OptimizedScraper(headless=True, max_workers=4)
    
    # Example student list (roll_no, dob)
    students = [
        ("2514670010038", "05/11/2006"),
        # Add more students here
    ]
    
    # Scrape batch
    scraper.scrape_batch(students, semester="2", skip_existing=True)
    
    # Export results to CSV
    scraper.export_to_csv("semester2_results.csv")
    
    # Get failed records for retry
    failed = scraper.get_failed_records()
    if failed:
        print(f"\nFailed roll numbers: {failed}")