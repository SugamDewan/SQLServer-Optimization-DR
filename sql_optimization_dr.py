import pyodbc
import time
from datetime import datetime, timedelta
#Connect to SQL Server 
conn = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=WOLF;'
    'DATABASE=OptimizationDB;'
    'UID=sa;'
    'PWD=Sugam@123'
)
cursor = conn.cursor()
# Function to log messages to a file
def log_message(message):
    with open('optimization_log.txt', 'a') as log_file:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_file.write(f"{timestamp} - {message}\n")
    print(message)

# Function to check recent backups
def check_backup_status():
    log_message("Checking SQL Server backup status...")
    query = """
    SELECT TOP 1 database_name, backup_finish_date
    FROM msdb.dbo.backupset
    WHERE type = 'D'  -- Full backup
    ORDER BY backup_finish_date DESC
    """
    cursor.execute(query)
    result = cursor.fetchone()
    if result:
        db_name, last_backup = result
        backup_age = datetime.now() - last_backup
        if backup_age > timedelta(days=1):
            log_message(f"Warning: Last backup for {db_name} was {backup_age.days} days ago on {last_backup}!")
        else:
            log_message(f"Backup for {db_name} is recent: {last_backup}")
    else:
        log_message("No backups found!")

# Function to optimize a query by analyzing execution time
def optimize_query(query):
    log_message(f"Analyzing query performance: {query}")
    start_time = time.time()
    cursor.execute(query)
    rows = cursor.fetchall()
    end_time = time.time()
    execution_time = end_time - start_time
    log_message(f"Query took {execution_time:.2f} seconds. Rows returned: {len(rows)}")
    if execution_time > 1:  # Threshold for optimization
        log_message("Suggestion: This query is slow. Try adding an index or rewriting it.")
    return execution_time

# Main execution
if __name__ == "__main__":
    try:
        # Check backup status
        check_backup_status()

        # Optimize a sample query
        sample_query = "SELECT * FROM TestTable WHERE Name = 'John'"
        optimize_query(sample_query)

    except Exception as e:
        log_message(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()
