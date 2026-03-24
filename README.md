# FIrst-ETL-Proyect
# Excel to MySQL ETL Pipeline

![Python Version](https://img.shields.io/badge/python-3.8+-blue)
![Status](https://img.shields.io/badge/status-production-green)
![MySQL](https://img.shields.io/badge/mysql-8.0+-orange)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

##  About This Project

This is an ETL (Extract, Transform, Load) project made with Python. It helps you move data from an Excel file to a MySQL database. The project was made for a real estate team. It cleans the data and prepares it so you can use it in MySQL Workbench.

###  What It Does

This tool helps you:
- Take data from an Excel file
- Clean the data (remove spaces, fix dates, etc.)
- Save the data to a MySQL database
- Make the data ready for SQL queries

###  Types of Data
- **Text**: Addresses, names, descriptions
- **Numbers**: Prices, areas, quantities
- **IDs**: Property IDs, transaction numbers

---

##  Project Structure
proyecto_etl/
- config.py #MySQL connection settings
- scripts/ #ETL code
  - extract.py #Read Excel file
  - transform.py #Clean and fix data
  - load.py #Save to MySQL
  - main.py #Run everything
- datos/ #Data files
  - bienes_raices_Ventas.xls #Original Excel file
  - ventas_limpio.csv #Clean data (created by the tool)
- pycache/ #Python cache (auto-generated)


---

##  What You Need

### Software

| Tool | Version | What It Does |
|------|---------|--------------|
| Python | 3.8+ | Runs the code |
| MySQL Server | 8.0+ | Stores the data |
| MySQL Workbench | 8.0+ | Lets you see and query the data |

### Python Packages

Open your terminal and run:

```bash
pip install pandas==2.0.3
pip install mysql-connector-python==8.1.0
pip install openpyxl==3.1.2
```

## Set up Instructions

### Configure MySQL Conecction
Open confing.py and change the settings:
```bash
# config.py
DB_CONFIG = {
    'host': 'localhost',           # Your MySQL server
    'port': 3306,                  # Default MySQL port
    'user': 'root',                # Your MySQL username
    'password': 'your_password',   # Your MySQL password
    'database': 'bienes_raices'    # Database name
}
```
### Create the Database
Open MySQL Workbench and run 
``` sql
-- Run this in MySQL Workbench
CREATE DATABASE IF NOT EXISTS bienes_raices;
USE bienes_raices;
```
### How it works
#### Simple flow
Excel File → Clean Data → MySQL Database
### Step by Step
#### Step 1: Extract (scripts/extract.py)
- Reads the Excel file
- Loads data into Python (pandas DataFrame)
- Shows how many records were found
#### Step 2: Transform (scripts/transform.py)
- Cleans column names (lowercase, no spaces)
- Fixes data types (text, numbers, dates)
- Removes empty values
- Removes duplicate records
- Saves a clean CSV file
#### Step 3: Load (scripts/load.py)
- Connects to MySQL
- Creates a table (if it doesn't exist)
- Inserts the clean data
- Shows how many records were saved
#### Step 4: Main (scripts/etl_completo.py)
- Runs all steps in order
- Shows progress messages
- Stops if something goes wrong

### How to run
#### Run the ETL Process
``` bash
# 1. Go to the project folder
cd proyecto_etl

# 2. Check that the Excel file exists
ls datos/bienes_raices_Ventas.xls

# 3. Update config.py with your MySQL info

# 4. Run the ETL
python scripts/main.py
```
#### What you will see
``` bash
==================================================
      STARTING ETL PROCESS
==================================================

[1/3] EXTRACT: Reading Excel file...
      ✓ File loaded: bienes_raices_Ventas.xls
      ✓ Records found: 1,234

[2/3] TRANSFORM: Cleaning data...
      ✓ Column names fixed
      ✓ Data types converted
      ✓ Empty values handled
      ✓ Duplicates removed: 45
      ✓ Final records: 1,189
      ✓ File saved: datos/ventas_limpio.csv

[3/3] LOAD: Saving to MySQL...
      ✓ Connected to MySQL (localhost:3306)
      ✓ Table 'ventas' ready
      ✓ Records inserted: 1,189
      ✓ Records failed: 0

==================================================
      ETL PROCESS COMPLETED
==================================================
Total time: 3.45 seconds
```

### Check your data in MySQL Workbench
After running the ETL, open MySQL workbench and try these queries:
#### Basic Checks
``` sql
-- See all tables
SHOW TABLES;

-- See the table structure
DESCRIBE ventas;

-- See the first 10 records
SELECT * FROM ventas LIMIT 10;

-- Count all records
SELECT COUNT(*) FROM ventas;
```
#### Example Queries
``` sql
-- Sales by property type
SELECT 
    tipo_propiedad,
    COUNT(*) AS total_sales,
    AVG(precio_venta) AS average_price
FROM ventas
GROUP BY tipo_propiedad;

-- Sales by month
SELECT 
    DATE_FORMAT(fecha_venta, '%Y-%m') AS month,
    COUNT(*) AS sales_count,
    SUM(precio_venta) AS total_sales
FROM ventas
WHERE fecha_venta IS NOT NULL
GROUP BY month
ORDER BY month DESC;

-- Top 5 sellers
SELECT 
    vendedor,
    COUNT(*) AS sales_made,
    SUM(precio_venta) AS total_sold
FROM ventas
WHERE vendedor IS NOT NULL
GROUP BY vendedor
ORDER BY total_sold DESC
LIMIT 5;
```

##  Common Problems and Solutions

| Problem | Likely Cause | Solution |
|---------|--------------|----------|
| `ModuleNotFoundError: No module named 'pandas'` | Python packages not installed | Run: `pip install pandas mysql-connector-python openpyxl` |
| `FileNotFoundError: [Errno 2] No such file or directory: 'bienes_raices_Ventas.xls'` | Excel file is missing or in wrong location | Check that `bienes_raices_Ventas.xls` is inside the `datos/` folder |
| `mysql.connector.errors.ProgrammingError: 1049 (42000): Unknown database 'bienes_raices'` | Database does not exist | Create the database in MySQL Workbench: `CREATE DATABASE bienes_raices;` |
| `mysql.connector.errors.ProgrammingError: 1045 (28000): Access denied for user 'root'@'localhost'` | Wrong username or password | Check your MySQL credentials in `config.py` |
| `mysql.connector.errors.InterfaceError: 2003: Can't connect to MySQL server` | MySQL is not running | Start MySQL service: `sudo systemctl start mysql` (Linux) or open MySQL Workbench |
| `UnicodeDecodeError: 'utf-8' codec can't decode byte` | File encoding issue | Save Excel file as UTF-8 or add `encoding='latin1'` to pandas `read_excel()` |
| `mysql.connector.errors.DataError: 1265 (01000): Data truncated for column` | Wrong data type for a column | Check that all data matches the column type in your table |
| `KeyError: 'column_name'` | Column name does not match | Check column names in Excel. They should match what the code expects |
| `Empty DataFrame` | Excel file has no data | Check that your Excel file has data and the first row has column names |
| `pandas.errors.ParserError: Error tokenizing data` | CSV file format is wrong | Check that your CSV uses commas (,) and not semicolons (;) as separators |
| `Permission denied: 'ventas_limpio.csv'` | File is open or no write permission | Close the CSV file if it is open in Excel. Check folder permissions |
| `NameError: name 'df' is not defined` | Variable not created | Run the previous step first. Make sure you ran extract before transform |
| `MySQL server has gone away` | Connection timeout or data too large | Increase timeout or load data in smaller batches |
| `Duplicate entry for primary key` | Trying to insert same ID twice | Remove duplicates in transform step or use `INSERT IGNORE` |
| `ImportError: No module named 'mysql'` | MySQL connector not installed | Run: `pip install mysql-connector-python` |

##  Technical Notes

### How the Code is Organized

The project follows a modular structure. Each part of the ETL process has its own file:

| File | Responsibility |
|------|----------------|
| `config.py` | Stores database connection settings |
| `scripts/extract.py` | Reads the Excel file |
| `scripts/transform.py` | Cleans and fixes the data |
| `scripts/load.py` | Saves data to MySQL |
| `scripts/main.py` | Runs all steps in order |

This modular design makes it easy to:
- Test each part separately
- Fix errors in one part without affecting others
- Reuse code for other projects

### Configuration

All settings are in one place: `config.py`. This file contains:

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'your_password',
    'database': 'bienes_raices'
}
```
##  What It Can Do Now

This is a list of all the features that are working in the current version of the project.

### Data Processing Features

| Feature | Description | Status |
|---------|-------------|--------|
| Read Excel files | Reads `.xls` and `.xlsx` files |  Working |
| One sheet only | Processes the first sheet of the Excel file |  Working |
| Handle text data | Reads names, addresses, descriptions |  Working |
| Handle numbers | Reads prices, areas, quantities |  Working |
| Handle IDs | Reads property IDs, transaction numbers |  Working |

### Data Cleaning Features

| Feature | Description | Status |
|---------|-------------|--------|
| Clean column names | Changes "ID Propiedad" to "id_propiedad" |  Working |
| Remove spaces | Removes extra spaces from text |  Working |
| Fix data types | Converts text to numbers and dates |  Working |
| Remove duplicates | Deletes repeated records |  Working |
| Handle empty values | Removes rows with missing data |  Working |
| Save CSV file | Creates `ventas_limpio.csv` in the `datos/` folder |  Working |

### Database Features

| Feature | Description | Status |
|---------|-------------|--------|
| Connect to MySQL | Connects using settings from `config.py` |  Working |
| Create table | Creates a table called `ventas` if it does not exist |  Working |
| Insert data | Adds all records to the MySQL table |  Working |
| Show results | Displays how many records were inserted |  Working |

### Project Structure Features

| Feature | Description | Status |
|---------|-------------|--------|
| Modular code | Extract, transform, and load are in separate files |  Working |
| Central config | All database settings in `config.py` |  Working |
| Console messages | Shows progress while running |  Working |
| Error handling | Shows error messages if something fails |  Working |

---

##  Ideas for the Future

This section lists improvements you can add to make the project better. They are organized by priority. Start with the "High Priority" items first.

---

### High Priority

These improvements are important for security, reliability, and usability.

| Priority | Improvement | Description | Why It Matters |
|----------|-------------|-------------|----------------|
| **High** | Environment Variables | Move database passwords from `config.py` to a `.env` file | Passwords are visible in the code. Anyone with access to the repository can see them. Using a `.env` file keeps secrets safe |
| **High** | Better Error Handling | Add try-except blocks to each step | Currently, if one record fails, the whole process stops. Better error handling would skip bad records and continue with good ones |
| **High** | Data Validation | Check data quality before inserting | Bad data (like text in a number field) can break the load step. Validation catches these problems early |

##  Contact

If you have questions, find a bug, or need help using this project, here is how to reach us.

---

### Who to Contact

| Role | Responsibility | Contact |
|------|----------------|---------|
| **Project Owner** | Overall project management, feature requests, questions | [Luis Eduardo Hernandez Olayo] - [acereroslalo1@gmail.com] ||

---

