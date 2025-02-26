Simple backend API for a data query tool.

# About

Backend using flask with cherrypy or tornado server.

This manages a data cohort, allowing users to make requests of the data. See the `dqt-client` for more info on frontend
use cases.

# Doco

## Setup

1. Create a base directory where logging and configuration information can be stored, and put this in `config.py` file.

        BASE_DIR='path/to/local/storage'  # location for logs, etc.
        SECRET_KEY=b''  # generate this with os.urandom(24), any number is fine
        SQLALCHEMY_DATABASE_URI = 'sqlalchemy-connection-string'  # see # see: http://docs.sqlalchemy.org/en/latest/core/engines.html
        ORIGINS = ['*']  # origins to allow for cors
        # optional
        LOG_KEY=b''  # if you want to encrypt old log files, os.urandom(32)
        MASK = 5  # mask values smaller than this number
        AGE_STEP = 5  # increment for age graph
        AGE_MAX = 90  # max age to show (otherwise inferred from data)
        AGE_MIN = 60  # minimum age to show (otherwise inferred from data)
        COHORT_TITLE = 'ACT'  # short name for population under review

2. Install requirements with `pip install -r requirements.txt`

3. Create database (run `create database data_query_tool;`)

4. Build tables:
    * `python manage.py --method create --config /path/to/config.py`
    * `python manage.py --method createuserdata --config /path/to/config.py`

5. Load data using `load_csv_pandas.py`
    * `load_csv` and `load_csv_async` probably work, but should only be relied on if data is too large to fit in memory
    * This is meant to be a general purpose load script, but it may require some modification on your part
    * You can also auto-fill 100 subjects by using:
        * `python manage.py --method load --count 100 --config /path/to/config.py`

6. Run the application with `python dqt_api --config /path/to/config.py`.
    * Instead of adding `--config`, you can specify the environment variable `FLASK_CONFIG_FILE`

7. Navigate to `http://127.0.0.1:8090` for test page.

8. First request from client will take longer (be patient) as indexes are being built.
    * When reloading data, delete the `dump.pkl` file in `BASE_DIR`

## Data

This tool is intended to provide a useful interface for exploring variables in a cohort.

It will require two input datasets:

* `data_dictionary`: a dataset defining the variables, labels, descriptions, and options
* `cohort`: a dataset with the variables populated for individuals from the cohort

### Data Dictionary

The data dictionary defines the values that can appear in the dataset and which a user can use to filter the data.

Some terminology:

* Domain: a collection of variables; e.g., the domain of 'demographics' may include gender, income, etc. and these
  variables will be grouped together
* Name: the variable name as it appears in the 'cohort' dataset
* Label: the display name for the variable (i.e., what you want it to look like on the website)
* Values: the possible values that are allowed for a vaiable
    * E.g., degree might include options like `GED`, `Bachelor`, `Master`, `Doctorate`, etc.
    * These values may be given an order by adding a number before the options:
        * `0 = GED`
        * `1 = Bachelor`
        * `2 = Master`
        * `3 = Doctorate`
        * This number will also serve as a replacement for the string value in the `cohort` dataset (so we can store `0`
          rather than `GED`)

There are two options for formatting: Excel (xlsx) or a CSV. The Excel format is recommended.

#### Excel Format

Excel has tabs, rows, and columns. Each tab defines a domain and the variables on that tab are within that domain. By
default, Excel labels the tab/domain `Sheet 1` -- change this to, e.g., `Demographics` and add demographic variables
like degree, gender, race, etc.

The first row defines what the columns contain for each variable. Additional columns are allowed and will not be ignored
by the upload process.

Here's an example for the sheet named `Demographics`:

| Name           | Label              | Description                                                           | Values                                                                    | Notes                                         |
|----------------|--------------------|-----------------------------------------------------------------------|---------------------------------------------------------------------------|-----------------------------------------------|
| gender         | Gender             | Gender                                                                | 1 = male<br/>2 = female<br/>. missing                                     | Notes not included in upload.                 |
| degree         | Highest Degree     | Highest degree reported at first visit                                | 0 = GED<br/>1 = Bachelor<br/>2 = Master<br/>3 = Doctorate<br/>. = Missing | Add other options?                            |
| current_status | Enrollment         | Enrollment status within cohort <br/>(e.g., enrolled, deceased, etc.) | 0 = Enrolled<br/>1 = Deceased<br/>2 = Disenrolled                         | Status as of latest update.                   |
| edu_yrs        | Years of Education | Total years of education as of first visit                            | Integers between 0 and 21 years<br/>. = Missing                           | These values will be read as an integer range |

All of these variable labels will appear in the left-hand filter of the web app under their respective domain. Only the values which appear in the dataset will be shown. For example, if no one is deceased, only `Enrolled` and `Disenrolled` options for `Enrollment` will display. Real values will appear as ranges (and the range/step will be inferred from the data).

This can be easily transformed into the `categorization-csv` file required by `load_csv_pandas.py` using:

```commandline
python xlsx_to_csv.py 
--input-file data-dictionary.xlsx
--output-file variable_categorization.csv
--columns-to-keep
Name
Label
Description
Values
--append-sheet-name=Domain
--ignore-empty-rows
--join-multiline=|| 
```
#### CSV Format

The csv format must follow the specifications for the `categorization-csv`:

```
Name,Label,Description,Values,Domain
gender,Gender,Gender,1 = male||2 = female||. = missing,Demographics
degree,Highest Degree,Highest degree reported at first visit,0 = GED||1 = Bachelor||2 = Master||3 = Doctorate||. = Missing,Demographics
current_status,Enrollment,"Enrollment status within cohort (e.g., enrolled, deceased, etc.)",0 = Enrolled||1 = Deceased||2 = Disenrolled,Demographics
edu_yrs,Years of Education,Total years of education as of first visit,"Integers between 0 and 21||. = Missing",Demographics
```

### Cohort

The cohort dataset defines one subject (i.e., patient/individual) per row. The first row is a header row whose names
must match the `name` column in the data dictionary. The cohort file is a CSV file. Extra columns will be ignored.

| subject_id | gender | degree | current_status | edu_yrs | age_bl | age_fu | followup_years | intake_date |
|------------|--------|--------|----------------|---------|--------|--------|----------------|-------------|
| 0          | 1      | 3      | 0              | 21      | 65     | 68     | 3              | 07/21/2013  |
| 1          | 2      | 1      | 2              | 16      | 65     | 92     | 27             | 09/09/1994  |

### Data Model Variables

The web app requires 6 variables to be populated for each individual in the cohort. These are the data elements from
which graphs and tables are constructed. Without them, there is insufficient information to generate the visualizations.

The column `Requires String` provides information on whether or not a string representation is required (e.g., in the
data dictionary).

* yes: this value must be populated in the data dictionary (see [cohort](#cohort), above) or included as a string in the
  cohort dataset (e.g., 'enrolled' rather than '0')
* no: this can just be a number/date in the cohort dataset

| Name             | Label                  | Description                                                                       | `load_csv_pandas` Argument        | Requires String |
|------------------|------------------------|-----------------------------------------------------------------------------------|-----------------------------------|-----------------|
| `age_bl`         | Age at Baseline        | Variable for age at baseline (i.e., cohort entry).                                | `--age-bl age_bl`                 | No              |
| `age_fu`         | Age at Follow-up       | Variable for age at baseline (i.e., cohort entry).                                | `--age-bl age_bl`                 | No              |
| `gender`         | Gender/sex             | Variable for gender or sex.                                                       | `--gender gender`                 | Yes             |
| `enrollment`     | Enrollment Status      | Variable for enrollment status (e.g., currently enrolled? deceased? disenrolled?) | `--enrollment current_status`     | Yes             |
| `followup-years` | Years of followup      | Variable for the number of years of data available for this subject               | `--followup-years followup_years` | No              |

### Masking and Jitter

To enable masking of values (e.g., to avoid identifiability of small counts), ensure that the `MASK = 5` option appears in the `config.py` (see [config](#config) below). If the mask is set to 5, any cell with less than or equal to 5 will be omitted (i.e., set to 0).  

By default, there is a 'jitter' which pseudo-randomly pushes cells up or down (the function is stable for all queries for a day). This can be configured in `config.py` by setting a random string as a sort of seed value. Alternatively, to turn this off, set `JITTER=None`

WARNING: Note that the jitter and masking can appear to cause peculiar results in small datasets. For example, the jitter can cause a query to be masked one day (jitter removes count) and unmasked another day (adding count back). With larger datasets, this has minimal impact. 

## Web Interface

### Enabling Search

After running the `load_csv_pandas.py` script, ensure that the resulting `whooshee.idx` folder is moved to wherever specified by the `WHOOSHEE_DIR` in the `cnofig,py` file.

### Config

Connection and parameter settings are placed in a `config.py` file.

```python
# base directory for logs, etc. to be stored in
BASE_DIR = r'C:\data\config\dqt'
# os.urandom(n)
SECRET_KEY = b'>)r\x01\xc5\xeap q\xa1|\x89\xa8gq\tX\x95\xb3\x8d\xadgA\xf7'
# os.urandom(32)
LOG_KEY = b'6\x1d\xef\n\xd8\x8clT/\xf3h1v\xebfF\xa6\x9f\xc2\xc0-e"\xab\xd9\xcf\x93\xf6\x1f\xad\xe9c'
SQLALCHEMY_DATABASE_URI = r'mssql+pyodbc://SERVER/DATABASE?driver=SQL Server'
AGE_STEP = 5
MASK = 5  # don't show groups with values smaller than this
JITTER = 'random string'  # add to jitter function, set to `JITTER = None` to disable
AGE_MAX = 100  # maximum age to show (everything above will be shown as 100+)
AGE_MIN = 30  # minimum age to show (everything below will be shown as <=30)
ORIGINS = ['*']  # best to supply a specific URL
COHORT_TITLE = 'My Data'
UPDATE_DATE = 'January 2020'
# optional, if you don't want in default location when running app.py; no effect from __main__.py
WHOOSHEE_DIR = r'C:\data\config\dqt\whooshee.idx'
```

### Adding Tabs

By default, the web app has two tabs: a 'login' and the query tool. To add additional columns, the format is:

    TAB_NAME==TAB_INDEX==STYLE==TEXT_TO_DISPLAY

* `TAB_NAME`: title appearing on the tab (e.g., 'FAQ')
* `TAB_INDEX`: integer for the order of the tabs (e.g., 0, 1, 2,  etc.)
* `STYLE`: style to display text in (can be 'header', 'text', or 'bold')
* `TEXT_TO_DISPLAY`: text to actually show

To add newlines (multiple lines), stack these in order:

```
FAQ==2==header==What is the ABC study?
FAQ==2==text==The ABC study is a longitudinal study of...
FAQ==2==header==How do I use the data query tool?
FAQ==2==text==The data query tool provides frequencies of study participants by age...
```
