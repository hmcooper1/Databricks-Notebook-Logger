# Databricks Notebook Logger & Audit Trail System

A comprehensive logging system for Databricks notebooks that creates **SAS-style audit trails** for regulatory compliance and validation. This tool generates detailed execution logs and HTML snapshots of your notebooks, then transfers them securely via SFTP to wherever you need them stored. See `demo/demo_notebook` and `demo/demo_notebook.log` for a full example of this utilized in a notebook with further explanations.

## Why This Tool?

In regulated industries (biotechnology, finance, healthcare), regulatory organizations require proof that analytical results were generated from validated code. Traditional SAS programs provide this through detailed log files that are automatically generated when the code runs. However, SAS is expensive, proprietary, and many organizations are moving toward open-source alternatives. 

Databricks is traditionally used for exploratory analysis and interactive development, but it lacks the automatic audit logging that regulatory work requires. This tool brings SAS-style audit trails to Databricks, making it a viable platform for validated analytic workflows with regulatory compliance.

**Key Benefits:**
* **Regulatory Compliance**: Creates a complete audit trail showing exactly what code was executed, when, by whom, and what results were produced
* **Reproducibility**: Captures all metadata (Python/Spark versions, cluster info, timestamps) needed to reproduce results
* **Secure Artifact Management**: Automatically transfers log files and HTML notebook snapshots to your secure project repository via SFTP
* **SAS-style Documentation**: Mimics familiar SAS log format that regulatory reviewers expect to see


## What Gets Captured

The logging system creates a comprehensive record of your notebook execution:
* **Metadata**: User, timestamp, cluster configuration, Python/Spark versions
* **All executed Python code**: Every code cell that runs in Python and it's output
* **SQL queries**: All queries executed via `spark.sql()` with their results
* **Complete outputs**: Results from `print()`, `df.show()`, and table displays
* **Warnings and errors**: Full error tracebacks and warning messages
* **Performance metrics**: Runtime per cell and total execution time

**Secure Delivery**: Both the text log and HTML notebook snapshot are automatically uploaded to your specified location via SFTP

Note: for a visual overview of how the logging system works on the backend, refer to *[databricks_log_diagram.pdf](databricks_log_diagram.pdf)*. The PDF may not render clearly in GitLab, so you will need to download it to zoom in


## Quick Start

Follow these steps to enable logging in your notebook:

#### 1. Set Notebook Language to Python

Before adding any code, ensure your notebook is set to use Python as the default language — this can be changed in the language dropdown at the top left of the notebook.

#### 2. Import the Logging Module

Add this code to the **first cell** of your notebook:

```python
%pip install notebook-logger
from notebook_logger import *
```

#### 3. Start Logging

In the **second cell**, initialize the logger:

```python
start_logging(output_directory="path/to/output/directory")
```

The `output_directory` filepath is the location where your audit trail artifacts (log file and HTML notebook snapshot) will be uploaded via SFTP.

When prompted:
* Enter your SFTP host and port
* Enter your SFTP credentials
* Optionally save your credentials for the cluster session duration

This establishes the secure connection and begins capturing all cells for your audit trail. If you do not specify an `output_directory`, SFTP upload will be skipped.

#### 4. Add Your Code

Place all your notebook code in the cells following `start_logging()`. Everything will be automatically logged, including:
* Python code
* SQL queries executed using `spark.sql("INSERT SQL QUERY HERE")`
* Output from SQL queries displayed using `spark.sql("INSERT SQL QUERY HERE").show(truncate=False)`
* Spark tables displayed using `log_df()` (see examples of this in the **Logging SQL...** section below)
* Output from `print()` statements
* DataFrame displays using `df.show()`
* Warnings and errors

#### 5. Stop Logging

In the **last cell** of your notebook, finalize the log:

```python
stop_logging()
```

This will:
* Generate a complete audit log file in your Workspace directory (e.g., `notebook_name.log`)
* Securely transfer both the log file and HTML notebook snapshot to your designated storage location via SFTP
* Display an execution summary including total runtime, warnings, and confirmation of successful artifact delivery

#### 6. Run Your Notebook

Click **Run all** to execute the entire notebook. After completion, your audit trail artifacts will be available in:
* Your Workspace directory (same location as your notebook)
* Your secure storage location (uploaded via SFTP)

Note: If your notebook encounters an error during execution, the error is automatically recorded in the log file, and all artifacts are still securely transferred to ensure a complete audit trail even for failed runs.


## Logging SQL Queries, SQL Output, and Spark Tables

The logging system displays SQL queries and SQL outputs in the log file using `spark.sql()`. A helper function `log_df` is available to display Spark tables in the log file. See example usage below:

**SQL Queries: Display SQL query in the log file**
```python
spark.sql("""
INSERT SQL QUERY HERE
""")
```
Note: the output of the SQL query will not be shown - see below for displaying the output.

**SQL Output: Display SQL query AND output in the log file**
```python
spark.sql("""
INSERT SQL QUERY HERE
""").show(truncate=False)
```
Note: it is good practice to include `truncate=False` to ensure none of the output values are cut off.

**Spark table: Display the contents of a Spark table in the log file**
```python
log_df("schema.table_name", limit=10)
```
Note: see the demo notebook for examples.


## Example Output

When you call `start_logging()`, in the log you'll see something like:
```
====================================================================================================
user                 : user@example.com
run start timestamp  : 2026-02-18T22:05:09+00:00
notebook             : /Repos/user@example.com/your_notebook
log file             : /Repos/user@example.com/your_notebook.log
cluster id           : cluster_id
python               : 3.10.12
spark                : 3.5.0
====================================================================================================
```

When you call `stop_logging()`, in the log you'll see something like:
```
====================================================================================================
NOTEBOOK RAN SUCCESSFULLY WITH NO ERRORS
run end timestamp    : 2026-02-18T22:05:56+00:00
total runtime        : 47.25 seconds
====================================================================================================
```

If your notebook errors before `stop_logging()` is called, in the log you'll see something like:
```
====================================================================================================
NOTEBOOK DID NOT SUCCESSFULLY RUN DUE TO ERROR
ERROR: ValueError: This is a test error
run end timestamp    : 2026-02-18T23:13:24+00:00
total runtime        : 2.79 seconds
====================================================================================================
```


## Limitations

* **Markdown cells**: Cannot be captured in the log (not executed by the Python kernel)
* **Cell titles**: Not included in the log (part of the notebook interface, not executable code)