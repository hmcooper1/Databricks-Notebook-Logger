### ALL FUNCTIONS TO IMPORT ========================================================================= ###
__all__ = ["start_logging", "stop_logging", "log_df"]

### IMPORTS ========================================================================================= ###
import time, datetime                     # for date and time
import base64, json                       # encode content for Workspace API
import re, io                             # re: regex to ensure safe filename, io: in memory StringIO buffer for log
import platform, pyspark                  # python and spark version in the header
import IPython, sys                       # ipython version in header
import requests, warnings                 # HTTP client to call DB Workspace REST APIs, intercept python warnings
from pathlib import Path                  # path handling (mainly for filenames in warnings)
from IPython import get_ipython           # get access to ipython to register pre/post hooks and access cell history
from IPython.display import HTML, display # to display clickable downloadable links for html and log
import urllib.parse                       # url encode the text for clickable downloadable links
from pyspark.sql import SparkSession      # to access spark session

# import helper functions for SFTP
try:
    from . import sftp as sftp
except Exception:
    sftp = None

### GLOBALS REPRESENTING CURRENT RUN ================================================================ ###
# logger / run state
_is_logging = False                       # keep track if a run is active
_buf = None                               # io.StringIO: in memory text stream that stores all log data
_ip = None                                # reference to IPython shell
_cell_start_time = None                   # timestamp of current cell start (for elapsed seconds)
_cell_start_iso = None                    # human readable timestamp of current cell start
_log_start_time = None                    # timestamp of run start
_timestamp = None                         # run timestamp (start of run)
_current_log_base = None                  # base log name, without extension
_workspace_target_path = None             # '/Users/.../<name>.log'
_auto_stopping = False                    # guarantee you stop writing to a log when auto-stopping (with error)

# trailer and error details
_termination_reason = "OK"                # "OK" or "ERROR"
_last_error_text = None                   # last error line to echo in trailer

# warnings capture
_prev_showwarning = None                  # to save original warning display function (warnings.showwarning)
_cell_warnings = []                       # warnings captured during current cell
_all_warnings = []                        # all warnings across the whole notebook run

# stdout/stderr capture (tee)
_orig_stdout = None                       # original sys.stdout (regular output)
_orig_stderr = None                       # original sys.stderr (error output)
_stdout_tee = None                        # tee object that writes to real stdout + StringIO (log buffer)
_stderr_tee = None                        # tee object that writes to real stdout + StringIO (log buffer)

# globals for sftp
_sftp_creds = None
_sftp_local_path = None
_sftp_remote_dir = None     # str remote dir passed by start_logging(...)

### HELPER FUNCTIONS ================================================================================ ###
def _safe_filename(name: str) -> str:
    """ Replace invalid characters in a Workspace path (spaces, weird characters) with underscores, and get rid of leading or trailing underscores. If name is empty after this, just label "notebook" """
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_") or "notebook"

def _ctx():
    """ Recieves Databricks notebook context object for notebook metadata (can access current user, cluster ID, etc.) """
    # live IPython shell object (kernel running notebook)
    ip = get_ipython()
    return ip.user_ns["dbutils"].notebook.entry_point.getDbutils().notebook().getContext()

def _workspace_folder_for_current_notebook() -> str:
    """ Returns logical Workspace folder for current notebook, e.g. '/Users/<user>/path'. Strips '/Workspace' prefix if present because it's not needed in the file paths """
    nb = _ctx().notebookPath().get()
    if nb.startswith("/Workspace/"):
        nb = nb[len("/Workspace"):]
    return str(Path(nb).parent) # return parent directory of current file (file path without actual file name)

def _default_log_basename() -> str:
    """ Returns default log base name for current notebook (without .log which is added in start_logging). Uses characters after last / to capture notebook name as base """
    nb = _ctx().notebookPath().get()
    name = nb.rstrip("/").split("/")[-1] # remove trailing /, split on /, get last element
    return _safe_filename(name)

def _get_basic_metadata():
    """ Get user, timestamp, notebook path, cluster id to be used in log file header """
    c = _ctx()
    return {
        "user": c.userName().get(),
        "timestamp": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
        "notebook_path": c.notebookPath().get(),
        "cluster_id": c.clusterId().get(),
    }

### USE DB API TO UPLOAD LOG TO WORKSPACE =========================================================== ###
def _workspace_import_text(path: str, content: str, overwrite: bool = True):
    """
    Upload plain text directly to Workspace using the DB REST API and the token from the current notebook session. Don't create any temporary files and don't need any extra input from user.
    """
    c = _ctx()                               # DB notebook context (current user, workspace URL, etc.)
    api_url = c.apiUrl().get().rstrip("/")   # base URL for DB API for current workspace
    token = c.apiToken().get()               # temporary DB access token, automatically created
                                             # allows you to call DB REST API
    if not token:
        raise RuntimeError(
            "Could not obtain Databricks context token (apiToken()). "
            "Ask your admin to enable apiToken() in notebook context or provide a personal access token."
        )

    # workspace import endpoints, creates/overwrites files in Workspace (what is shown in side bar)
    url = f"{api_url}/api/2.0/workspace/import"
    # converts log content into the correct format for the Workspace import API
    content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    # tells API where to put the file, what to send, whether to overwrite, etc.
    payload = {
        "path": path,                        # path for log file
        "format": "AUTO",
        "language": "AUTO",
        "overwrite": overwrite,              # always want to overwrite file if it exists
        "content": content_b64,
    }
    # token: DB checks my access with my token, content-type: body of payload is JSON
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # actually send API request, dump python dict into json format
    resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
    # 200 is standard HTTP response for success, if anything else treat this as a failure
    if resp.status_code != 200:
        raise RuntimeError(f"Workspace import failed: {resp.status_code} {resp.text}")

### EXPORT WORKSPACE FILES ========================================================================== ###
def _workspace_export_text(path: str, format: str):
    """
    Export a Workspace file (SOURCE, HTML, JUPYTER) and return decoded text
    """
    c = _ctx()                               # DB notebook context (current user, workspace URL, etc.)
    api_url = c.apiUrl().get().rstrip("/")   # base URL for DB APi for my workspace
    token = c.apiToken().get()               # temporary DB access token, automatically created
                                             # allows you to call DB REST API

    # workspace import endpoints, creates/overwrites files in Workspace (what is shown in side bar)
    url = f"{api_url}/api/2.0/workspace/export"
    # token: DB checks my access with my token
    headers = {"Authorization": f"Bearer {token}"}
    # path of the file I want to export, what format I want export in
    params = {
        "path": path,
        "format": format,
    }
    # actually send API request
    resp = requests.get(url, headers=headers, params=params, timeout=60)
    # 200 is standard HTTP repsponse for success, if anything else treat this as a failure
    if resp.status_code != 200:
        raise RuntimeError(f"Workspace export failed: {resp.status_code} {resp.text}")

    # decode response text from json file to string
    payload = resp.json()
    return base64.b64decode(payload["content"]).decode("utf-8")

def _download_link(filename: str, text: str, label: str):
    """ Create a clickable link in notebook that prompts the browser to download log or html files """
    # convert raw text into a URL-safe format
    encoded = urllib.parse.quote(text)
    # create a url that embeds file's contents
    data_url = f"data:text/plain;charset=utf-8,{encoded}"
    # html code used to display log/html interactively in notebook
    html = f"""
    <a download="{filename}"
       href="{data_url}"
       style="margin-right:20px;">
       {label}
    </a>
    """
    # tell ipython to render html in notebook output
    display(HTML(html))

### RESET LOGGING FUNCTIONS AND STATE =============================================================== ###
def _force_reset_logging_state():
    """
    Clean up everything logger has modified (hooks, warnings, globals, buffers) so a future call to start_logging begins from a clean state. Run at the beginning of start_logging, at the end of stop_logging, and during an emergency if an error occurs in the middle of the notebook
    """
    # global variables to reset
    global _is_logging, _buf, _ip, _cell_start_time, _log_start_time, _timestamp, _current_log_base, _workspace_target_path
    global _auto_stopping, _termination_reason, _last_error_text
    global _prev_showwarning, _cell_warnings, _all_warnings
    global _sftp_creds, _sftp_local_path, _sftp_remote_dir

    # unregister ipython hooks (functions that are run before and after a cell runs) even if not set
    try:
        if _ip:
            try: _ip.events.unregister("pre_run_cell", _log_cell_pre)
            except Exception: pass
            try: _ip.events.unregister("post_run_cell", _log_cell_post)
            except Exception: pass
    except Exception:
        pass

    # restore warnings to original state, notebook will act as normal
    try:
        if _prev_showwarning is not None:
            warnings.showwarning = _prev_showwarning
    except Exception:
        pass

    # reset state completely
    _is_logging = False
    _buf = None
    _ip = None
    _cell_start_time = None
    _log_start_time = None
    _timestamp = None
    _current_log_base = None
    _workspace_target_path = None
    _auto_stopping = False
    _termination_reason = "OK"
    _last_error_text = None
    _prev_showwarning = None
    _cell_warnings = []
    _all_warnings = []
    _sftp_creds = None
    _sftp_local_path = None
    _sftp_remote_dir = None

### HELPER CLASS TO WRITE OUTPUT TO NOTEBOOK AND LOG FILE =========================================== ###
class _Tee:
    # allows stdout/stderr to be redirected to both streams (assign sys.stdour and sys.stderr to Tee objects)
    # real_stream: where output normally goes, buffer_stream: output is also recorded in log file
    def __init__(self, real_stream, buffer_stream):
        self.real = real_stream # sys.stdout
        self.buffer = buffer_stream # io.StringIO()
    # whenever print()/show()/etc. called, write data to both streams, protect against failed writes
    def write(self, data):
        try:
            self.real.write(data)
        except Exception:
            pass
        try:
            self.buffer.write(data)
        except Exception:
            pass
        # python asumes sys.stdout.write() returns number of characters written, keeping compatible
        return len(data)
    # python calls flush to push data to the destination (notebook or io.StringIO) immediately
    def flush(self):
        try:
            self.real.flush()
        except Exception:
            pass
        try:
            self.buffer.flush()
        except Exception:
            pass
    # tty: is the output a real terminal (e.g. unix), some programs only show colors/animations when writing to a tty 
    # if isatty returns false, disable animations or colors 
    def isatty(self): 
        try: 
            return self.real.isatty() 
        except Exception: 
            return False

### ERRORS AND WARNINGS ============================================================================= ###
def _showwarning_proxy(message, category, filename, lineno, file=None, line=None):
    """
    Capture all warnings from warnings.warn() while logging is active, send them to the notebook UI, and store them in _all_warnings to eventually be printed to the end of the notebook.
    """
    global _cell_warnings, _all_warnings
    # want plain string not warning instance
    msg = str(message)

    # store the warning in the list for the current cell and for the global list of warnings
    entry = {"message": msg}
    _cell_warnings.append(entry)
    _all_warnings.append(entry.copy())

    # warning writes to stderr/stdout (which will be Tee objects) which will send warning to notebook UI and log buffer
    try:
        # prefer stderr if available during tee, otherwise stdout
        import sys
        # give me the attribute named stderr/stdout from sys (Tee object)
        stream = getattr(sys, "stderr", None) or getattr(sys, "stdout", None)
        if stream:
            stream.write(f"WARNING: {msg}\n")
            stream.flush()
    except Exception:
        pass

def _extract_error_from_result(result):
    """
    Returns (error type, message) if an error was raised by the cell, if no errors returns (None, None). Covers runtime errors (error_in_exec) and compile-time errors (error_before_exec). ExecutionResult is passed in by IPython.
    """
    # if there is no result object return none
    if result is None:
        return (None, None)
    # give me the attribute named error_in_exec (runtime) from result object
    err = getattr(result, "error_in_exec", None)
    # give me the attribute named error_in_exec (compile time) from result object
    if err is None:
        err = getattr(result, "error_before_exec", None)
    if err is None:
        return (None, None)
    return (type(err).__name__, (str(err) or repr(err)))

### PRE-CELL HOOK =================================================================================== ###
# ipython pre_run_cell event passes an object with metadata about the cell being executed into _log_cell_pre, providing details such as raw cell text
# different versions of ipython submit the event differently > use *args and **kwargs to account for this
def _log_cell_pre(*args, **kwargs):
    """
    Runs right before each notbook cell executes and prepares everything logger needs to capture the cell and the cell's contents cleanly.
    """
    global _cell_start_time, _buf, _cell_warnings
    global _orig_stdout, _orig_stderr, _stdout_tee, _stderr_tee

    # if there is no log currently, don't do anything
    if _buf is None:
        return

    # reset per-cell warnings at the start of each cell
    _cell_warnings = []

    # if ipython passed info in positionally, find it in args[0], if not find it in kwargs
    info = args[0] if args else kwargs.get("info")
    # source: content of the cell that is about to run, provide default None if I can't get text
    source = None
    if info is not None:
        source = getattr(info, "raw_cell", None)

    # if source is None replace with empty string, otherwise remove trailing whitespaces/newlines from string so only the cell content is kept
    source = (source or "").rstrip()
    if not source:
        return

    # cell start time for exact time cell started/ended running and total elapsed time for logging
    global _cell_start_iso
    _cell_start_iso = datetime.datetime.now().astimezone().isoformat(timespec="seconds")
    _cell_start_time = time.time()

    # stdout: stream object containing standard output (print(), normal messages, etc.)
    # stderr: stream object containing standard error (tracebacks, warnings, etc.) 
    import sys, io
    # save original stdout/stderr streams so they can be restored after the cell runs
    _orig_stdout, _orig_stderr = sys.stdout, sys.stderr
    # replace sys.stdout and sys.stderr with a Tee object (allows output to be written to notebook and log buffer)
    _stdout_tee = _Tee(_orig_stdout, io.StringIO()) 
    _stderr_tee = _Tee(_orig_stderr, io.StringIO())
    sys.stdout = _stdout_tee
    sys.stderr = _stderr_tee

    try:
        # cell separator
        _buf.write("-" * 100 + "\n")
        _buf.write(source + "\n\n")
    except Exception as e:
        _emergency_flush(f"ERROR logging cell input: {e}")

### POST-CELL HOOK ================================================================================== ###
def _log_cell_post(result=None):
    """
    Runs right after each notebook cell executes and restores stdout/stderr, captures all output/warnings/errors from cell, and stops logging upon error.
    """
    global _cell_start_time, _cell_start_iso, _buf, _auto_stopping, _termination_reason, _last_error_text
    global _cell_warnings
    global _orig_stdout, _orig_stderr, _stdout_tee, _stderr_tee

    # ensure logging is active
    if _buf is None or _cell_start_time is None:
        return
    
    # safely extract everything printed to stdout and stderr
    # ensure logging continues even if stdout or stderr fails
    try:
        import sys
        # initialize empty output
        captured_out = ""
        captured_err = ""

        # read stdout and stderr tee buffers, but skip if there are no contents
        try:
            if _stdout_tee is not None and hasattr(_stdout_tee, "buffer"):
                captured_out = _stdout_tee.buffer.getvalue()
        except Exception:
            pass
        try:
            if _stderr_tee is not None and hasattr(_stderr_tee, "buffer"):
                captured_err = _stderr_tee.buffer.getvalue()
        except Exception:
            pass

        # restore original streams: dont want sys.stout to still be a tee object
        try:
            if _orig_stdout is not None: sys.stdout = _orig_stdout
            if _orig_stderr is not None: sys.stderr = _orig_stderr
        finally:
            _orig_stdout = _orig_stderr = _stdout_tee = _stderr_tee = None

        # print captured stdout/stderr to log buffer _buf (e.g., print(), df.show()): right now they are only in cell-specific StringIO buffer
        if captured_out:
            text = captured_out.rstrip()
            if text:
                _buf.write(text + "\n")
        if captured_err:
            text = captured_err.rstrip()
            if text:
                _buf.write(text + "\n")
        
        # append cell start/end time to log buffer
        end_iso = datetime.datetime.now().astimezone().isoformat(timespec="seconds")
        start_part = f"Cell started at  {_cell_start_iso}" if _cell_start_iso else "Cell start time unknown"

        # ensure exactly one blank line between output and NOTE lines
        current_text = _buf.getvalue()
        if not current_text.endswith("\n\n"):
            if not current_text.endswith("\n"):
                _buf.write("\n")
            _buf.write("\n")

        _buf.write(f"NOTE: {start_part} and finished at {end_iso}\n")
        _cell_start_iso = None

        # append elapsed time to log buffer
        elapsed = time.time() - _cell_start_time
        _buf.write(f"NOTE: Step completed in {elapsed:.2f} seconds\n")

        # append error to log buffer if cell failed
        exc_type_name, msg = _extract_error_from_result(result)
        if exc_type_name is not None:
            err_line = f"ERROR: {exc_type_name}: {msg}"
            _buf.write(err_line + "\n")
            _buf.write("NOTE: Logging stopped due to cell error.\n")

            _termination_reason = "ERROR"
            _last_error_text = err_line

            # print error in UI and stop logging process
            print(f"[LOGGING] ERROR detected in cell: {exc_type_name}: {msg}")
            if not _auto_stopping:
                # stop_logging can only be called once
                _auto_stopping = True
                try:
                    stop_logging(overwrite=True)
                finally:
                    pass
            return

    # if logger has problems, preserve and upload what we have of the log
    except Exception as e:
        _emergency_flush(f"ERROR logging cell timing: {e}")
    # reset timing and warnings for next cell run
    finally:
        _cell_start_time = None
        _cell_warnings = []

### EMERGENCY FLUSH IF LOGGER HAS PROBLEMS ========================================================== ###
def _emergency_flush(reason: str):
    """
    Append an error footer and try to upload whatever we have to the log buffer so far. Calls _force_reset_logging_state at the end to reset the logging state. Called when the logger itself encounters an internal problem, not when there is an error in the code.
    """
    global _buf, _workspace_target_path
    try:
        # if there is no buffer or no path, log that there is an error with the logging process
        # write all current data stored in the buffer to the log and upload it to the workspace
        if _buf is None or _workspace_target_path is None:
            print(f"[LOGGING] Emergency flush skipped (no buffer/path). Reason: {reason}")
            return
        _buf.write("\n" + "=" * 100 + "\n")
        _buf.write(f"*** ERROR WITH LOGGING PROCESS ***\nReason: {reason}\n")
        _buf.write("=" * 100 + "\n")
        content = _buf.getvalue()
        _workspace_import_text(_workspace_target_path, content, overwrite=True)
        print(f"[LOGGING] Emergency flush uploaded to Workspace → {_workspace_target_path}")
    # even if emergency upload fails, print diagnostic with emergency failure and original reason this function was invoked
    except Exception as ee:
        print(f"[LOGGING] Emergency flush failed: {ee} (original reason: {reason})")
    finally:
        # hard reset so future starts are clean (also restores warnings handler)
        _force_reset_logging_state()

### USE SFTP TO UPLOAD LOG AND HTML TO PROJ FOLDER ================================================== ###
def _sftp_upload_artifacts(content: str):
    """
    Save local .log, export .html, and upload both to project folder using SFTP
    """
    # write log file locally
    try:
        if _sftp_local_path:
            Path(_sftp_local_path).parent.mkdir(parents=True, exist_ok=True)
            Path(_sftp_local_path).write_text(content, encoding="utf-8")
            print(f"SFTP: wrote local log → {_sftp_local_path}")
    except Exception as e:
        print(f"SFTP local write failed: {e}")

    # upload to SFTP (log + html)
    try:
        if sftp is None:
            print("[LOGGING] sftp_utils module not available; skipping SFTP upload.")
        elif not _sftp_creds:
            print("SFTP: no credentials; skipping upload")
        elif not _sftp_remote_dir:
            print("SFTP: remote directory not set; skipping upload")
        else:
            # upload log to project folder
            remote_log = f"{_sftp_remote_dir.rstrip('/')}/{Path(_sftp_local_path).name}"
            sftp.upload_file(
                local_path=_sftp_local_path,
                remote_path=remote_log,
                creds=(
                    _sftp_creds["host"],
                    int(_sftp_creds["port"]),
                    _sftp_creds["username"],
                    _sftp_creds["password"],
                ),
            )
            print(f"SFTP: uploaded → {remote_log}")
            # delete local log after successful upload
            try:
                Path(_sftp_local_path).unlink(missing_ok=True)
                print(f"SFTP: deleted local log → {_sftp_local_path}")
            except Exception as e:
                print(f"SFTP: failed to delete local log: {e}")

            # export notebook HTML, save locally, upload
            try:
                html_text = _workspace_export_text(_ctx().notebookPath().get(), "HTML")
                html_local_path = str(Path(_sftp_local_path).with_suffix(".html"))
                Path(html_local_path).write_text(html_text, encoding="utf-8")
                print(f"SFTP: wrote local HTML → {html_local_path}")

                remote_html = f"{_sftp_remote_dir.rstrip('/')}/{Path(html_local_path).name}"
                sftp.upload_file(
                    local_path=html_local_path,
                    remote_path=remote_html,
                    creds=(
                        _sftp_creds["host"],
                        int(_sftp_creds["port"]),
                        _sftp_creds["username"],
                        _sftp_creds["password"],
                    ),
                )
                print(f"SFTP: uploaded → {remote_html}")
                # delete local html after successful upload
                try:
                    Path(html_local_path).unlink(missing_ok=True)
                    print(f"SFTP: deleted local HTML → {html_local_path}")
                except Exception as e:
                    print(f"SFTP: failed to delete local HTML: {e}")
            except Exception as e:
                print(f"SFTP HTML upload failed: {e}")

    except Exception as e:
        print(f"SFTP upload failed: {e}")

### START LOGGING =================================================================================== ###
def start_logging(project_folder : str | None = None):
    """
    Start the notebook logging system > set up a log buffer, register pre/post cell hooks to capture cell input/output, patch Python warning system so warnings can be logged, writes header to the log. Logging will continue until the first cell error, when stop_logging is automatically called (or when stop_logging is called in the notebook).
    """
    # globals this function will initialize or modify
    global _is_logging, _buf, _ip, _log_start_time, _timestamp, _current_log_base, _workspace_target_path, _auto_stopping
    global _termination_reason, _last_error_text
    global _prev_showwarning, _cell_warnings, _all_warnings

    # reset state to make sure no previous run's hooks/etc. are still active (don't want them to run twice)
    _force_reset_logging_state()
    _auto_stopping = False              # we haven't autostopped yet (if error, sets this to True)
    _termination_reason = "OK"          # switch to ERROR if a cell fails
    _last_error_text = None             # stores ERROR: type if cell fails
    _cell_warnings = []                 # clear all cell/notebook warnings
    _all_warnings = []

    # naming filepaths and folders
    base = _default_log_basename()
    folder = _workspace_folder_for_current_notebook()
    _workspace_target_path = f"{folder}/{base}.log"
    _current_log_base = base

    # creates StringIO object to store entire log buffer, will eventually be uploaded to Workspace
    _buf = io.StringIO()
    # attaches functions to name events managed by IPython's EventManager 
    # register(event, function): when event happens, run this function
    # pre_run_cell (runs before any cell is run) and post_run_cell (runs after any cell is run) are predefined by IPython's built-in event system
    _ip = get_ipython()
    _ip.events.register("pre_run_cell", _log_cell_pre)
    _ip.events.register("post_run_cell", _log_cell_post)

    _timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    _log_start_time = time.time()

    # replace the built-in warnings.showwarning function with a custom function that will capture warnings, print them under each cell, and write the warnings to the log buffer
    try:
        global _prev_showwarning
        # saves current default warnings display to reset at the end
        _prev_showwarning = warnings.showwarning
        # replaces default warnings display with custom function
        warnings.showwarning = _showwarning_proxy
        
    # catch any exception when trying to patch the warnings system
    except Exception:
        _prev_showwarning = None

    # get metadata to display under start_logging in the notebook and at the top of the log file
    meta = _get_basic_metadata()
    header_lines = [
        "=" * 100,
        f"user                 : {meta['user']}",
        f"run start timestamp  : {meta['timestamp']}",
        f"notebook             : {meta['notebook_path']}",
        f"log file             : {_workspace_target_path}",
        f"cluster id           : {meta['cluster_id']}",
        f"python               : {platform.python_version()}",
        f"ipython              : {IPython.__version__}",
    ]
    # in case pyspark version isnt available
    try:
        header_lines.append(f"spark                : {pyspark.__version__}")
    except Exception:
        header_lines.append("spark      : (unavailable)")
    # indicate that the logging has begun
    header_lines.extend([
        "=" * 100,
        "NOTE: Logging started",
        "",
    ])
    header_text = "\n".join(header_lines)

    # write header to log buffer and print out
    _buf.write(header_text + "\n")
    _buf.flush()
    print(header_text)

    _is_logging = True

    # SFTP setup
    global _sftp_creds, _sftp_local_path, _sftp_remote_dir
    _sftp_remote_dir = project_folder  # e.g., "/userdata/cfor/projects/p26_ifx_015/logs"

    if not project_folder:
        print("No project folder specified. SFTP upload will be skipped.")
        print("Please specify a project folder to enable SFTP upload, e.g. start_logging(project_folder='path/to/project/folder')")
        _sftp_creds = None
        _sftp_local_path = None
    elif sftp is None:
        print("[LOGGING] sftp_utils module not available; SFTP upload will be skipped.")
        _sftp_creds = None
        _sftp_local_path = None
    else:
        try:
            # load saved creds if present, otherwise prompt once and persist to file
            # cache creds in memory (no re-prompt in this session)
            host, port, user, pwd = sftp.get_sftp_credentials()
            _sftp_creds = {"host": host, "port": int(port), "username": user, "password": pwd}

            # get username from metadata to add to local tmp filepath
            meta = _get_basic_metadata()
            username = meta["user"].split("@")[0]

            # write final log to same path as credentials
            cred_file = sftp.CRED_FILE # path to credentials object
            log_dir = cred_file.parent / username
            # have to make this directory since it doens't already exist
            log_dir.mkdir(parents=True, exist_ok=True)
            _sftp_local_path = str(log_dir / f"{_current_log_base}.log")
            print(f"SFTP: local log path will be {_sftp_local_path}")
        except Exception as e:
            print(f"{e}")
            _sftp_creds = None
            _sftp_local_path = None

            _force_reset_logging_state()
            raise

### STOP LOGGING ===================================================================================== ###
def stop_logging(overwrite: bool = True):
    """
    Stops the notebook logging system > appends trailer to the log buffer, prints all warnings, uploads log buffer to the workspace, reset hooks and state, displays log and html file as a clickable downloadable link.
    """
    # globals this function will initialize or modify
    global _is_logging, _buf, _ip, _log_start_time, _timestamp, _current_log_base, _workspace_target_path, _cell_start_time
    global _termination_reason, _last_error_text
    global _prev_showwarning, _all_warnings

    # don't proceed if logging was never started
    if _buf is None or _workspace_target_path is None:
        print("Logging is not active.")
        _force_reset_logging_state()
        return

    # BUILD TRAILER (time, if notebook ran successfully, warnings, timestamp, total runtime)
    try:
        _run_end_iso = datetime.datetime.now().astimezone().isoformat(timespec="seconds")
        elapsed = time.time() - _log_start_time if _log_start_time else 0.0
        trailer_lines = [
            "",
            "=" * 100,
        ]
        
        # record whether or not the notebook ran successfully
        if _termination_reason == "OK":
            trailer_lines.append("NOTEBOOK RAN SUCCESSFULLY WITH NO ERRORS")
        if _termination_reason == "ERROR":
            trailer_lines.append("NOTEBOOK DID NOT SUCCESSFULLY RUN DUE TO ERROR")
            if _last_error_text:
                trailer_lines.append(_last_error_text)

        # print all warnings
        if _all_warnings:
            for w in _all_warnings:
                trailer_lines.append(f"WARNING: {w['message']}")

        # add timestamp and total runtime
        trailer_lines.append(f"run end timestamp    : {_run_end_iso}")
        trailer_lines.append(f"total runtime        : {elapsed:.2f} seconds")
        trailer_lines.append("=" * 100)
        trailer_text = "\n".join(trailer_lines)

        # write trailer to log buffer
        _buf.write(trailer_text + "\n")
        _buf.flush()
        print(trailer_text)
    
    # if building trailer fails, logs the error and continue (don't crash logger)
    except Exception as e:
        print(f"[LOGGING] Error writing trailer: {e}")

    # unregister hooks
    try:
        if _ip:
            try: _ip.events.unregister("pre_run_cell", _log_cell_pre)
            except Exception: pass
            try: _ip.events.unregister("post_run_cell", _log_cell_post)
            except Exception: pass
    except Exception:
        pass

    # upload log file to workspace (overwrite any existing log file) and clean up
    try:
        # upload log to workspace with helper function (using databricks rest api)
        content = _buf.getvalue()
        _workspace_import_text(_workspace_target_path, content, overwrite=overwrite)
        print(f"NOTE: Logging stopped. Uploaded to Workspace → {_workspace_target_path}")

        # SFTP upload
        _sftp_upload_artifacts(content)

    # exception in case uploading log fails
    except Exception as e:
        print(f"[LOGGING] Error during finalization: {e}")

        # try to upload whatever we currently have in the buffer
        try:
            _buf.write("\n" + "=" * 100 + "\n")
            _buf.write(f"*** ERROR WITH LOGGING PROCESS ***\nReason: {e}\n")
            _buf.write("=" * 100 + "\n")
            _workspace_import_text(_workspace_target_path, _buf.getvalue(), overwrite=True)
            print(f"[LOGGING] Upload after error succeeded → {_workspace_target_path}")
        except Exception as ee:
            print(f"[LOGGING] FINAL ERROR: Could not upload log to Workspace: {ee}")

    finally:
        # final reset restores the original warnings handler and clears state
        _force_reset_logging_state()

### SQL =========================================================================================== ###
def log_df(table_name: str, limit: int = 20):
    """ Allow user to display a spark dataframe in the log file and display in the notebook. """
    spark = SparkSession.getActiveSession()
    if spark is None:
        print("[LOG] ERROR: SparkSession not found")
        return
    try:
        df = spark.table(table_name)
    except Exception as e:
        print(f"[LOG] ERROR loading table {table_name}: {e}")
        return
    
    try:
        # display in notebook UI
        display(df)
    except Exception:
        pass

    # display in log (don't shorten anything, want full contents of cell/column in log)
    print(f"[LOG] Showing table: {table_name}")
    return df.show(limit, truncate = False)