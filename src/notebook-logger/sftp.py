### ALL FUNCTIONS TO IMPORT ========================================================================= ###
# Requirements implemented:
# - Databricks-friendly: host/port widgets only; username/password never shown in widgets
# - Installs paramiko if missing
# - ALWAYS prompts/sets up credentials when this notebook is %run
# - If you choose NOT to persist credentials, uploads in the SAME notebook session will NOT re-prompt
#   (credentials are cached in-memory for this run)
# - Optionally persists credentials to a secure file (best effort chmod 600)
# - On %run: prints function availability and runs an automatic upload test

### SETUP =========================================================================================== ###
# 0) paramiko install if missing
try:
    import paramiko  # noqa: F401
except ModuleNotFoundError:
    import sys, subprocess
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "paramiko", "-q"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    import paramiko  # noqa: F401

# 1) standard imports
import getpass
import json
import logging
import os
import sys
import socket
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# verbose output (set SFTP_VERBOSE=0 to reduce console output)
SFTP_VERBOSE = os.getenv("SFTP_VERBOSE", "1").lower() in ("1", "true", "yes")

def _vprint(msg: str):
    if SFTP_VERBOSE:
        print(msg)

# 2) configuration flags (env)
FORCE_PROMPT = os.getenv("SFTP_FORCE_PROMPT", "") not in ("", "0", "false", "False")
CLEAR_SAVED = os.getenv("SFTP_CLEAR", "") not in ("", "0", "false", "False")

# silent mode for saved creds (no prompt)
AUTO_USE_SAVED_CREDS = False

# in-memory cache so reruns within the SAME notebook session don't re-prompt
# even if user chose not to persist
_CACHED_CREDS: Optional[Tuple[str, int, str, str]] = None

# get username to use for naming files
def _get_dbutils_username() -> Optional[str]:
    # try to obtain dbutils from the active IPython shell (works in notebooks)
    try:
        import IPython
        ip = IPython.get_ipython()
        if ip and "dbutils" in ip.user_ns:
            return ip.user_ns["dbutils"].notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    except Exception:
        pass
    # fallback: try the direct symbol if available
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()  # type: ignore
    except Exception:
        return None

### FILE PATHS ====================================================================================== ###
# 3) choose a writable credential file path (best-effort)
def _candidate_cred_paths() -> List[Path]:
    
    u = _get_dbutils_username()
    if not u:
        u = os.getenv("USER") or os.getenv("USERNAME") or "user"
    u = u.split("@")[0]
    fname = f".sftp_saved_creds_{u}.json"

    candidates: List[Path] = []
    try:
        home = Path.home()
        candidates.append(home / fname)
    except Exception:
        pass
    candidates.append(Path(f"/tmp/{fname}"))
    candidates.append(Path(f"/dbfs/tmp/{fname}"))
    return candidates

def _select_writable_cred_file() -> Optional[Path]:
    for p in _candidate_cred_paths():
        try:
            parent = p.parent
            if not parent.exists():
                parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(".test")
            with tmp.open("w", encoding="utf-8") as fh:
                fh.write("")
            tmp.unlink()
            return p
        except Exception:
            continue
    return None

CRED_FILE = _select_writable_cred_file()
if CRED_FILE is None:
    print("Warning: no writable location found to persist credentials. Saved-credential feature will be disabled.")
else:
    print(f"Credential persistence path chosen: {CRED_FILE}")

### WIDGETS ========================================================================================= ###
# 4) databricks widget helpers (host & port only)
def _in_databricks() -> bool:
    try:
        _ = dbutils  # type: ignore[name-defined]
        return True
    except Exception:
        return False

def _get_widget(name: str) -> Optional[str]:
    try:
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return None

def _set_widget(name: str, value: str):
    try:
        try:
            dbutils.widgets.remove(name)  # type: ignore[name-defined]
        except Exception:
            pass
        dbutils.widgets.text(name, value)  # type: ignore[name-defined]
    except Exception:
        pass

### SAVE/LOAD CREDENTIALS =========================================================================== ###
# 5) secure save/load of credentials (best-effort, chmod 600)
def validate_credentials(host: str, port: int, username: str, password: str) -> None:
    """
    Attempt to open and immediately close an SFTP connection.
    Raises descriptive error depending on failure type.
    """
    try:
        with SFTPClientManager(host, port, username, password):
            pass

    except paramiko.AuthenticationException:
        raise RuntimeError("Authentication failed: Invalid user ID or password.")
    except socket.gaierror:
        raise RuntimeError("Connection failed: Invalid SFTP host name.")
    except ConnectionRefusedError:
        raise RuntimeError("Connection refused: Invalid SFTP port or server not accepting connections.")
    except TimeoutError:
        raise RuntimeError("Connection timed out: Host unreachable.")
    except paramiko.SSHException as e:
        raise RuntimeError(f"SSH negotiation failed: {e}")
    except Exception as e:
        raise RuntimeError(f"Unexpected SFTP connection error: {e}")

def _read_saved_creds() -> Optional[Dict[str, str]]:
    if CLEAR_SAVED:
        if CRED_FILE and CRED_FILE.exists():
            try:
                CRED_FILE.unlink()
            except Exception:
                pass
        if _in_databricks():
            try:
                dbutils.widgets.remove("SFTP_ONE_TIME")  # type: ignore[name-defined]
            except Exception:
                pass
        return None

    if CRED_FILE is None or not CRED_FILE.exists():
        return None
    try:
        with CRED_FILE.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if all(k in data for k in ("host", "port", "username", "password")):
            return data
    except Exception:
        return None
    return None

def _write_saved_creds(host: str, port: int, username: str, password: str) -> bool:
    if CRED_FILE is None:
        return False
    try:
        tmp = CRED_FILE.with_suffix(".tmp")
        obj = {"host": host, "port": port, "username": username, "password": password}
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(obj, fh)
        try:
            tmp.chmod(0o600)
        except Exception:
            pass
        tmp.replace(CRED_FILE)
        try:
            CRED_FILE.chmod(0o600)
        except Exception:
            pass
        return True
    except Exception as e:
        print("Warning: failed to save credentials to file:", e)
        return False

# 6) credential flow
def get_sftp_credentials(
    default_host: str = "sftp.example.com",
    default_port: int = 22,
    prefer_cached: bool = True
) -> Tuple[str, int, str, str]:
    """
    Returns (host, port, username, password).

    Priority order:
      1) In-memory cached creds (same notebook session) if prefer_cached=True
      2) Env vars SFTP_HOST, SFTP_USER, SFTP_PASSWORD
      3) Saved creds file (if present and allowed)
      4) Interactive prompt (optionally saving to file)

    Databricks: host/port kept in widgets; username/password never put in widgets.
    """
    global _CACHED_CREDS

    # 1) in-memory cache
    if prefer_cached and _CACHED_CREDS is not None and not FORCE_PROMPT:
        _vprint("Using credentials already entered in this notebook session.")
        return _CACHED_CREDS

    # 2) env vars override
    env_host = os.getenv("SFTP_HOST")
    env_user = os.getenv("SFTP_USER")
    env_pass = os.getenv("SFTP_PASSWORD")
    env_port = os.getenv("SFTP_PORT")
    if env_host and env_user and env_pass:
        port = int(env_port) if env_port else default_port
        creds = (env_host, port, env_user, env_pass)
        _CACHED_CREDS = creds
        _vprint("Using credentials from environment variables.")
        return creds

    # databricks path
    if _in_databricks():
        # ensure host/port widgets exist
        try:
            dbutils.widgets.get("SFTP_HOST")  # type: ignore[name-defined]
        except Exception:
            dbutils.widgets.text("SFTP_HOST", default_host)  # type: ignore[name-defined]
        try:
            dbutils.widgets.get("SFTP_PORT")  # type: ignore[name-defined]
        except Exception:
            dbutils.widgets.text("SFTP_PORT", str(default_port))  # type: ignore[name-defined]

        host_widget = _get_widget("SFTP_HOST") or default_host
        port_widget = _get_widget("SFTP_PORT") or str(default_port)

        saved = _read_saved_creds()
        saved_flag = (_get_widget("SFTP_ONE_TIME") or "").lower() in ("1", "true", "yes")

        # 3) saved creds
        if saved and saved_flag and not FORCE_PROMPT:
            if AUTO_USE_SAVED_CREDS:
                _vprint("Saved credentials found. Using them without prompting.")
                host = host_widget.strip() or saved["host"]
                try:
                    port = int(port_widget)
                except Exception:
                    port = int(saved["port"])
                creds = (host, port, saved["username"], saved["password"])
                _CACHED_CREDS = creds
                return creds

            ans = input("Saved SFTP credentials detected. Use saved credentials? [Y/n]: ").strip().lower()
            if ans in ("", "y", "yes"):
                host = host_widget.strip() or saved["host"]
                try:
                    port = int(port_widget)
                except Exception:
                    port = int(saved["port"])
                creds = (host, port, saved["username"], saved["password"])
                _CACHED_CREDS = creds
                return creds
            # else fall through to interactive collection

        # 4) interactive collection
        print("Enter SFTP credentials for this run.")
        host = input(f"SFTP host [press ENTER for default - {host_widget}]: ").strip() or host_widget
        port_in = input(f"SFTP port [press ENTER for default - {port_widget}]: ").strip() or port_widget
        try:
            port = int(port_in)
        except Exception:
            port = default_port
        username = input("SFTP username: ").strip()
        password = getpass.getpass("SFTP password (hidden): ")

        if not all([host, username, password]):
            raise RuntimeError("Hostname, username, and password are required.")

        # validate credentials before offering to save
        print("Validating SFTP credentials...")
        try:
            validate_credentials(host, port, username, password)
        except Exception as e:
            raise
        
        print("SFTP authentication successful.")

        # cache for this session immediately (so later uploads won't re-prompt)
        creds = (host, port, username, password)
        _CACHED_CREDS = creds

        # save option (persist for future sessions)
        save_ans = input("Save your credentials locally for this environment (cleared when it restarts)? [y/N]: ").strip().lower()
        if save_ans in ("y", "yes"):
            ok = _write_saved_creds(host, port, username, password)
            if ok:
                _set_widget("SFTP_ONE_TIME", "true")
                _set_widget("SFTP_HOST", host)
                _set_widget("SFTP_PORT", str(port))
                print("Credentials saved securely. Future reruns can reuse them.")
            else:
                _set_widget("SFTP_ONE_TIME", "false")
                _set_widget("SFTP_HOST", host)
                _set_widget("SFTP_PORT", str(port))
                print("Failed to persist credentials. They will not be available next session.")
        else:
            # still update host/port widgets, but do not set one-time flag
            _set_widget("SFTP_ONE_TIME", "false")
            _set_widget("SFTP_HOST", host)
            _set_widget("SFTP_PORT", str(port))
            print("Credentials not persisted. They will still be reused for uploads in this notebook session.")

        return creds

    # non-Databricks fallback
    saved = None if FORCE_PROMPT else _read_saved_creds()
    if saved:
        if AUTO_USE_SAVED_CREDS:
            creds = (saved["host"], int(saved["port"]), saved["username"], saved["password"])
            _CACHED_CREDS = creds
            _vprint("Saved credentials found. Using them without prompting.")
            return creds

        use = input("Saved SFTP credentials found. Use them? [Y/n]: ").strip().lower()
        if use in ("", "y", "yes"):
            creds = (saved["host"], int(saved["port"]), saved["username"], saved["password"])
            _CACHED_CREDS = creds
            return creds

    host = input(f"SFTP host [press ENTER for default - {default_host}]: ").strip() or default_host
    port_in = input(f"SFTP port [press ENTER for default - {default_port}]: ").strip() or str(default_port)
    try:
        port = int(port_in)
    except Exception:
        print(f"Invalid SFTP port entered. Defaulting to {default_port}.")
        port = default_port
    username = input("User ID: ").strip()
    password = getpass.getpass("Password (hidden): ")

    if not all([host, username, password]):
        raise RuntimeError("Hostname, username and password are required.")

    # validate credentials before offering to save
    print("Validating SFTP credentials...")
    try:
        validate_credentials(host, port, username, password)
    except Exception as e:
        raise
    
    print("SFTP authentication successful.")

    creds = (host, port, username, password)
    _CACHED_CREDS = creds

    save_ans = input("Save your credentials for this cluster only (cleared when it restarts)? [y/N]: ").strip().lower()
    if save_ans in ("y", "yes"):
        ok = _write_saved_creds(host, port, username, password)
        if ok:
            _vprint(f"Credentials saved to {CRED_FILE} (mode 0o600).")
        else:
            _vprint("Failed to save credentials; they will not persist.")
    else:
        _vprint("Credentials not persisted. They will still be reused for uploads in this session.")

    return creds

### SFTP ============================================================================================ ###
# 7) SFTP client manager & helpers
class SFTPClientManager:
    def __init__(self, hostname: str, port: int, username: str, password: str):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.transport = None
        self.sftp = None

    def __enter__(self):
        _vprint(f"Connecting to {self.hostname}:{self.port} as {self.username} ...")
        self.transport = paramiko.Transport((self.hostname, self.port))
        self.transport.connect(username=self.username, password=self.password)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)
        _vprint("Connection established.")
        return self

    def __exit__(self, exc_type, exc, tb):
        _vprint("Closing connection.")
        if self.sftp:
            try:
                self.sftp.close()
            except Exception:
                pass
        if self.transport:
            try:
                self.transport.close()
            except Exception:
                pass

    def _mkdirs(self, remote_dir: str):
        if remote_dir in ("", "/"):
            return
        _vprint(f"Ensuring remote directory exists: {remote_dir}")
        parts = remote_dir.strip("/").split("/")
        path = ""
        for part in parts:
            path += "/" + part
            try:
                self.sftp.stat(path)
            except Exception:
                try:
                    self.sftp.mkdir(path)
                    _vprint(f"Created directory: {path}")
                except Exception:
                    pass

    def upload(self, local_path: str, remote_path: str):
        remote_dir = remote_path.rsplit("/", 1)[0]
        self._mkdirs(remote_dir)
        _vprint(f"Uploading file:\n remote: {remote_path}")
        self.sftp.put(local_path, remote_path)
        _vprint("Upload completed successfully.")

### UPLOAD FILES ==================================================================================== ###
def upload_file(local_path: str, remote_path: str, creds: Optional[Tuple[str, int, str, str]] = None):
    """Upload a single file. If creds is None, uses cached creds if available; otherwise runs credential flow."""
    if not Path(local_path).exists():
        raise FileNotFoundError(f"Local file does not exist: {local_path}")

    if creds is None:
        _vprint("Resolving SFTP credentials...")
        host, port, user, pwd = get_sftp_credentials(prefer_cached=True)
    else:
        host, port, user, pwd = creds

    _vprint("Opening SFTP session...")
    with SFTPClientManager(host, port, user, pwd) as sftp_mgr:
        sftp_mgr.upload(local_path=local_path, remote_path=remote_path)   

def upload_log_to_project(
    local_log_path: Optional[str] = None,
    project_remote_dir: str = "/project/logs"
) -> str:
    """Upload a log file to the project's remote log directory. Returns the remote path."""
    local_path = str(local_log_path)
    if not local_path or not Path(local_path).exists():
        raise FileNotFoundError(f"Local log file not found: {local_log_path}")

    remote_path = f"{project_remote_dir.rstrip('/')}/{Path(local_path).name}"
    upload_file(local_path=local_path, remote_path=remote_path)
    return remote_path