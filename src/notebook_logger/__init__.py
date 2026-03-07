# make functions available directly from notebook-logger package
from .logger import start_logging, stop_logging, log_df

# list of functions available for external use
__all__ = ['start_logging', 'stop_logging', 'log_df']

# package version
__version__ = '0.1.0'