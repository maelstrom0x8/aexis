import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logging(service_type: str, service_id: str = None):
    """
    Sets up structured logging for a service.
    Logs are stored in /home/godelhaze/dev/megalith/aexis/.aexis/log/<type>_[id]/
    
    Args:
        service_type: The type of service (station, pod, api, web)
        service_id: The unique identifier for the service instance
    """
    # Root log directory
    root_log_dir = Path("/home/godelhaze/dev/megalith/aexis/.aexis/log")
    
    # Specific service log directory
    if service_id:
        if service_id.startswith(f"{service_type}_"):
            log_dir = root_log_dir / service_id
        else:
            log_dir = root_log_dir / f"{service_type}_{service_id}"
    else:
        log_dir = root_log_dir / service_type
        
    # Ensure directory exists
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / "active.log"
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers if any
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        
    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s [%(name)s] %(levelname)s %(message)s"
    )
    
    # File handler (Rotating)
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Stream handler (Stdout)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    logging.info(f"Logging initialized. Service: {service_type}, ID: {service_id}, Path: {log_file}")
    
    return logger
