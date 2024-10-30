import logging

# Set up a logger with custom configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set the lowest level you want to capture

# File handler to write logs to a file
file_handler = logging.FileHandler('data_pipeline.log')
file_handler.setLevel(logging.INFO)  # Adjust as needed

# Console handler for on-screen logging
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Custom log format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Adding handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

