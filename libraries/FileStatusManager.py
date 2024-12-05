import json
import os

class FileStatusManager:
    def __init__(self, filename='files_status.json'):
        self.filename = filename
        self.ensure_directory_exists(filename)
        self.files_status = self.load_files_status()

    def save_files_status(self):
        """Saves the status of files to the specified JSON file."""
        try:
            with open(self.filename, 'w') as file:
                json.dump(self.files_status, file)
        except Exception as e:
            print("Error saving files' status:", e)

    def load_files_status(self):
        """Loads the status of files from the specified JSON file. 
        Creates an empty file if it does not exist or is empty."""
        try:
            with open(self.filename, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            return self.create_empty_file()
        except json.JSONDecodeError:
            return self.create_empty_file()

    def create_empty_file(self):
        """Creates an empty JSON file and returns an empty dictionary."""
        with open(self.filename, 'w') as file:
            json.dump({}, file)
        return {}

    def ensure_directory_exists(self, filepath):
        """Ensures the directory for the file exists. Creates it if it does not exist."""
        directory = os.path.dirname(filepath)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
        elif not directory:
            print("No directory in filepath. Using current directory.")

    def update_file_status(self, file_hash, status, timestamp=None, category=None):
        """Updates the status of a file based on its hash, and optionally adds a timestamp and category."""
        if file_hash not in self.files_status:
            self.files_status[file_hash] = {}
        self.files_status[file_hash]['status'] = status
        if timestamp:
            self.files_status[file_hash]['timestamp'] = timestamp
        if category:
            self.files_status[file_hash]['category'] = category
        self.save_files_status()

    def save_category(self, file_hash, category):
        """Updates the category of a file based on its hash. If file_hash doesn't exist, create a new entry."""
        if file_hash not in self.files_status:
            self.files_status[file_hash] = {}
        self.files_status[file_hash]['category'] = category
        self.save_files_status()
    def save_sender_hash(self, file_hash, sender_hash):
        """Updates the sender_hash of a file based on its hash. If file_hash doesn't exist, create a new entry."""
        if file_hash not in self.files_status:
            self.files_status[file_hash] = {}
        self.files_status[file_hash]['sender_hash'] = sender_hash
        self.save_files_status()

