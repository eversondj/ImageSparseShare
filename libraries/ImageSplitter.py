import os
import hashlib
from PIL import Image
from datetime import datetime
import shutil
import logging

class ImageSplitter:
    def __init__(self, block_size=102400, target_directory="./ToSend",sender_hash="unknown"):
        self.block_size = block_size
        self.target_directory = target_directory
        self.sender_hash=sender_hash
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def generate_file_hash(self, file_path):
        try:
            file_name = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)
            unique_identifier = f"{file_name}_{file_size}"
            hash_obj = hashlib.sha256(unique_identifier.encode())
            return hash_obj.hexdigest()
        except Exception as e:
            logging.error(f"Error generating hash for {file_path}: {e}")
            raise

    def split_image(self, file_name):
        parts = []
        counter = 0
        try:
            with open(file_name, 'rb') as file:
                while True:
                    piece = file.read(self.block_size)
                    if not piece:
                        break
                    parts.append(piece)
                    counter += 1
            return parts, counter
        except Exception as e:
            logging.error(f"Error splitting image {file_name}: {e}")
            raise

    def save_parts(self, parts, base_name):
        try:
            if not os.path.exists(self.target_directory):
                os.makedirs(self.target_directory)
            for i, part in enumerate(parts, start=1):
                part_file_path = os.path.join(self.target_directory, f"{base_name}_{i:03}.dat")
                with open(part_file_path, 'wb') as part_file:
                    part_file.write(f"{base_name}_{i:03}.dat".encode() + part)
                    part_file.flush()
                    os.fsync(part_file.fileno())

                if not os.path.isfile(part_file_path) or os.path.getsize(part_file_path) == 0:
                    raise Exception(f"Failed to write part {i} for {base_name}")
        except Exception as e:
            logging.error(f"Error saving parts for {base_name}: {e}")
            raise

    def get_image_resolution(self, file_path):
        try:
            with Image.open(file_path) as img:
                return img.size
        except Exception as e:
            logging.error(f"Error getting resolution for {file_path}: {e}")
            raise

    def create_info_file(self, checksum, resolution, file_name, counter, sender_hash, category="unknown"):
        try:
            info_file_path = os.path.join(self.target_directory, f"{checksum}_000.txt")
            info_content= f"{checksum}_000.txt"
            info_content += f"resolution: {resolution[0]}x{resolution[1]}\n"
            info_content += "localization: 32.0727429,-52.1736719\n"
            info_content += f"checksum: {checksum}\n"
            info_content += f"sender_hash: {sender_hash}\n"
            info_content += f"timestamp: {datetime.now().isoformat()}\n"
            info_content += f"file_name: {file_name}\n"
            info_content += f"category: {category}\n"
            info_content += f"parts: {counter}\n"
            with open(info_file_path, 'w') as info_file:
                info_file.write(info_content)
        except Exception as e:
            logging.error(f"Error creating info file for {checksum}: {e}")
            raise

    def cut(self, file_path, category):
        try:
            image_hash = self.generate_file_hash(file_path)
            resolution = self.get_image_resolution(file_path)
            file_name = os.path.basename(file_path)
            parts, counter = self.split_image(file_path)
            self.save_parts(parts, image_hash)
            self.create_info_file(image_hash, resolution, file_name, counter, self.sender_hash, category)
            return image_hash
        except Exception as e:
            logging.error(f"Error processing the image {file_path}: {e}")
