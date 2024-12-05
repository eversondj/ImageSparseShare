import os
import shutil
from libraries.MQTTNode import MQTTNode
from libraries.FileStatusManager import FileStatusManager
from libraries.ImageSplitter import ImageSplitter
from time import time, sleep
import csv
from datetime import datetime
import uuid
import hashlib

def mac_to_md5():
    mac = ':'.join(("%012X" % uuid.getnode())[i:i+2] for i in range(0, 12, 2))
    return hashlib.md5(mac.encode('utf-8')).hexdigest()

class ImageSender(MQTTNode):
    def __init__(self, broker, topic_pub, topic_sub, block_size, parts_directory,metadata_dir="Metadata", extensions=['.jpg', '.jpeg', '.png'], time_between_parts=0.01, time_between_photos=0.1, username=None, password=None):
        super().__init__(broker=broker, topic_pub=topic_pub, topic_sub=mac_to_md5(), username=username, password=password)
        self.image_processor = ImageSplitter(block_size=block_size, target_directory=parts_directory,sender_hash=mac_to_md5())
        self.extensions = extensions
        self.parts_directory = parts_directory
        self.time_between_parts = time_between_parts
        self.time_between_photos = time_between_photos
        self.metadata_dir=metadata_dir
        self.status_manager = FileStatusManager(filename=f"{metadata_dir}/senderdata.json")
        self.photos_sended = 0
        self.finished_photos = 0
        self.recused_photos = 0
        self.files_sended = 0
        self.files_resended = 0

    def send(self, filepath, category="unknown"):
        filepaths = filepath if isinstance(filepath, list) else [filepath]
        for path in filepaths:
            if any(path.lower().endswith(ext) for ext in self.extensions):
                hash = self.image_processor.cut(path, category)
                self.status_manager.update_file_status(hash, 'pending')
                header = f"{hash}_000.txt"
                self.send_file(header)
                previous_time = time()
                while self.status_manager.files_status.get(hash, {}).get('status') not in ('completed', 'recused'):
                    file_status = self.status_manager.files_status.get(hash, {}).get('status')
                    if file_status == 'pending':
                        current_time = time()
                        if (current_time - previous_time) > 10:
                            print("Pending timeout reached. Resending header.")
                            self.send_file(header)
                            self.files_resended += 1
                            previous_time = time()
                        sleep(0.1)
                    elif file_status == 'working':
                        related_files = [f for f in os.listdir(self.parts_directory) if f.startswith(hash) and f.endswith('.dat')]
                        for file in related_files:
                            self.send_file(file)
                            sleep(self.time_between_parts)
                        self.status_manager.update_file_status(hash, 'waiting')
                        self.photos_sended += 1
            else:
                print(f"Unsupported file format {path}")
            sleep(self.time_between_photos)

        start_time = time()
        while not all(info.get('status') in ('completed', 'recused') for info in self.status_manager.files_status.values()):
            print("Waiting for all files to be marked as completed...")
            elapsed_time = time() - start_time

            if elapsed_time >= 30:
                for file_hash, info in self.status_manager.files_status.items():
                    if info.get('status') == 'waiting':
                        self.publish(f"{file_hash}_wai")
                start_time = time()
            sleep(1)

        self.show_stats()
        print("All files have been processed and are marked as completed.")
        self.publish("finished",topic=f"{mac_to_md5()}_orq")
        self.clear_storage()


    def on_message(self, client, userdata, msg):
        message = msg.payload.decode()
        ret = 0  
        while True:
            base_filename = None
            new_status = 'working'

            if message.endswith(".dat"):
                ret = self.send_file(message)
                if ret==0:
                    self.files_resended += 1
                break
            elif message.endswith(".txt"):
                base_filename = message[:-8]
            elif message.endswith(".rec"):
                base_filename = message[:-4]
                self.recused_photos+=1
                new_status = 'recused'
                self.show_stats()
            elif message.endswith("_del"):
                self.finished_photos += 1
                base_filename = message[:-4]
                new_status = 'completed'
            else:
                print(f"Received unsupported message type: {message}")
                ret = -1
                break

            if base_filename and new_status:
                current_timestamp = time()
                if base_filename in self.status_manager.files_status:
                    self.status_manager.update_file_status(base_filename, new_status, current_timestamp)
            break

        return ret
     

    def send_file(self, part_file):
        ret=-1
        part_file_path = os.path.join(self.parts_directory, part_file)
        if os.path.isfile(part_file_path):
            with open(part_file_path, 'rb') as file:
                file_content = file.read()
            if part_file_path.endswith('.txt'):
                self.publish(file_content)
            else:
                self.publish(file_content, topic=f"{mac_to_md5()}_orq")
            self.files_sended += 1
            ret = 0
        self.show_stats()
        return ret
    
    def clear_storage(self):
        """Remove all files from the specified directory."""
        for filename in os.listdir(self.parts_directory):
            file_path = os.path.join(self.parts_directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # Use shutil to remove directories
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')

    def show_stats(self):
        print(
            "-------------------------------------------------\n"
            "| {:24s}{:>3} imagens começadas |\n"
            "| {:24s}{:>3} imagens terminadas|\n"
            "| {:24s}{:>3} imagens recusadas |\n"
            "| {:24s}{:>3} partes enviadas   |\n"
            "| {:24s}{:>3} partes reenviadas |\n"
            "-------------------------------------------------"
            .format(
                '', self.photos_sended,
                '', self.finished_photos,
                '', self.recused_photos,
                '', self.files_sended,
                '', self.files_resended
            )
        )
        self.save_stats_to_csv(f"{self.metadata_dir}/stats.csv")

    def save_stats_to_csv(self, filename):
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data = [now, self.photos_sended, self.finished_photos, self.recused_photos, self.files_sended, self.files_resended]
        try:
            with open(filename, 'a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(data)
        except Exception as e:
            print("An error occurred while writing to the file:", e)
