import os
from libraries.MQTTNode import MQTTNode  
from libraries.FileStatusManager import FileStatusManager
from libraries.ImageRestore import ImageRestore
from time import sleep, time
from concurrent.futures import ThreadPoolExecutor
import threading

class ImageReceiver(MQTTNode):
    def __init__(self, broker, topic_pub, topic_sub, received_data_dir, restored_images_dir, file_queue=None, metadata_dir="Metadata", time_between_recovery_tries=10, username=None, password=None):
        super().__init__(broker=broker, topic_pub=topic_pub, topic_sub=topic_sub, username=username, password=password)
        self.received_data_dir = received_data_dir
        self.restored_images_dir = restored_images_dir
        self.time_between_recovery_tries = time_between_recovery_tries
        self.image_recovery = ImageRestore(received_data_dir=self.received_data_dir, restored_images_dir=self.restored_images_dir)
        self.status_manager = FileStatusManager(filename=f"{metadata_dir}/receiverdata.json")
        self.keep_running = True
        self.file_queue = file_queue 
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.lock = threading.Lock()
    def on_message(self, client, userdata, msg):
        message = msg.payload
        while True:
            if msg.topic == "test_type_request":
                folder_to_send = message.decode()
                print(f"Me pediram a pasta {folder_to_send}")
                if os.path.isdir(f"./storage/{folder_to_send}"):
                    self.file_queue.put(folder_to_send)
                break
            if message == b'finished':
                self.remove_topic(msg.topic)
                break
            if len(message) >= 65 and message[64] == ord('_') and message[68] == ord('.'):
                file_hash = message[:64].decode()
                file_name_short = message[:72].decode()
                current_timestamp = time()
                if file_name_short[-4:] == "_wai":
                    if (file_hash in self.status_manager.files_status) and (self.status_manager.files_status[file_hash].get('status') == 'completed'):
                        self.publish(f"{file_hash}_del", sender_hash)
                    break
                if file_name_short[-4:] == ".txt":
                    file_name = os.path.join(self.received_data_dir, file_name_short)
                    os.makedirs(os.path.dirname(file_name), exist_ok=True)
                    with open(file_name, 'wb') as file:
                        file.write(message[72:])
                    with open(file_name, 'r') as file:
                        lines = file.readlines()
                        category = None
                        sender_hash = None
                        for line in lines:
                            if line.startswith("category:"):
                                category = line.split(":")[1].strip()
                            if line.startswith("sender_hash:"):
                                if (file_hash in self.status_manager.files_status) and (self.status_manager.files_status[file_hash].get('status') == 'completed'):
                                    if line.startswith("sender_hash:"):
                                            sender_hash = line.split(":")[1].strip()
                                            self.add_topic(sender_hash + "_orq")
                                            self.publish(f"{file_hash}.rec", sender_hash)
                                    break
                                self.status_manager.update_file_status(file_hash, 'working', current_timestamp)
                                sender_hash = line.split(":")[1].strip()
                                self.add_topic(sender_hash + "_orq")
                                self.publish(file_name_short, sender_hash)
                            if category and sender_hash:
                                break
                    if category:
                        self.status_manager.save_category(file_hash, category)
                    if sender_hash:
                        self.status_manager.save_sender_hash(file_hash, sender_hash)
                else:
                    file_name = os.path.join(self.received_data_dir, file_name_short)
                    os.makedirs(os.path.dirname(file_name), exist_ok=True)
                    with open(file_name, 'wb') as file:
                        file.write(message[72:])
                break

    def process_files(self):
        while self.keep_running:
            current_time = time()
            tasks = []
            with self.lock:
                for file_hash, status_info in self.status_manager.files_status.items():
                    if status_info['status'] == 'working':
                        last_update_time = status_info['timestamp']
                        category = status_info.get('category', "unknown")
                        sender_hash = status_info.get('sender_hash', "unknown")
                        if last_update_time and (current_time - last_update_time) > self.time_between_recovery_tries:
                            tasks.append(self.executor.submit(self.handle_file, file_hash, category, sender_hash))
            sleep(0.01)

    def handle_file(self, file_hash, category, sender_hash):
        with self.lock:
            missing_parts = self.image_recovery.restore(file_hash, category)
            current_time = time()
            if missing_parts == 2:
                self.status_manager.update_file_status(file_hash, 'completed', current_time)
                self.publish(f"{file_hash}_del", sender_hash)
            else:
                for part in missing_parts:
                    part_request = f"{file_hash}_{part:03}.dat"
                    self.publish(part_request, sender_hash)
                self.status_manager.update_file_status(file_hash, 'working', current_time)

    def stop(self):
        self.keep_running = False
        self.executor.shutdown(wait=True)
