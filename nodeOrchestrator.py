import os
from libraries.ImageReceiver import ImageReceiver
from libraries.ImageSender import ImageSender
from time import sleep
import threading
from queue import Queue

def send_files(file_queue):
    while True:
        if not file_queue.empty():
            category = file_queue.get()
            files_to_send = [os.path.join(f"storage/{category}", file) for file in os.listdir(f"storage/{category}") if file.lower().endswith(('.png', '.jpeg', '.jpg'))]
            if files_to_send:
                sender = ImageSender(
                    broker="test.mosquitto.org",
                    topic_pub=f"{category}_req",
                    topic_sub=f"{category}_req_ret",
                    # username='astral',
                    # password='Senhaastral',
                    block_size=500000,
                    parts_directory="SplitedParts",
                    extensions=['.jpg', '.jpeg', '.png'],
                    time_between_parts=0,
                    time_between_photos=0
                )
                sender.send(files_to_send, category)
            file_queue.task_done()
        sleep(1)

if __name__ == "__main__":
    file_queue = Queue()

    receiver = ImageReceiver(
        broker="test.mosquitto.org",
        topic_pub="test_image_receiver",
        topic_sub=["test_image_sender", "test_type_request"],
        received_data_dir="./ReceivedData",
        restored_images_dir="./storage",
        file_queue=file_queue,
        time_between_recovery_tries=10
    )
    receiver_thread = threading.Thread(target=receiver.process_files)
    receiver_thread.start()

    num_threads = 4
    sender_threads = []
    for i in range(num_threads):
        sender_thread = threading.Thread(target=send_files, args=(file_queue,))
        sender_threads.append(sender_thread)
        sender_thread.start()

    try:
        while True:
            sleep(10)
    except KeyboardInterrupt:
        print("Program terminated by user.")
        exit

    receiver_thread.join()
    for thread in sender_threads:
        thread.join()
