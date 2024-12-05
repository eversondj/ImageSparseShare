import os
from libraries.ImageSender import ImageSender
import threading
from time import sleep

def send_files(sender, files_to_send, category):
        sender.send(files_to_send, category)

if __name__ == "__main__":
    sender = ImageSender(
        broker="test.mosquitto.org",
        topic_pub="test_image_sender",
        topic_sub="test_image_receiver",
        # username='astral',
        # password='Senhaastral',
        block_size=500000,
        parts_directory="SplitedParts",
        extensions=['.jpg', '.jpeg', '.png'],
        time_between_parts=0.01,
        time_between_photos=0.01
    )

    directory = 'gatinho'
    files_to_send = [os.path.join(f"./storage/{directory}", file) for file in os.listdir(f"./storage/{directory}") if file.lower().endswith(('.png', '.jpeg', '.jpg'))]
    try:
        thread = threading.Thread(target=send_files,args=(sender,files_to_send,directory))
        thread.start() 
        while True:
            sleep(10) 
    except KeyboardInterrupt:
        print("Program terminated by user.")
        thread.join()
