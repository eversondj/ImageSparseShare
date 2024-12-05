from libraries.ImageReceiver import ImageReceiver
from time import sleep
import threading

category = "gatinho"

if __name__ == "__main__":
    receiver = ImageReceiver(
        broker="test.mosquitto.org",
        topic_pub=f"{category}_req_ret",
        topic_sub=f"{category}_req",
        received_data_dir="./ReceivedData",
        restored_images_dir=f"./Storage/{category}",
        time_between_recovery_tries=10
    )
    
    try:
        receiver.publish(f"{category}", "test_type_request")
        thread = threading.Thread(target=receiver.process_files)
        thread.start()
        while True:
            sleep(10)
    except KeyboardInterrupt:
        print("Program terminated by user.")
        thread.join()
