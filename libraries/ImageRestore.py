import os
from PIL import Image

class ImageRestore:
    def __init__(self, received_data_dir="ReceivedData", restored_images_dir="RestoredImages"):
        self.received_data_dir = received_data_dir
        self.restored_images_dir = restored_images_dir
        os.makedirs(self.restored_images_dir, exist_ok=True)

    def restore(self, file_to_recovery, category="unknown"):
        os.makedirs(f"{self.restored_images_dir}/{category}", exist_ok=True)
        txt_file = os.path.join(self.received_data_dir, f"{file_to_recovery}_000.txt")
        with open(txt_file, 'r') as file:
            content = file.read()
        metadata = dict(line.split(': ') for line in content.split('\n') if line)
        parts, checksum, file_name = int(metadata['parts']), metadata['checksum'], metadata['file_name']
        combined_image_path = os.path.join(self.restored_images_dir, f"{checksum}.png")

        with open(combined_image_path, 'wb') as output_image:
            missing_parts = [] 
            for i in range(1, parts + 1):
                part_file_name = f"{checksum}_{i:03}.dat"
                part_path = os.path.join(self.received_data_dir, part_file_name)
                if os.path.isfile(part_path):
                    with open(part_path, 'rb') as part:
                        output_image.write(part.read())
                else:
                    print(f"Part {i} is missing for {checksum}")
                    missing_parts.append(i)

        if missing_parts:
            missing_parts.sort()
            print(f"Missing parts for {checksum}: {missing_parts}")
        else:
            self.convert_image_format(combined_image_path, os.path.join(self.restored_images_dir, category, file_name))
            missing_parts=2
        return missing_parts


    def write_part(self, part_index, checksum, output_path, missing_parts):
        part_file_name = f"{checksum}_{part_index:03}.dat"
        part_path = os.path.join(self.received_data_dir, part_file_name)
        if os.path.isfile(part_path):
            with open(part_path, 'rb') as part, open(output_path, 'ab') as output_image:
                output_image.write(part.read())
        else:
            missing_parts.append(part_index)

    def convert_image_format(self, png_image_path, original_format_path):
        try:
            
            directory, filename = os.path.split(original_format_path)
            base, ext = os.path.splitext(filename)
            
            target_path = original_format_path
            counter = 1
            
            while os.path.exists(target_path):
                new_filename = f"{base}_{counter}{ext}"
                target_path = os.path.join(directory, new_filename)
                counter += 1
            
            with Image.open(png_image_path) as image:
                image.save(target_path, quality=100)
            
            print(f"Restored and converted image saved to {target_path}")
            
        except IOError as e:
            print(f"Cannot convert image: {e}")
