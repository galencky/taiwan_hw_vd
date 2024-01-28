import os
from tqdm import tqdm
import pandas as pd
import xml.etree.ElementTree as ET

def convert_xml_to_csv(date):
    input_dir = f"D:\\VD_data\\{date}\\decompressed"
    output_dir = f"D:\\VD_data\\{date}\\csv"

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Define the namespace
    namespace = {'ns1': 'http://traffic.transportdata.tw/standard/traffic/schema/'}

    def get_nested_element_text(parent, path):
        element = parent.find(path, namespace)
        return element.text if element is not None else ''

    # Get the list of XML files
    xml_files = [f for f in os.listdir(input_dir) if f.endswith('.xml')]
    total_files = len(xml_files)

    # Initialize the progress bar
    with tqdm(total=total_files) as progress:
        for file_name in xml_files:
            try:
                tree = ET.parse(os.path.join(input_dir, file_name))
                root = tree.getroot()

                # Prepare a dictionary to store data for each VDID
                data_dict = {}

                for vdlive in root.findall('.//ns1:VDLive', namespace):
                    vdid = get_nested_element_text(vdlive, 'ns1:VDID')

                    if vdid not in data_dict:
                        data_dict[vdid] = {
                            'VDID': vdid
                        }

                    for lane in vdlive.findall('.//ns1:Lane', namespace):
                        lane_id = get_nested_element_text(lane, 'ns1:LaneID')
                        speed = get_nested_element_text(lane, 'ns1:Speed')
                        occupancy = get_nested_element_text(lane, 'ns1:Occupancy')

                        data_dict[vdid][f'L{lane_id}_Speed'] = speed
                        data_dict[vdid][f'L{lane_id}_Occupancy'] = occupancy

                        # Initialize volume and speed values for S, L, T
                        data_dict[vdid][f'L{lane_id}_S_Volume'] = 0
                        data_dict[vdid][f'L{lane_id}_L_Volume'] = 0
                        data_dict[vdid][f'L{lane_id}_T_Volume'] = 0
                        data_dict[vdid][f'L{lane_id}_S_Vehicle_Speed'] = 0
                        data_dict[vdid][f'L{lane_id}_L_Vehicle_Speed'] = 0
                        data_dict[vdid][f'L{lane_id}_T_Vehicle_Speed'] = 0

                        for vehicle in lane.findall('.//ns1:Vehicle', namespace):
                            vehicle_type = get_nested_element_text(vehicle, 'ns1:VehicleType')
                            volume = get_nested_element_text(vehicle, 'ns1:Volume')
                            speed2 = get_nested_element_text(vehicle, 'ns1:Speed')

                            if vehicle_type == 'S':
                                data_dict[vdid][f'L{lane_id}_S_Volume'] = volume
                                data_dict[vdid][f'L{lane_id}_S_Vehicle_Speed'] = speed2
                            elif vehicle_type == 'L':
                                data_dict[vdid][f'L{lane_id}_L_Volume'] = volume
                                data_dict[vdid][f'L{lane_id}_L_Vehicle_Speed'] = speed2
                            elif vehicle_type == 'T':
                                data_dict[vdid][f'L{lane_id}_T_Volume'] = volume
                                data_dict[vdid][f'L{lane_id}_T_Vehicle_Speed'] = speed2

                # Create DataFrame from the data dictionary values
                df = pd.DataFrame(list(data_dict.values()))

                # Save the modified data to a CSV file
                output_file = os.path.join(output_dir, file_name.replace('.xml', '.csv'))
                df.to_csv(output_file, index=False)

                # Update the progress bar
                progress.update(1)
            except Exception as e:
                print(f"Error converting file {file_name}: {e}")