#!/Library/Frameworks/Python.framework/Versions/3.12/bin/python3

import json
import rosbag
import math
import pandas as pd
import matplotlib.pyplot as plt
from scipy.spatial import KDTree

class JSONProcessor:

    def __init__(self, json_file_path):
        with open(json_file_path, "r") as json_file:
            self.json_data = json.load(json_file)

        self.validate_json_structure()
        self.data = self.extract_all()

    def validate_json_structure(self):
        if 'points' not in self.json_data or 'datum' not in self.json_data:
            raise ValueError("Invalid JSON structure. Missing 'points' or 'datum'.")

    def extract_all(self):
        treatment_area_indices = [i for i, point in enumerate(self.json_data['points']) if point.get('treatment_area', False)]
        first_treatment_area_index = next((i for i, point in enumerate(self.json_data['points']) if point.get('treatment_area', False)), None)
        last_point_index = len(self.json_data['points']) - 1
        last_treatment_area_index = next((i for i in range(last_point_index, -1, -1) if self.json_data['points'][i].get('treatment_area', False)), None)

        data = {
            'rows': [
                {
                    'x': (point['head']['position']['x']),
                    'y': (point['head']['position']['y'])
                }
                for point in self.json_data['points'] if point.get('treatment_area', False)
            ],
            'turns': [
                {
                    'x': (point['head']['position']['x']),
                    'y': (point['head']['position']['y'])
                }
                for i in range(1, len(treatment_area_indices))
                for point in self.json_data['points'][treatment_area_indices[i - 1] + 1: treatment_area_indices[i]]
            ],
            'start_path': [
                {
                    'x': (point['head']['position']['x']),
                    'y': (point['head']['position']['y'])
                }
                for point in self.json_data['points'][:first_treatment_area_index + 1]
            ],
            'end_path': [
                {
                    'x': (point['head']['position']['x']),
                    'y': (point['head']['position']['y'])
                }
                for point in self.json_data['points'][last_treatment_area_index + 1:]
            ],
            'datum': [
                {
                    'x': 0,
                    'y': 0
                }
            ],
            'wing_boom_pos': [
                {
                    'x': (point['point']['position']['x']),
                    'y': (point['point']['position']['y']),
                    'boom_pos': point['boom_position'],
                    'left_wing_pos': point['left_wing_position'],
                    'right_wing_pos': point['right_wing_position']
                }
                for point in self.json_data['wing_boom_position']
            ]
        }

        return data
    
    def create_points_dataframe(self):
        data = {
            'x': [],
            'y': [],
            'treatment_area': []
        }

        for point in self.json_data['points']:
            data['x'].append(point['head']['position']['x'])
            data['y'].append(point['head']['position']['y'])
            data['treatment_area'].append(point.get('treatment_area', False))

        df = pd.DataFrame(data)
        return df

class GPSDataProcessor:

    def __init__(self, bag_path, topics):
        self.bag_path = bag_path
        self.topics = topics
        self.messages = self.load_rosbag()

    def load_rosbag(self):
        bag = rosbag.Bag(self.bag_path)
        messages = {topic: [] for topic in self.topics}

        for topic, msg, t in bag.read_messages(topics=self.topics):
            messages[topic].append(msg)

        bag.close()
        return messages
    
    def gps_to_meters(self, lon1, lat1, lon2, lat2):
        R = 6371  # radius of earth at equator (km)
        alpha1 = lat1  # alpha (deg)
        r1 = R * math.cos(math.pi * alpha1/180) 
        l1 = r1 * math.pi/180
        L1 = R * math.pi/180

        alpha2 = lat2  # alpha (deg)
        r2 = R * math.cos(math.pi * alpha2/180) 
        l2 = r2 * math.pi/180
        L2 = R * math.pi/180

        x_km = L1 * (lat2 - lat1)
        y_km = l1 * (lon2 - lon1)
        x_m = x_km * 1000  # convert km to meters
        y_m = y_km * 1000

        return x_m, y_m

    def create_dataframe(self):
        data = {'x': [], 'y': [], 'timestamp': []}

        # Find the start time of the recording
        start_time = pd.to_datetime(self.messages['/tric_navigation/gps/head_data'][0].header.stamp.to_sec(), unit='s')

        for gps_msg in self.messages['/tric_navigation/gps/head_data']:
            # Convert the GPS coordinates to distances from the datum
            x, y = self.gps_to_meters(json_map.json_data['datum']['longitude'], json_map.json_data['datum']['latitude'], gps_msg.longitude, gps_msg.latitude)
            data['x'].append(x)
            data['y'].append(y)

            # Calculate timestamp relative to the recording start, rounded to seconds with 2 decimal places
            timestamp_relative = pd.to_datetime(gps_msg.header.stamp.to_sec(), unit='s') - start_time
            timestamp_seconds = timestamp_relative.total_seconds()
            data['timestamp'].append(timestamp_seconds)

        df = pd.DataFrame(data)
        return df
        
class JoystickDataProcessor:
    def __init__(self, bag_path, topics):
        self.bag_path = bag_path
        self.topics = topics
        self.messages = self.load_rosbag()

    def load_rosbag(self):
        bag = rosbag.Bag(self.bag_path)
        messages = {topic: [] for topic in self.topics}

        for topic, msg, t in bag.read_messages(topics=self.topics):
            messages[topic].append(msg)

        bag.close()
        return messages

    def create_dataframe(self, assumed_frequency=12.3):
        data = {'joystick_control': [], 'timestamp': []}

        # Assuming messages are sorted by timestamp
        time_since_start = 0  # Initialize time_since_start to 0

        for joystick_msg in self.messages['/tric_navigation/joystick_control']:
            data['joystick_control'].append(joystick_msg.data)

            # Calculate timestamp based on the assumed frequency, rounded to seconds with 2 decimal places
            timestamp_seconds = time_since_start
            data['timestamp'].append(timestamp_seconds)
            time_since_start += 1 / assumed_frequency  # Increase time_since_start by the reciprocal of the assumed frequency

        df = pd.DataFrame(data)
        return df
    
    def create_gps_dataframe(self):
        gps_processor = GPSDataProcessor(self.bag_path, ['/tric_navigation/gps/head_data'])
        gps_df = gps_processor.create_dataframe()
        return gps_df
        
    def merge_dataframes(self):

        gps_df = self.create_gps_dataframe()
        joystick_df = self.create_dataframe()
        # Merge dataframes
        merged_df = pd.merge_asof(joystick_df, gps_df, on='timestamp', direction='nearest')

        # Calculate the differences in longitude (x) and latitude (y) between consecutive rows
        delta_x = merged_df['x'].diff()
        delta_y = merged_df['y'].diff()

        # Create a mask for rows where both longitude and latitude don't change
        mask = (delta_x == 0) & (delta_y == 0)

        # Remove rows where both longitude and latitude don't change
        merged_df = merged_df[~mask]

        # Reset the index to have a continuous sequence
        merged_df = merged_df.reset_index(drop=True)

        return merged_df

class UVCLightDataProcessor:
    def __init__(self, bag_path, topics):
        self.bag_path = bag_path
        self.topics = topics
        self.messages = self.load_rosbag()

    def load_rosbag(self):
        bag = rosbag.Bag(self.bag_path)
        messages = {topic: [] for topic in self.topics}

        for topic, msg, t in bag.read_messages(topics=self.topics):
            messages[topic].append(msg)

        bag.close()
        return messages

    def create_dataframe(self, assumed_frequency=12.3):
        data = {'uvc_light_status': [], 'timestamp': []}

        # Assuming messages are sorted by timestamp
        time_since_start = 0  # Initialize time_since_start to 0

        for uvc_msg in self.messages['/tric_navigation/uvc_light_status']:
            data['uvc_light_status'].append(uvc_msg.data)

            # Calculate timestamp based on the assumed frequency, rounded to seconds with 2 decimal places
            timestamp_seconds = time_since_start
            data['timestamp'].append(timestamp_seconds)
            time_since_start += 1 / assumed_frequency  # Increase time_since_start by the reciprocal of the assumed frequency

        df = pd.DataFrame(data)
        return df
    
    def create_gps_dataframe(self):
        gps_processor = GPSDataProcessor(self.bag_path, ['/tric_navigation/gps/head_data'])
        gps_df = gps_processor.create_dataframe()
        return gps_df
        
    def merge_dataframes(self):

        gps_df = self.create_gps_dataframe()
        uvc_df = self.create_dataframe()
        # Merge dataframes
        merged_df = pd.merge_asof(uvc_df, gps_df, on='timestamp', direction='nearest')

        # Calculate the differences in longitude (x) and latitude (y) between consecutive rows
        delta_x = merged_df['x'].diff()
        delta_y = merged_df['y'].diff()

        # Create a mask for rows where both longitude and latitude don't change
        mask = (delta_x == 0) & (delta_y == 0)

        # Remove rows where both longitude and latitude don't change
        merged_df = merged_df[~mask]

        # Reset the index to have a continuous sequence
        merged_df = merged_df.reset_index(drop=True)

        return merged_df

class PLCFeedbackDataProcessor:
    def __init__(self, bag_path, topics, json_data):
        self.bag_path = bag_path
        self.topics = topics
        self.messages = self.load_rosbag()
        self.json_data = json_data

    def load_rosbag(self):
        bag = rosbag.Bag(self.bag_path)
        messages = {topic: [] for topic in self.topics}

        for topic, msg, t in bag.read_messages(topics=self.topics):
            messages[topic].append(msg)

        bag.close()
        return messages

    def create_dataframe(self, assumed_frequency=10.6):
        data = {'boom_position': [],'left_wing_position':[], 'right_wing_position':[] , 'timestamp': []}

        # Assuming messages are sorted by timestamp
        time_since_start = 0  # Initialize time_since_start to 0

        for plc_msg in self.messages['/tric_navigation/plc_feedback']:
            data['boom_position'].append(plc_msg.boom_position)
            data['left_wing_position'].append(plc_msg.left_wing_position)
            data['right_wing_position'].append(plc_msg.right_wing_position)

            # Calculate timestamp based on the assumed frequency, rounded to seconds with 2 decimal places
            timestamp_seconds = time_since_start
            data['timestamp'].append(timestamp_seconds)
            time_since_start += 1 / assumed_frequency  # Increase time_since_start by the reciprocal of the assumed frequency

        df = pd.DataFrame(data)
        return df
    
    def create_gps_dataframe(self):
        gps_processor = GPSDataProcessor(self.bag_path, ['/tric_navigation/gps/head_data'])
        gps_df = gps_processor.create_dataframe()
        return gps_df
    
    def create_wing_boom_dataframe(self):
        data = {
            'x': [],
            'y': [],
            'boom_pos': [],
            'left_wing_pos': [],
            'right_wing_pos': []
        }

        json_data = self.json_data.data

        for point in json_data['wing_boom_pos']:
            data['x'].append(point['x'])
            data['y'].append(point['y'])
            data['boom_pos'].append(point['boom_pos'])
            data['left_wing_pos'].append(point['left_wing_pos'])
            data['right_wing_pos'].append(point['right_wing_pos'])

        df = pd.DataFrame(data)
        return df
        
    def merge_dataframes(self):
        gps_df = self.create_gps_dataframe()
        plc_df = self.create_dataframe()
        wing_boom_df = self.create_wing_boom_dataframe()
        points_df = json_map.create_points_dataframe()

        # Merge gps_df and plc_df
        merged_df = pd.merge_asof(plc_df, gps_df, on='timestamp', direction='nearest')

        # Create a KDTree from wing_boom_df
        tree_wing_boom = KDTree(wing_boom_df[['x', 'y']])
        # Find indices of the nearest points in wing_boom_df for each point in merged_df
        _, indices_wing_boom = tree_wing_boom.query(merged_df[['x', 'y']])
        # Add columns from wing_boom_df to merged_df based on the indices
        for column in ['boom_pos', 'left_wing_pos', 'right_wing_pos']:
            merged_df[column] = wing_boom_df[column].iloc[indices_wing_boom].values

        # Create a KDTree from points_df
        tree_points = KDTree(points_df[['x', 'y']])
        # Find indices of the nearest points in points_df for each point in merged_df
        _, indices_points = tree_points.query(merged_df[['x', 'y']])
        # Add 'treatment_area' column from points_df to merged_df based on the indices
        merged_df['treatment_area'] = points_df['treatment_area'].iloc[indices_points].values

        # Drop duplicate rows based on 'x' and 'y' columns
        merged_df = merged_df.drop_duplicates(subset=['x', 'y'])
        merged_df = merged_df.reset_index(drop=True)

        return merged_df


json_map = JSONProcessor('json_maps/testrow.json')
json_data = json_map.data
gps_data = GPSDataProcessor('e0_rosbags/2023-12-06-15-32-37.bag', ['/tric_navigation/gps/head_data'])
joystick_data = JoystickDataProcessor('e0_rosbags/2023-12-06-15-32-37.bag', ['/tric_navigation/joystick_control'])
uvc_data = UVCLightDataProcessor('e0_rosbags/2023-12-06-15-32-37.bag', ['/tric_navigation/uvc_light_status']) 
plc_data = PLCFeedbackDataProcessor('e0_rosbags/2023-12-06-15-32-37.bag', ['/tric_navigation/plc_feedback'], json_map)