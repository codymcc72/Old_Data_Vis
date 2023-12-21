#!/Library/Frameworks/Python.framework/Versions/3.12/bin/python3

import pandas as pd
import numpy as np
import math
from topic_subscriber import JSONProcessor
from topic_subscriber import GPSDataProcessor
from topic_subscriber import JoystickDataProcessor
from topic_subscriber import UVCLightDataProcessor
from topic_subscriber import PLCFeedbackDataProcessor
from math import radians, cos, sin, asin, sqrt, atan2

bag_path = 'e0_rosbags/2023-12-06-15-32-37.bag'
gps_topic = '/tric_navigation/gps/head_data'
joystick_topic = '/tric_navigation/joystick_control'
uvc_topic = '/tric_navigation/uvc_light_status'
plc_feedback_topic = '/tric_navigation/plc_feedback'
json_map = 'json_maps/testrow.json'

class GPSDataLogger:
    def __init__(self, gps_data_processor):
        self.gps_data = GPSDataProcessor(bag_path, [gps_topic])
        self.gps_data_processor = gps_data_processor
        self.df = gps_data_processor.create_dataframe()

    def calculate_runtime(self):
        # Extract the timestamp of the first and last rows
        start_time = self.df['timestamp'].iloc[0]
        end_time = self.df['timestamp'].iloc[-1]

        # Calculate the total runtime
        total_runtime = end_time - start_time
        return total_runtime

    def calculate_distances(self):
        # Calculate the distance between consecutive points
        x = self.df['x']
        y = self.df['y']

        # Euclidean distance calculation
        distances = np.sqrt(np.diff(x)**2 + np.diff(y)**2)

        # Total distance traveled
        total_distance = np.sum(distances)
        return total_distance
    
    def find_stops(self):
        df = self.gps_data.create_dataframe()

        if df.empty:
            print("DataFrame is empty.")
            return pd.DataFrame()

        delta_x = df['x'].diff()
        delta_y = df['y'].diff()

        stop_indices = df.index[(delta_x == 0) & (delta_y == 0)].tolist()

        stop_data = []
        for stop_index in stop_indices:
            stop_duration = df.loc[stop_index, 'timestamp'] - df.loc[stop_index - 1, 'timestamp']
            stop_data.append([stop_index, df.loc[stop_index, 'y'], df.loc[stop_index, 'x'], stop_duration])

        stop_df = pd.DataFrame(stop_data, columns=['Index', 'y', 'x', 'Duration'])
        return stop_df
    
class JoystickDataLogger:
    def __init__(self, gps_data_logger):
        self.joystick_data = JoystickDataProcessor(bag_path, [joystick_topic])
        self.gps_data_logger = gps_data_logger
    
    def time_between_assists(self):
        df = self.joystick_data.merge_dataframes()

        if df.empty:
            print("DataFrame is empty. Cannot calculate time of assists.")
            return []

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

        # Create groups of consecutive True values
        groups = df['joystick_control'].ne(df['joystick_control'].shift()).cumsum()

        # Filter rows where joystick_control is True
        assist_df = df[df['joystick_control']]

        # Identify start and end indices
        assist_starts = assist_df.groupby(groups).head(1).index
        assist_ends = assist_df.groupby(groups).tail(1).index

        assist_pairs = list(zip(assist_starts, assist_ends))

        time_of_assists = [
            {
                'Assist': i + 1,
                'start_index': start,
                'end_index': end,
                'time_between': round((df.loc[end, 'timestamp'] - df.loc[start, 'timestamp']).total_seconds() / 60, 2)
            }
            for i, (start, end) in enumerate(assist_pairs)
        ]

        time_between_assists = [
            round((df.loc[prev_end, 'timestamp'] - df.loc[start, 'timestamp']).total_seconds() / 60, 2)
            for (prev_end, start) in zip(assist_ends[:-1], assist_starts[1:])
        ]

        print("Time of Assists:")
        for assist in time_of_assists:
            print(f"Assist {assist['Assist']}: 'start_index': {assist['start_index']}, 'end_index': {assist['end_index']}, 'duration': {assist['time_between']} minutes")

        print("\nTime Between Assists:")
        for time_between in time_between_assists:
            print(f"{time_between} minutes")

    def calculate_distance(self, point1, point2):
        # Calculate the distance between two points
        x_diff = point2['x'] - point1['x']
        y_diff = point2['y'] - point1['y']

        # Euclidean distance calculation
        distance = np.sqrt(x_diff**2 + y_diff**2)

        return distance
    
    def calculate_distances_and_times(self):
        df = self.joystick_data.merge_dataframes()

        # Ensure the DataFrame is not empty
        if df.empty:
            return 0, 0, 0, 0, 0

        # Convert 'timestamp' column to datetime format
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

        # Initialize variables for distances and times
        distance_in_manual = 0
        distance_in_auto = 0
        time_in_manual_minutes = 0
        time_in_auto_minutes = 0

        # Initialize variables to track the current mode and timestamp
        current_mode = df['joystick_control'].iloc[0]
        current_timestamp = df['timestamp'].iloc[0]

        for i in range(1, len(df)):
            time_diff = (df.loc[i, 'timestamp'] - current_timestamp).total_seconds() / 60
            distance = self.calculate_distance(df.loc[i-1], df.loc[i])

            if current_mode:
                time_in_manual_minutes += time_diff
                distance_in_manual += distance
            else:
                time_in_auto_minutes += time_diff
                distance_in_auto += distance

            if df.loc[i, 'joystick_control'] != current_mode:
                current_mode = df.loc[i, 'joystick_control']

            current_timestamp = df.loc[i, 'timestamp']

        # Calculate percentages
        total_time = time_in_manual_minutes + time_in_auto_minutes
        percent_time_in_auto = (time_in_auto_minutes / total_time) * 100
        percent_time_in_manual = 100 - percent_time_in_auto

        return (
            time_in_manual_minutes,
            time_in_auto_minutes,
            distance_in_manual,
            distance_in_auto,
            percent_time_in_manual,
            percent_time_in_auto
        )

class UVCLightDataLogger:
    def __init__(self, uvc_data_processor):
        self.uvc_data = UVCLightDataProcessor(bag_path, [uvc_topic])
        self.uvc_data_processor = uvc_data_processor
        self.uvc_df = uvc_data_processor.create_dataframe()

    def payload_runtime(self):
        # Convert the 'timestamp' column to datetime format
        self.uvc_df['timestamp'] = pd.to_datetime(self.uvc_df['timestamp'], unit='s')
        # Filter rows where uvc_light_status is '000' (lights off)
        lights_off_data = self.uvc_df[self.uvc_df['uvc_light_status'] == '000']
        # Filter rows where uvc_light_status is '111' (lights on)
        lights_on_data = self.uvc_df[self.uvc_df['uvc_light_status'] == '111']
        # Calculate the total time lights were off
        total_lights_off_time = lights_off_data['timestamp'].diff().sum()
        # Calculate the total time lights were on
        total_lights_on_time = lights_on_data['timestamp'].diff().sum()
        # Print the results
        print(f"Total time lights were off: {total_lights_off_time}")
        print(f"Total time lights were on: {total_lights_on_time}")

    def payload_distance(self):
        # Ensure the DataFrame is not empty
        if self.uvc_df.empty:
            print("DataFrame is empty. Cannot calculate payload distance.")
            return
        # Convert the 'timestamp' column to datetime format
        self.uvc_df['timestamp'] = pd.to_datetime(self.uvc_df['timestamp'], unit='s')
        # Filter rows where uvc_light_status is '111' (lights on)
        lights_on_data = self.uvc_df[self.uvc_df['uvc_light_status'] == '111']
        # Calculate the distance traveled when lights were on
        total_distance = 0.0
        # Iterate through consecutive rows with lights on to calculate distance
        for i in range(len(lights_on_data) - 1):
            lat1, lon1 = lights_on_data.iloc[i]['y'], lights_on_data.iloc[i]['x']
            lat2, lon2 = lights_on_data.iloc[i + 1]['y'], lights_on_data.iloc[i + 1]['x']
            # Calculate distance between consecutive rows
            distance = self.haversine_distance(lat1, lon1, lat2, lon2)
            # Accumulate the total distance
            total_distance += distance
        # Print the result
        print(f"Total distance traveled with Payload: {total_distance} meters")
        return total_distance

class JSONDataLogger:
    def __init__(self):
        self.json_data = JSONProcessor(json_map)

    def calculate_distance(self, point1, point2):
        x_diff = point2['x'] - point1['x']
        y_diff = point2['y'] - point1['y']
        return math.sqrt(x_diff**2 + y_diff**2)
    
    def calculate_ideal_time(self, points, speed):
        total_distance = sum(self.calculate_distance(points[i], points[i+1]) for i in range(len(points) - 1))
        time = total_distance / speed
        return time

    def calculate_total_distances(self):
        total_distances = {}
        for key in ['rows', 'turns', 'start_path', 'end_path']:
            points = self.json_data.data[key]
            total_distances[key] = sum(self.calculate_distance(points[i], points[i+1]) for i in range(len(points) - 1))
        return total_distances

    def ideal_times(self, treatment_speed=0.8, non_treatment_speed=0.8):
        rows_ideal_time = self.calculate_ideal_time(self.json_data.data['rows'], treatment_speed)/60
        turns_ideal_time = self.calculate_ideal_time(self.json_data.data['turns'], non_treatment_speed)/60
        start_path_ideal_time = self.calculate_ideal_time(self.json_data.data['start_path'], non_treatment_speed)/60
        end_path_ideal_time = self.calculate_ideal_time(self.json_data.data['end_path'], non_treatment_speed)/60

        return {
            'rows': rows_ideal_time,
            'turns': turns_ideal_time,
            'start_path': start_path_ideal_time,
            'end_path': end_path_ideal_time
        }
    
    def print_total_distances(self):
        total_distances = self.calculate_total_distances()
        for key, value in total_distances.items():
            print(f"Total Distance {key.capitalize()}: {round(value, 2)} meters")
    
    def print_ideal_times(self):
        ideal_times = self.ideal_times()
        for key, value in ideal_times.items():
            print(f"Ideal Time {key.capitalize()}: {round(value, 2)} minutes")

class PLCDataLogger:
    def __init__(self, bag_path, plc_feedback_topic, json_file_path):
        self.json_processor = JSONProcessor(json_file_path)
        self.plc_processor = PLCFeedbackDataProcessor(bag_path, [plc_feedback_topic], self.json_processor)
        self.dataframe = self.plc_processor.merge_dataframes()

    def find_rows(self):
        start_index = None
        rows = []
        for i, row in self.dataframe.iterrows():
            if row['treatment_area'] and start_index is None:
                start_index = i
            elif not row['treatment_area'] and start_index is not None:
                rows.append((start_index, i - 1))
                start_index = None
        if start_index is not None:
            rows.append((start_index, i))
        return rows
    
    def find_turns(self):
        turns = []
        turn_start_index = None
        prev_row_treatment_area = False
        for i, row in self.dataframe.iterrows():
            if not row['treatment_area'] and prev_row_treatment_area:
                turn_start_index = i
            elif row['treatment_area'] and turn_start_index is not None:
                turns.append((turn_start_index, i - 1))
                turn_start_index = None
            prev_row_treatment_area = row['treatment_area']
        return turns
    
    def find_start_path(self):
        start_path = []
        for i, row in self.dataframe.iterrows():
            if not row['treatment_area']:
                start_path.append(i)
            else:
                break
        return start_path
    
    def find_end_path(self):
        end_path = []
        for i in reversed(range(len(self.dataframe))):
            if not self.dataframe.iloc[i]['treatment_area']:
                end_path.append(i)
            else:
                break
        return list(reversed(end_path))

    def calculate_average_differences_rows(self, start, stop):
        df_slice = self.dataframe.iloc[start:stop+1]
        boom_diff = (df_slice['boom_position'] - df_slice['boom_pos']).abs().mean()
        left_wing_diff = (df_slice['left_wing_position'] - df_slice['left_wing_pos']).abs().mean()
        right_wing_diff = (df_slice['right_wing_position'] - df_slice['right_wing_pos']).abs().mean()
        return boom_diff, left_wing_diff, right_wing_diff
    
    def calculate_average_differences_turns(self, start, stop):
        df_slice = self.dataframe.iloc[start:stop+1]
        boom_diff = (df_slice['boom_position'] - df_slice['boom_pos']).abs().mean()
        left_wing_diff = (df_slice['left_wing_position'] - df_slice['left_wing_pos']).abs().mean()
        right_wing_diff = (df_slice['right_wing_position'] - df_slice['right_wing_pos']).abs().mean()
        return boom_diff, left_wing_diff, right_wing_diff
    
    def calculate_average_differences_start_path(self):
        start_path = self.find_start_path()
        if not start_path:
            return None, None, None
        df_slice = self.dataframe.iloc[start_path[0]:start_path[-1]+1]
        boom_diff = (df_slice['boom_position'] - df_slice['boom_pos']).abs().mean()
        left_wing_diff = (df_slice['left_wing_position'] - df_slice['left_wing_pos']).abs().mean()
        right_wing_diff = (df_slice['right_wing_position'] - df_slice['right_wing_pos']).abs().mean()
        return boom_diff, left_wing_diff, right_wing_diff
    
    def calculate_average_differences_end_path(self):
        end_path = self.find_end_path()
        if not end_path:
            return None, None, None
        df_slice = self.dataframe.iloc[end_path[0]:end_path[-1]+1]
        boom_diff = (df_slice['boom_position'] - df_slice['boom_pos']).abs().mean()
        left_wing_diff = (df_slice['left_wing_position'] - df_slice['left_wing_pos']).abs().mean()
        right_wing_diff = (df_slice['right_wing_position'] - df_slice['right_wing_pos']).abs().mean()
        return boom_diff, left_wing_diff, right_wing_diff

    def print_rows(self):
        rows = self.find_rows()
        for i, (start, stop) in enumerate(rows, start=1):
            boom_diff, left_wing_diff, right_wing_diff = self.calculate_average_differences_rows(start, stop)
            print(f"Row {i}: Start index = {start}, Stop index = {stop}, "
                  f"\nAverage differences - Boom: {boom_diff}, Left Wing: {left_wing_diff}, Right Wing: {right_wing_diff}")
            
    def print_turns(self):
        turns = self.find_turns()
        for i, (start, stop) in enumerate(turns, start=1):
            boom_diff, left_wing_diff, right_wing_diff = self.calculate_average_differences_turns(start, stop)
            print(f"Turn {i}: Start index = {start}, Stop index = {stop}, "
                  f"\nAverage differences - Boom: {boom_diff}, Left Wing: {left_wing_diff}, Right Wing: {right_wing_diff}")
            
    def print_start_path(self):
        start_path = self.find_start_path()
        if not start_path:
            print("No start path found.")
            return
        boom_diff, left_wing_diff, right_wing_diff = self.calculate_average_differences_start_path()
        print(f"Start Path: Start index = {start_path[0]}, Stop index = {start_path[-1]}, "
              f"\nAverage differences - Boom: {boom_diff}, Left Wing: {left_wing_diff}, Right Wing: {right_wing_diff}")
        
    def print_end_path(self):
        end_path = self.find_end_path()
        if not end_path:
            print("No end path found.")
            return
        boom_diff, left_wing_diff, right_wing_diff = self.calculate_average_differences_end_path()
        print(f"End Path: Start index = {end_path[0]}, Stop index = {end_path[-1]}, "
              f"\nAverage differences - Boom: {boom_diff}, Left Wing: {left_wing_diff}, Right Wing: {right_wing_diff}")

def main():

    gps_logger = GPSDataLogger(GPSDataProcessor(bag_path, [gps_topic]))
    joystick_logger = JoystickDataLogger(gps_logger)
    uvc_logger = UVCLightDataLogger(UVCLightDataProcessor(bag_path, [uvc_topic]) )
    json_logger = JSONDataLogger()
    plc_logger = PLCDataLogger(bag_path, plc_feedback_topic, json_map)
    time_in_manual_minutes, time_in_auto_minutes, distance_in_manual, distance_in_auto, percent_time_in_manual, percent_time_in_auto = joystick_logger.calculate_distances_and_times()

    #JSON SUMMARY
    print("\nJSON Distance Summary\n")
    json_logger.print_total_distances()

    #JSON Ideal Time SUMMARY
    print("\nJSON Ideal Time Summary\n")
    json_logger.print_ideal_times()

    #Runtime SUMMARY
    print("\n__________________________\n")
    print("\nRuntime Summary\n")
    total_runtime = gps_logger.calculate_runtime()
    print(f"Total Runtime: {round(total_runtime, 2)}")

    #Distance SUMMARY
    print("\n__________________________\n")
    print("\nDistance Summary\n")
    total_distance = gps_logger.calculate_distances()
    print(f"Total Distance Traveled: {round(total_distance, 2)} meters")

    #Stop SUMMARY
    print("\n__________________________\n")
    print("\nStop Summary\n")
    print(gps_logger.find_stops())

    #Mode SUMMARY
    
    print("\n__________________________\n")
    print(f"\nMode Summary\n")
    print(f"Distance in manual mode: {round(distance_in_manual, 2)} meters")
    print(f"Distance in auto mode: {round(distance_in_auto, 2)} meters")
    print(f"Total distance: {round(distance_in_manual + distance_in_auto, 2)} meters")
    print(f"Time in auto mode: {round(time_in_auto_minutes, 2)} minutes")
    print(f"Time in manual mode: {round(time_in_manual_minutes, 2)} minutes")
    print(f"Total time: {round(time_in_manual_minutes + time_in_auto_minutes, 2)} minutes")
    print(f"Percentage of time in manual mode: {round(percent_time_in_manual, 2)}%")
    print(f"Percentage of time in auto mode: {round(percent_time_in_auto, 2)}%")

    #Payload SUMMARY
    print("\n__________________________\n")
    print("\nPayload Summary\n")
    uvc_logger.payload_runtime()
    uvc_logger.payload_distance()
    
    print("\n__________________________\n")
    print("\nPLC Summary\n")
    plc_logger.print_rows()
    plc_logger.print_turns()
    plc_logger.print_start_path()
    plc_logger.print_end_path()

if __name__ == "__main__":
    main()