#!/Library/Frameworks/Python.framework/Versions/3.12/bin/python3

from topic_subscriber import JSONProcessor, GPSDataProcessor
import matplotlib.pyplot as plt

bag_path = 'e0_rosbags/2023-12-06-15-32-37.bag'
gps_topic = '/tric_navigation/gps/head_data'
joystick_topic = '/tric_navigation/joystick_control'
uvc_topic = '/tric_navigation/uvc_light_status'
plc_feedback_topic = '/tric_navigation/plc_feedback'
json_map = 'json_maps/testrow.json'
output_file = 'output.png'  # Output file for the plot


class MapPlotter:
    def __init__(self, json_processor, gps_data_processor):
        self.json_processor = json_processor
        self.gps_data_processor = gps_data_processor

    def plot(self, output_file):
        fig, ax = plt.subplots()

        # Define colors, sizes, and markers for each key
        colors = {
            'rows': 'green',
            'turns': 'orange',
            'start_path': 'blue',
            'end_path': 'tomato',
            'datum': 'Black'
        }
        sizes = {
            'rows': 15,
            'turns': 7,
            'start_path': 7,
            'end_path': 7,
            'datum': 15
        }
        markers = {
            'rows': 'o',
            'turns': 'o',
            'start_path': 'o',
            'end_path': 'o',
            'datum': 'x'
        }

        # Plot data
        data = self.json_processor.data
        for key, points in data.items():
            if key == 'wing_boom_pos':  # Skip 'wing_boom_pos'
                continue
            ax.scatter([point['x'] for point in points], [point['y'] for point in points], 
                        color=colors[key], s=sizes[key], marker=markers[key], label=key.capitalize())

        # Plot GPS data
        gps_df = self.gps_data_processor.create_dataframe()
        ax.scatter(gps_df['x'], gps_df['y'], color='black', s=1, marker='_', label='GPS Data')

        ax.legend()
        plt.savefig(output_file, format='png')  # Save the plot as a PNG file

json_processor = JSONProcessor(json_map)
gps_data_processor = GPSDataProcessor(bag_path, [gps_topic])
plotter = MapPlotter(json_processor, gps_data_processor)
plotter.plot(output_file)