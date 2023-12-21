#!/Library/Frameworks/Python.framework/Versions/3.12/bin/python3

from reportlab.platypus import SimpleDocTemplate, Paragraph, Image
from reportlab.lib.pagesizes import letter
import subprocess
import os
from PIL import Image as PilImage
from reportlab.platypus.flowables import Flowable


def run_script(script_name, output_file):
    elements = []
    try:
        # Run the script and capture both stdout and stderr
        result = subprocess.run(['python3', script_name], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        # If the output file exists (i.e., the script generated a PNG file), resize it, add it to the elements list
        if os.path.exists(output_file):
            # Open the image file
            img = PilImage.open(output_file)
            # Resize the image
            img = img.resize((int(img.width / 2), int(img.height / 2)))
            # Save the resized image back to the file
            img.save(output_file)
            # Add the image to the elements list
            elements.append(Image(output_file))

        # Replace newline characters with <br/> tags and underscores with <hr/>
        formatted_text = result.stdout.replace('\n', '<br/>').replace('\n__________________________\n', '<hr/>')

        # Add the output of the script to the elements list
        text = Paragraph(formatted_text)
        elements.append(text)

        print(f"Script execution completed.")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e}")
    
    return elements

# Run the map_plotter.py script to generate a PNG file
elements1 = run_script('map_plotter.py', 'output.png')

# Run the data_processor.py script to generate output text
elements2 = run_script('data_processor.py', '')

# Combine elements from both scripts
elements = elements1 + elements2

# Create a PDF
doc = SimpleDocTemplate('output.pdf', pagesizes=letter)

# Build the PDF with the elements
doc.build(elements)

# Delete the PNG file
if os.path.exists('output.png'):
    os.remove('output.png')

print(f"Output saved to output.pdf")