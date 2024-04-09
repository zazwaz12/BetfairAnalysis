import json
import subprocess
import time

# Define a function to run the scripts
def run_scripts():
    # List of Python scripts to execute
    scripts = ['s3.py', 'snowflakePricing.py', 'snowflakeMapping.py', 's3.py', 'snowflakeInfo.py']
    
    # Iterate through the list of scripts and execute them sequentially
    for script in scripts:
        print(f"Running {script}")
        # Execute the script using subprocess
        completed_process = subprocess.run(['python', script], capture_output=True, text=True)
        
        # Print the output of the script
        print(f"Output of {script}:")
        print(completed_process.stdout)
        
        # Check if there's any error output
        if completed_process.stderr:
            print(f"Error in {script}:")
            print(completed_process.stderr)

# Run the 'stream.py' script to generate the JSON file
# Loop until the JSON file is not empty
while True:
    try:
        completed_process = subprocess.run(['python', 'stream.py'], capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        print("stream.py has run for 30 seconds and will run again if no data exists.")
    # Read the JSON file
    with open('newStream.json', 'r') as file:
        data = json.load(file)
    
    # Check if JSON data is empty
    if data:
        break
    else:
        print("JSON file is empty. Waiting for data...")


# Once the JSON file is not empty, run the scripts
run_scripts()
