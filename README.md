# User Behavior Analysis: Identify High-Value Premium Features

This project generates CSV files simulating user events for an eligibility study. The main script, [gen_csv.py](gen_csv.py), constructs a CSV file of user events based on randomized timestamps and event names.

## Files

- **[gen_csv.py](gen_csv.py)**  
  Main script that generates `before_sub_events.csv` using random events and timestamps.

- **before_sub_events.csv**  
  Generated CSV file containing user events.

- **eligibility_traces_rank.csv**  
  (Optional) A CSV file related to eligibility traces ranking.

- **[et.py]((gen_csv.py))**  
  Additional Python script (purpose to be defined).

- **[Main Idea.pdf](Main Idea.pdf)**  
  Supporting documentation related to model-free prediction.

## How It Works

1. **Random Timestamp Generation:**  
   The function [`generate_timestamp`](gen_csv.py) generates a random timestamp within 7 days from a given start date.

2. **Data Generation:**  
   - The CSV schema is defined with headers for user ID, subscribe timestamp, session ID, event details, etc.  
   - For 15 unique users, the script generates a random number of sessions and assigns randomized events based on weighted values.  
   - Data is written to the file `before_sub_events.csv`.

## Running the Script

Ensure you have Python installed. From the terminal, navigate to the project directory and run:

```sh
python gen_csv.py
python et.py
