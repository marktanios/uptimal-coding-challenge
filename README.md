# uptimal-coding-challenge
Uptimal's coding challenge submission for Mark Tanios

## Intro

### Opening words
Thank you for letting me get the chance of doing this coding challenge. 
I really enjoyed it! 

## Solution
### Technologies

- For dealing with APIs and downloading files, I chose `requests`
- I chose `pandas` for dealing with data manipulation
- **BONUS**: I chose `tqdm` for progress bar in the download

### Solution Details

I've chosen pandas as the main data manipulation framework because:
1. Ease of Use – Pandas provides a high-level, intuitive API for handling structured data, making operations like filtering, grouping, and merging easy.
2. Efficient Data Processing: It is optimized for handling large datasets efficiently, which is crucial for analyzing car crash data.
3. File Formats Seamless Integration: It works with CSVs and Parquet formats

### Project Structure
```raw
   
   uptimal-coding-challenge
   ├── .venv/                           # files needed for python venv
   │     └── bin/
   ├── config/                          # configuration folder
   │     └── settings.yaml              # setting file which contains all the configuration
   ├── data/                            # data folder which will have the downloaded data sources
   │     └──  output/                   # output data folder which cotains the output of the processing parquet file
   ├── notebooks/
   │     └── nyc_crash_analysis.ipynb   # Jupyter Notebook to do some analysis work
   ├── src/                             # the main folder for coding challenge
   │    ├── lib/                        # configuration file for the app
   │    │    └── utils.py               # helper code to handle other operations like save_to_parquet
   │    │    └── fetch_data.py          # helper code to fetch the data from NYC website and Holiday website
   │    │    └── transform.py           # helper code to transform the dataframe
   │    └── main.py                     # main python file to get the crash dataset and holiday and merge them
   ├── .gitignore
   ├── LICENSE.txt
   ├── README.rst
   └── requirements.txt
```
## Steps to test the solution

### Clone the repo to your local machine
In the terminal type:
```bash
$ git clone git@github.com:marktanios/uptimal-coding-challenge.git
```

### Install `python virtual environment`

Navigate to the cloned project location and just type the following command in the terminal. (Assuming you are using bash)
```bash
$ python3 -m venv venv
```

Then just activate it using the below command.
```bash
$ source venv/bin/activate
```

Then install all the needed python packages.
```bash
$ (env) pip install -r requirements.txt
```
Then, in any IDE, run the `main.py` file.

## Deliverables:
### Core Functionality:
- [x] Download and read the collision dataset csv as pandas dataframe
- [x] Normalize column names: lowercase, strip spaces, replace spaces & hyphens with underscores
- [x] Fetch holiday data from holiday API and merge it with the year calendar
- [x] Merge collision and holiday dataframes
- [x] Save output to parquet
- [x] README file

## Notes:
- In the config file, you can change many settings like: `collision_dataset_url`, `collision_dataset_path` or the `filtration year`
- In the `get_holiday_data` function, the logic creates a 365 day rows represents all the days in a calendar year and the join it with the holiday data to have a full-range dataframe which can be easily merged with the collisions dataframe
- In the `get_collision_dataset` function, the logic:
  1. check if the file is already downloaded and skip the download
  2. then download the crash file if it's not downloaded
  3. filter the data to specified year in the configuration
  4. return the filtered data

### Finally, thank you for this opportunity and have a great day! :rocket: 