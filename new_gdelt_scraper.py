import pandas as pd
import os
import requests
import numpy as np
from newspaper import fulltext
import articleDateExtractor
#from contextlib import closing
from user_agent import generate_user_agent
from multiprocessing import Pool, cpu_count

# Set working directory
work_dir = os.path.dirname(os.path.realpath(__file__)) #This should return the directory path of this scipt
os.chdir(work_dir)

# Load in GDELT csv file: requiremnt for analysis below: csv file must contain globaleventid & sourceurl
df = pd.read_csv("url_GDELT.csv")

# check for duplicates in globaleventid
print(df.duplicated('globaleventid').sum())

def gdelt_cleaner(df):
    """
    This function takes a GDELT dataframe (df) and returns the "cleaned" dataframe with:
    1.) columns globaleventid & sourceurl
    2.) only http(s) sourceurl observations
    3.) ID in front of all "globaleventid"s
    """
    # 1.) clean from observations without http(s) urls
    df['ind'] = df['sourceurl'].str.contains('^http.*', regex=True)
    df = df[df['ind']==True]

    # 2.) File names can't start with a number: add ID in front of globaleventids
    df['globaleventid'] = 'ID' + df['globaleventid'].astype(str)

    # 2.) keep globaleventid & sourceurl columns
    df = df[['globaleventid', 'sourceurl']]

    return df

def gdelt_extractor(df, headers={'User-Agent': generate_user_agent(device_type="desktop", os=('mac', 'linux'))}, timeout=10):
    """
    This function takes a GDELT dataframe and:
    1.) extracts the html file from the sourceurl
    2.) extracts text and publication date of the article
    3.) saves the text file on the drive and names it after the
        globaliventid + Article_Date

    Parameters
    ==============
        df   :    the dataframe
        headers:  HTTP User-Agent header. By default a "random" HTTP User-Agent
                  header is generated.
        timeout:  Stop waiting for a response after a given number of seconds.
                  By default, wait for 10 seconds
    """
    # loop through urls
    for row in df.itertuples():
        try:
            #with closing(requests.get(row.sourceurl, timeout=timeout, headers=headers)) as res:
            res = requests.get(row.sourceurl, timeout=timeout, headers=headers)
            res.raise_for_status()
            html = res.text
            text = fulltext(html)
            publish_date = str(articleDateExtractor.extractArticlePublishedDate(html))
            name = [row.globaleventid, '.txt']
            with open(''.join(name), "w") as text_file:
                print(f'publication date: {publish_date}\n{text}', file=text_file)
        except Exception as exc:
            print('There was a problem: %s' % (exc))

def parallelize_dataframe(df, func, num_partitions = cpu_count()-1):
    """
    1.) Partition dataframe. By default, the number of partitions to split the
        dataframe is equal to the number of cores -1 (to prevent freezing machine).
    2.) Apply a function separately to each part of the dataframe, in parallel.

    Parameters
    ==============
        df   :          The dataframe
        func :          The function to be applied
        num_partitions: Number of partitions to split dataframe.
    """
    df_split = np.array_split(df, num_partitions)
    with Pool(num_partitions) as pool:
        pool.map(func, df_split)

### RUN CODE ###

# clean dataframe
df = df_cleaner(df)

### IMPORTANT ###
# just for test run
df = df[0:20]

# parallize text dumping task
parallelize_dataframe(df, gdelt_extractor)

print("Task completed")
