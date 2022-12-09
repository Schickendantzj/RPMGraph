import re

import pandas as pd
import matplotlib.pyplot as plt
import os

__FILENAMES__ = {
    "foreign": "-foreign-",
    "self": "-self-",
    "download": "-throughput-download",
    "upload": "-throughput-upload"
}

# Duration is measured in Seconds now



def secondsSinceStart(dfs, start):
    """
    Adds "SecondsSinceStart" column to all dataframes
    :param dfs:
    :param start:
    :return:
    """
    for df in dfs:
        df["SecondsSinceStart"] = (df["CreationTime"]-start).apply(pd.Timedelta.total_seconds)

def findEarliest(dfs):
    """
    Assumes sorted dfs
    :param dfs:
    :return:
    """
    earliest = dfs[0]["CreationTime"].iloc[0]
    for df in dfs:
        if earliest > df["CreationTime"].iloc[0]:
            earliest = df["CreationTime"].iloc[0]
    return earliest


def timeSinceStart(dfs, start):
    """
    Adds "TimeSinceStart" column to all dataframes
    :param dfs:
    :param start:
    :return:
    """
    for df in dfs:
        df["TimeSinceStart"] = df["CreationTime"]-start

def probeClean(df):
    # ConnRTT and ConnCongestionWindow refer to Underlying Connection
    df.columns = ["CreationTime", "NumRTT", "Duration", "ConnRTT", "ConnCongestionWindow", "Type", "Empty"]
    df = df.drop(columns=["Empty"])
    df["CreationTime"] = pd.to_datetime(df["CreationTime"], format="%m-%d-%Y-%H-%M-%S.%f")
    df["Type"] = df["Type"].apply(str.strip)
    # df["TimeSinceStart"] = df["CreationTime"]-df["CreationTime"][0]
    # df["SecondsSinceStart"] = df["TimeSinceStart"].apply(pd.Timedelta.total_seconds)
    df["ADJ_Duration"] = df["Duration"] / df["NumRTT"]
    df = df.sort_values(by=["CreationTime"])
    return df


def throughputClean(df):
    df.columns = ["CreationTime", "Throughput", "Empty"]
    df = df.drop(columns=["Empty"])
    df["CreationTime"] = pd.to_datetime(df["CreationTime"], format="%m-%d-%Y-%H-%M-%S.%f")
    # df["TimeSinceStart"] = df["CreationTime"] - df["CreationTime"][0]
    # df["SecondsSinceStart"] = df["TimeSinceStart"].apply(pd.Timedelta.total_seconds)
    df["ADJ_Throughput"] = df["Throughput"] / 1000000
    df = df.sort_values(by=["CreationTime"])
    return df

def make90Percentile(df):
    df = df.sort_values(by=["ADJ_Duration"])
    df = df.reset_index()
    df = df.iloc[:int(len(df)*.9)]
    df = df.sort_values(by=["CreationTime"])
    return df


def main(title, paths):
    # Data Ingestion
    foreign = pd.read_csv(paths["foreign"])
    self = pd.read_csv(paths["self"])
    download = pd.read_csv(paths["download"])
    upload = pd.read_csv(paths["upload"])

    # Data Cleaning
    foreign = probeClean(foreign)
    self = probeClean(self)
    download = throughputClean(download)
    upload = throughputClean(upload)

    selfUp = self[self["Type"] == "SelfUp"]
    selfUp = selfUp.reset_index()
    selfDown = self[self["Type"] == "SelfDown"]
    selfDown = selfDown.reset_index()

    # Moving Average
    foreign["DurationMA5"] = foreign["ADJ_Duration"].rolling(window=5).mean()
    selfUp["DurationMA5"] = selfUp["ADJ_Duration"].rolling(window=5).mean()
    selfDown["DurationMA5"] = selfDown["ADJ_Duration"].rolling(window=5).mean()

    # Normalize
    dfs = [foreign, selfUp, selfDown, download, upload]
    timeSinceStart(dfs, findEarliest(dfs))
    secondsSinceStart(dfs, findEarliest(dfs))

    yCol = "SecondsSinceStart"

    # Graphing Complete
    fig, ax = plt.subplots()
    ax.set_title(title)
    ax.plot(foreign[yCol], foreign["ADJ_Duration"], "b.", label="foreign")
    ax.plot(selfUp[yCol], selfUp["ADJ_Duration"], "r.", label="selfUP")
    ax.plot(selfDown[yCol], selfDown["ADJ_Duration"], "c.", label="selfDOWN")
    ax.plot(foreign[yCol], foreign["DurationMA5"], "b--", label="foreignMA")
    ax.plot(selfUp[yCol], selfUp["DurationMA5"], "r--", label="selfUPMA")
    ax.plot(selfDown[yCol], selfDown["DurationMA5"], "c--", label="selfDOWNMA")
    ax.set_ylim([0, max(foreign["ADJ_Duration"].max(), self["ADJ_Duration"].max())])
    ax.legend(loc="upper left")

    secax = ax.twinx()
    secax.plot(download[yCol], download["ADJ_Throughput"], "g-", label="download (MB/s)")
    secax.plot(upload[yCol], upload["ADJ_Throughput"], "y-", label="upload (MB/s)")
    secax.legend(loc="upper right")


    ######### Graphing Removing 90th Percentile
    selfUp = make90Percentile(selfUp)
    selfDown = make90Percentile(selfDown)
    foreign = make90Percentile(foreign)

    # Recalculate MA
    foreign["DurationMA5"] = foreign["ADJ_Duration"].rolling(window=5).mean()
    selfUp["DurationMA5"] = selfUp["ADJ_Duration"].rolling(window=5).mean()
    selfDown["DurationMA5"] = selfDown["ADJ_Duration"].rolling(window=5).mean()

    # Graphing Complete
    fig, ax = plt.subplots()
    ax.set_title(title + " 90th Percentile (ordered lowest to highest duration)")
    ax.plot(foreign[yCol], foreign["ADJ_Duration"], "b.", label="foreign")
    ax.plot(selfUp[yCol], selfUp["ADJ_Duration"], "r.", label="selfUP")
    ax.plot(selfDown[yCol], selfDown["ADJ_Duration"], "c.", label="selfDOWN")
    ax.plot(foreign[yCol], foreign["DurationMA5"], "b--", label="foreignMA")
    ax.plot(selfUp[yCol], selfUp["DurationMA5"], "r--", label="selfUPMA")
    ax.plot(selfDown[yCol], selfDown["DurationMA5"], "c--", label="selfDOWNMA")
    ax.set_ylim([0, max(foreign["ADJ_Duration"].max(), selfUp["ADJ_Duration"].max(), selfDown["ADJ_Duration"].max())])
    ax.legend(loc="upper left")

    secax = ax.twinx()
    secax.plot(download[yCol], download["ADJ_Throughput"], "g-", label="download (MB/s)")
    secax.plot(upload[yCol], upload["ADJ_Throughput"], "y-", label="upload (MB/s)")
    secax.legend(loc="upper right")

def findFiles(dir):
    matches = {}

    files = os.listdir(dir)
    for file in files:
        if os.path.isfile(dir+file):
            for name in __FILENAMES__:
                regex = "(?P<start>.*)(?P<type>" + __FILENAMES__[name] + ")(?P<end>.*)"
                match = re.match(regex, file)
                if match is not None:
                    start = match.group("start")
                    end = match.group("end")
                    if start not in matches:
                        matches[start] = {}
                    if end not in matches[start]:
                        matches[start][end] = {}
                    if name in matches[start][end]:
                        print("ERROR ALREADY FOUND A FILE THAT HAS THE SAME MATCHING")
                    matches[start][end][name] = dir+file
    return matches

def generatePaths():
    return {
        "foreign": "",
        "self": "",
        "download": "",
        "upload": ""
    }

def makeGraphs(files):
    for start in files:
        x = 0
        for end in files[start]:
            # Check if it contains all file fields
            containsALL = True
            for key in __FILENAMES__:
                if key not in files[start][end]:
                    containsALL = False
            # If we don't have all files then loop to next one
            if not containsALL:
                continue

            main(start + " - " + str(x), files[start][end])
            x += 1

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    paths = generatePaths()

    files = findFiles("./Data/After Seperation/")
    print(files)
    makeGraphs(files)

    plt.show()