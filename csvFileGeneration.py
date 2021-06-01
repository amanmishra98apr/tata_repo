import pandas as pd
import re
import collections
import glob
import schedule
from datetime import date
# from csvFileTransfer import dataTransferToRemoteServer
from logInfo import LoggerSetup
import os


logger = LoggerSetup(loggerName=str(os.path.realpath('__file__'))).getLogger()


# read a file
def readFile(filePath):
    file_df = pd.read_csv(filePath, compression='gzip', header=None)
    return file_df.values.tolist()


# convert nested list into single list
def flatten(lis):
     for item in lis:
         if isinstance(item, collections.Iterable) and not isinstance(item, str):
             for x in flatten(item):
                 yield x
         else:
             yield item


# remove spaces from elements of list
def removeBlank(filePath):
    file_data = readFile(filePath)
    new_file_df = list(flatten(file_data))
    newList = [line.strip(' ') for line in new_file_df]
    return newList


# find filename
def findFileName(file_df):
    file_name = file_df[0].split('[')[1].split(']')[0]
    return file_name


# file date
def findFileDate(file_df):
    file_date = file_df[0].split('generated on')[1].split('-')[0].strip()
    return file_date


# CDR count
def findCdrCount(file_df):
    CDR_count = file_df[-4].split('Event Record Count:')[1].split('-')[0].strip()
    return CDR_count


# Data CDR
def findDataCdr(file_df):
    DATA_cdr = file_df.count('<TlnDataEvent>')
    return DATA_cdr


# SMS count
def findSmsCount(file_df):
    sms_code = [2100, 2200]
    sms_count = 0
    for code in sms_code:
        sms_count = sms_count + file_df.count('<Service>{0}</Service>'.format(code))
    return sms_count


# voice CDR
def findVoiceCdr(file_df):
    voice_code = [1100, 1200, 1300]
    voice_cdr = 0
    for code in voice_code:
        voice_cdr = voice_cdr + file_df.count('<Service>{0}</Service>'.format(code))
    return voice_cdr


# Data in mb
def findDataInMb(file_df):
    flag = 0
    data = 0
    for element in file_df:
        if flag == 1:
            result = re.search("^<RatingAmount>.*</RatingAmount>$", element)
            if result != None:
                data = data + eval(result.string.split('>')[1].split('<')[0])
        #             print(data)

        if element == "<TlnDataEvent>":
            flag = 1

        elif element == "</TlnDataEvent>":
            flag = 0

    return data


# voice in minute

def findVoiceinMin(file_df):
    flag = 0
    data = 0
    for element in file_df:
        if flag == 2:
            result = re.search("^<RatingAmount>.*</RatingAmount>$", element)
            if result != None:
                data = data + eval(result.string.split('>')[1].split('<')[0])
        #             print(data)

        if element == "<TlnVoiceSMSEvent>":
            flag = 1
        elif element == "</TlnVoiceSMSEvent>":
            flag = 0
        if flag == 1:
            if element == "<Service>{0}</Service>".format(1100) or element == "<Service>{0}</Service>".format(
                    1200) or element == "<Service>{0}</Service>".format(1300):
                flag = 2
    #                 print(element)

    return data

# transection processed
def findTransectionProcessed(file_df):
    transection_processed = file_df[-5].split('Transactions processed:')[1].split('-')[0].strip()
    return transection_processed


# total event count
def findTotalEventCount(file_df):
    total_event_count = file_df[-3].split('Total Event Count:')[1].split('-')[0].strip()
    return total_event_count


# Total Notification Count
def findTotalNotificationCount(file_df):
    Total_Notification_Count = file_df[-2].split('Total Notification Count:')[1].split('-')[0].strip()
    return Total_Notification_Count


# Total Balance Tracking Data Count
def findTotalBalanceTrackingDataCount(file_df):
    Total_Balance_Tracking_Data_Count = file_df[-1].split('Total Balance Tracking Data Count:')[1].split('-')[0].strip()
    return Total_Balance_Tracking_Data_Count


# actual usage for min
def findActualUsageForMin(file_df):
    flag = 0
    data = 0
    count = 0
    for element in file_df:
        #         print(count)
        if flag == 2:
            result = re.search("^<MsgAmount>.*</MsgAmount>$", element)
            if result != None:
                #                 print(result)
                data = data + eval(result.string.split('>')[1].split('<')[0])
        #             print(data)

        if element == "<TlnVoiceSMSEvent>":
            flag = 1
        elif element == "</TlnVoiceSMSEvent>":
            flag = 0
        if flag == 1:
            if element == "<Service>{0}</Service>".format(1100) or element == "<Service>{0}</Service>".format(
                    1200) or element == "<Service>{0}</Service>".format(1300):
                flag = 2
        #                 print(count, element)

        count = count + 1

    # print(data)
    return data


# actual usage for data
def findActualUsageForData(file_df):
    flag = 0
    data = 0
    for element in file_df:
        if flag == 1:
            result = re.search("^<MsgAmount>.*</MsgAmount>$", element)
            if result != None:
                data = data + eval(result.string.split('>')[1].split('<')[0])
        #             print(data)

        if element == "<TlnDataEvent>":
            flag = 1

        elif element == "</TlnDataEvent>":
            flag = 0

    # print(data)
    return data




def main():
    try:
        files = glob.glob('C:\\Users\\ammishra\\Desktop\\TATA_files\\*.gz', recursive=True)
        counter = 0
        for file_path in files:
            file_df = removeBlank(file_path)

            # sending data to csv
            # filename, filedate, cdr count, date cdr, sms count, voice cdr, data in mb, voice in min
            filename = findFileName(file_df)
            filedate = findFileDate(file_df)
            # cdr_count = findCdrCount(file_df)
            data_cdr = findDataCdr(file_df)
            sms_count = findSmsCount(file_df)
            voice_cdr = findVoiceCdr(file_df)
            cdr_count = voice_cdr + sms_count + data_cdr
            data_in_mb = findDataInMb(file_df)
            voice_in_min = findVoiceinMin(file_df)
            transection_processed = findTransectionProcessed(file_df)
            total_event_count = findTotalEventCount(file_df)
            total_Notification_Count = findTotalNotificationCount(file_df)
            total_Balance_Tracking_Data_Count = findTotalBalanceTrackingDataCount(file_df)

            retail_rounded_data = findDataInMb(file_df)
            retail_rounded_min = findVoiceinMin(file_df)
            actual_usage_data = findActualUsageForData(file_df)
            actual_usage_min = findActualUsageForMin(file_df)

            data = [[filename, filedate, cdr_count, data_cdr, sms_count, voice_cdr, data_in_mb, voice_in_min,
                     transection_processed, total_event_count, total_Notification_Count,
                     total_Balance_Tracking_Data_Count, retail_rounded_data, retail_rounded_min, actual_usage_data,
                     actual_usage_min]]

            df = pd.DataFrame(data, columns=['FileName', 'FileDate', 'RECORD_TOTAL', 'Data_CDR', 'SMS_count', 'Voice_CDR',
                                             'DATA in Bits', 'Voice in SEC', 'Transection_processed', 'Total_event_count',
                                             'Total_Notification_Count', 'Total_Balance_Tracking_Data_Count',
                                             'RETAIL_ROUNDED_DATA_BITS', 'RETAIL_ROUNDED_SEC', 'Actual_usage_data_bits',
                                             'Actual_usage_Sec'])

            if counter == 0:
                df.to_csv('C:\\Users\\ammishra\\Desktop\\tata_CSV_{0}.csv'.format(date.today()), index=False)
                # dataTransferToRemoteServer(df, counter, filename)
            else:
                df.to_csv('C:\\Users\\ammishra\\Desktop\\tata_CSV_{0}.csv'.format(date.today()), mode='a', header=False,
                          index=False)

                # dataTransferToRemoteServer(df, counter, filename)
            counter = 1

        logger.info("completed")

    except Exception as e:
        logger.error("error {0}".format(e))


if __name__ == '__main__':
    try:
        schedule.every().day.at("15:51").do(main)

        while True:
            schedule.run_pending()

    except Exception as e:
        logger.error("error {0}".format(e))
