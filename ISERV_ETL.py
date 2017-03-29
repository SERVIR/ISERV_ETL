#-------------------------------------------------------------------------------
# Name:        ISERV_ETL.py
# Purpose:     ISERV ETL.  Process of downloading files, parsing them and adding
#               the data to a ArcGIS service.
#
# Author:      Kris Stanton
# Last Modified By: Githika Tondapu
# Created:     2014
# Copyright:   (c) kstanto1 2014
# Licence:     <your licence>
#
# Note: Portions of this code may have been adapted from other code bases and authors
#-------------------------------------------------------------------------------
import arcpy
import datetime
import time
import ftplib
import os
import sys
import zipfile
import math
from PIL import Image            # for image processing
import boto             # for S3 connections
import ks_ConfigLoader
import ks_AdpatedLogger

# Globals
g_PathToConfigFile = r"D:\SERVIR\Scripts\ISERV\ISERV_Map_ETL_L0.xml"
g_PathToConfigFile2 = r"D:\SERVIR\Scripts\ISERV\ISERV_Map_ETL_D.xml"
g_PathToConfigFile3 = r"D:\SERVIR\Scripts\ISERV\ISERV_Map_ETL_R.xml"



# Load the Config XML File into a settings dictionary
g_ConfigSettings = ks_ConfigLoader.ks_ConfigLoader(g_PathToConfigFile)
g_ConfigSettings2 = ks_ConfigLoader.ks_ConfigLoader(g_PathToConfigFile2)
g_ConfigSettings3 = ks_ConfigLoader.ks_ConfigLoader(g_PathToConfigFile3)

# Operational Report string
g_DetailedLogging_Setting = False

#--------------------------------------------------------------------------
# Settings and Logger
#--------------------------------------------------------------------------
def get_Settings_Obj():
    Current_Config_Object = g_ConfigSettings.xmldict['ConfigObjectCollection']['ConfigObject']
    return Current_Config_Object

# Needed to prevent errors (while the 'printMsg' function is global...)
settingsObj = get_Settings_Obj()
# Logger Settings Vars
theLoggerOutputBasePath = settingsObj['Logger_Output_Location'] # Folder where logger output is stored.
theLoggerPrefixVar = settingsObj['Logger_Prefix_Variable'] # String that gets prepended to the name of the log file.
theLoggerNumOfDaysToStore = settingsObj['Logger_Num_Of_Days_To_Keep_Log'] # Number of days to keep log
# KS Mod, 2014-01   Adding a Script Logger 3        START
g_theLogger = ks_AdpatedLogger.ETLDebugLogger(theLoggerOutputBasePath, theLoggerPrefixVar+"_log", {

        "debug_log_archive_days":theLoggerNumOfDaysToStore
    })




# Add to the log
def addToLog(theMsg, detailedLoggingItem = False):

    global g_theLogger, g_DetailedLogging_Setting
    if detailedLoggingItem == True:
        if g_DetailedLogging_Setting == True:
            # This configuration means we should record detailed log items.. so do nothing
            pass
        else:
            # This config means we should NOT record detailed log items but one was passed in, so using 'return' to skip logging
            return

    currText = ""

    # Datetime is already part of the log message.
    currText += theMsg
    g_theLogger.updateDebugLog(currText)


#--------------------------------------------------------------------------
# Time functions
#--------------------------------------------------------------------------

# Calculate and return time elapsed since input time
def timeElapsed(timeS):
    seconds = time.time() - timeS
    hours = seconds // 3600
    seconds -= 3600*hours
    minutes = seconds // 60
    seconds -= 60*minutes
    if hours == 0 and minutes == 0:
        return "%02d seconds" % (seconds)
    if hours == 0:
        return "%02d:%02d seconds" % (minutes, seconds)
    return "%02d:%02d:%02d seconds" % (hours, minutes, seconds)

# Get a new time object
def get_NewStart_Time():
    timeStart = time.time()
    return timeStart

# Get the amount of time elapsed from the input time.
def get_Elapsed_Time_As_String(timeInput):
    return timeElapsed(timeInput)




#--------------------------------------------------------------------------
# Utilities
#--------------------------------------------------------------------------

# Utility, remove all rows from an attribute table.
def remove_All_Rows_From_AttributeTable(geoDBLocation, featureClassName, uniqueFieldName):
    # Path to the Featureclass, within the geodatabase or sde connection
    pathToFeatureClass = geoDBLocation + "\\" + featureClassName

    # Attempting to delete an entire row by only selecting and looking at one field.

    # Delete all rows
    theDelCounter = 0
    theFields = [uniqueFieldName]
    the_Update_Cursor = arcpy.da.UpdateCursor(pathToFeatureClass, theFields)
    for current_Row in the_Update_Cursor:
        the_Update_Cursor.deleteRow()
        theDelCounter += 1
    del the_Update_Cursor





# File Name processing.
##* DATA FILE NAMES
##The file naming convention for ISERV L-0
##data is as follows:
##
##IP0YYMMDDhhmmssLATLON.JPG where:
##
##IP - ISERV Pathfinder
##0 - Processing level
##YY - 2-numeral year
##MM - 2-numeral month
##DD - 2-numeral day
##hh - 2-numeral hour
##mm - 2-numeral minute
##ss - 2-numeral second
##LAT - latitude in decimal degrees plus hemispherical indicator (N or S)
##LON - longitude in decimal degrees plus hemispherical indicator (E or W)
##
##         0         1         2
##         012345678901234567890123456789
##Example: IP01306111234561234N12345W.jpg
##            YYMMDDhhmmss

# Correction, Years are 4 digits, here is an updated example,
# 0         1         2
# 012345678901234567890123456789
# IP0201401100751122830N08972E.zip

# Process the file name into an object according to logic provided
#def get_ISERV_Data_Obj_From_FileName(theFileName):
#def get_ISERV_Data_Obj_From_FileName(currentFTP_FileObject):
#def get_ISERV_Data_Obj_From_FileName(theFileName):
def get_FilenameParseObject_From_FileName(theFileName):


    try:
        # Raw Props
        theIP = theFileName[:2]
        theProcessingLevel = theFileName[2:3]
        theFourDigitYear = theFileName[3:7]         #theTwoDigitYear = theFileName[3:5]
        theTwoDigitMonth = theFileName[7:9]         #theTwoDigitMonth = theFileName[5:7]
        theTwoDigitDayOfMonth = theFileName[9:11]   #theTwoDigitDayOfMonth = theFileName[7:9]
        theHour = theFileName[11:13]                #theHour = theFileName[9:11]
        theMinute = theFileName[13:15]              #theMinute = theFileName[11:13]
        theSecond = theFileName[15:17]              #theSecond = theFileName[13:15]

        raw_LatNum = theFileName[17:21]             #raw_LatNum = theFileName[15:19]
        raw_LatNS = theFileName[21:22]              #raw_LatNS = theFileName[19:20]
        raw_LonNum = theFileName[22:27]             #raw_LonNum = theFileName[20:25]
        raw_LonWE = theFileName[27:28]              #raw_LonWE = theFileName[25:26]

        theFileExt = theFileName[-3:]

        the_FileName_NoExt = theFileName[:-4]    # Subtracts off the file extension

        # Additional Props
        theDateString = theFourDigitYear + "-" + theTwoDigitMonth + "-" + theTwoDigitDayOfMonth
        theTimeString = theDateString + " " + theHour + ":" + theMinute + "." + theSecond

        # Convert Lat/Long
        theLon = ""
        if raw_LonWE == 'W':
            theLon += "-"
        theLon += raw_LonNum[0:3] + "." + raw_LonNum[3:5]

        theLat = ""
        if raw_LatNS == 'S':
            theLat += "-"
        theLat += raw_LatNum[0:2] + "." + raw_LatNum[2:4]


        retObject = {
            "NoExt_FileName": the_FileName_NoExt,
            "FileName": theFileName,
            "IP": theIP,
            "ProcessingLevel": theProcessingLevel,
            "Year_YYYY": theFourDigitYear, 
            "Month_MM": theTwoDigitMonth,
            "Day_DD": theTwoDigitDayOfMonth,
            "Hours_hh": theHour,
            "Minutes_mm": theMinute,
            "Seconds_ss": theSecond,
            "Lat_Raw_Num": raw_LatNum,
            "Lat_Raw_NS": raw_LatNS,
            "Lon_Raw_Num": raw_LonNum,
            "Lon_Raw_WE": raw_LonWE,
            "FileExtension": theFileExt,
            "Lat": theLat,
            "Lon": theLon,
            #"Year_YYYY": theYear,
            "DateString": theDateString,
            "TimeString":theTimeString
        }

        # This single line generates a very large amount of data in the log file.  This is worth consideration even when detailed logging is set to true...
        addToLog("get_FilenameParseObject_From_FileName: Parsed Filename, " + str(theFileName) + ", returning object: " + str(retObject), True)

        return retObject
    except:
        addToLog("get_FilenameParseObject_From_FileName: Failed to parse filename, " + str(theFileName))
        return None



# FTP Functions
# Log into the FTP and get a list of all files and their paths
def get_All_Files_PathList(ftpOptions):

    # List to hold FULL Paths
    retList = []

    try:
        ftpHost = ftpOptions["location"]
        ftpUser = ftpOptions["user"]
        ftpPass = ftpOptions["pass"]
        ftpDir = ftpOptions["subfolder"]   
        ftp_Connection = ftplib.FTP(ftpHost,ftpUser,ftpPass)

        # Change Directory
        ftp_Connection.cwd(ftpDir)

        # Collect a listing of subfolders from the FTP Server
        subFolders_1 = []    # Expected, Years (2013, 2014 etc)
        subFolders_1_Raw = []
        ftp_Connection.dir(subFolders_1_Raw.append)

        # Parse to get the folder, '2013' and '2014' (start year and end year)
        for row1 in subFolders_1_Raw:
            currItem1 = row1.split(" ")[-1]

            # ignore ".", "..", and any files in this folder.
            if currItem1 == ".":
                pass
            elif currItem1 == "..":
                pass
            elif currItem1[-4] == ".":
                pass
            else:
                subFolders_1.append(currItem1)

        # go inside each of those and grab the list of folders that exist in there, add that to subFolders_2
        for currSubFolder_1 in subFolders_1:

            # Lists to hold the next level of subfolders
            subFolders_2 = []
            subFolders_2_Raw = []

            # Change Directory,
            nextDir1 = ftpDir + currSubFolder_1 + "/"
            ftp_Connection.cwd(nextDir1)
            ftp_Connection.dir(subFolders_2_Raw.append)

            # parse through the current level, again, append only folders
            for row2 in subFolders_2_Raw:

                currItem2 = row2.split(" ")[-1]

                # ignore ".", "..", and any files in this folder.
                if currItem2 == ".":
                    pass
                elif currItem2 == "..":
                    pass
                # Breaks when folder/file names are lower than 4 characters long..
                else:
                    subFolders_2.append(currItem2)

            # go inside the second level of subfolders and pull out the paths to each file found
            for currSubFolder_2 in subFolders_2:

                # at this point, inside each of these subfolders should be a list of zip files.. gather the list of each zip file from each subfolder,

                # Raw File List
                files_Raw = []

                # Change Directory
                nextDir2 = ftpDir + currSubFolder_1 + "/" + currSubFolder_2 + "/"
                ftp_Connection.cwd(nextDir2)
                ftp_Connection.dir(files_Raw.append)

                # Load the files, their ftppaths and download paths into an object.
                for row3 in files_Raw:
                    currItem3 = row3.split(" ")[-1]
                    if currItem3 == ".":
                        pass
                    elif currItem3 == "..":
                        pass
                    elif currItem3[-4] == ".":

                        # in the end, we need a list of EVERY zip file in these subfolders and a complete path leading to each of them..

                        # Some QUICK validation..       # Expected length is 32, and formatted like this, 'IP0201306222343485106N11415W.zip'
                        if len(currItem3) == 32:
                            # File found, Append name, ftp path and full path
                            dloadPath = "ftp://" + ftpOptions["location"] + nextDir2 + currItem3
                            currObj = {
                                "ftpPath":nextDir2,
                                "downloadURL":dloadPath,
                                "filename":currItem3
                            }
                            retList.append(currObj)
    except:
        addToLog("get_All_Files_PathList: Error with FTP")


    # Too much detail.. shorter version below
    addToLog("get_All_Files_PathList: retList (length): " + str(len(retList)), True)
    return retList


# ArcSupport
# Get list of objects from attribute table of a Geodatabase
def get_All_Objects_From_FeatureClass_Attribute_Table_In_GeoDB(pathToFeatureClass, the_List_Of_Fields):
    # List to hold the objects
    retList = []

    # Read Only connection
    the_Search_Cursor = arcpy.SearchCursor(pathToFeatureClass)

    # Iterate through and read data.  Each row gets converted into a dictionary and appended to the return objects
    for current_Row in the_Search_Cursor:
        # Create an object for each row
        # Creating an array of the field names, and the values
        # Creating a dictionary from the fieldnames and values.
        # That dictionary represents a single row and gets added to the return list
        curr_Row_Keys = []
        curr_Row_Vals = []
        for currField in the_List_Of_Fields:
            currVal = current_Row.getValue(currField)
            curr_Row_Keys.append(currField)
            curr_Row_Vals.append(currVal)
        curr_Row_Dict = dict(zip(curr_Row_Keys,curr_Row_Vals))
        retList.append(curr_Row_Dict)

    return retList

# Gets a list of FTP files with useful parts parsed into properties
def get_List_Of_All_ISERV_Objects_From_FTP(ftpOptions):
    FTP_List = get_All_Files_PathList(ftpOptions)
    return FTP_List

# Get List of All ISERV Objects from Filegeodatabase
def get_List_Of_All_ISERV_Objects_From_Filegeodatabase(geoDBOptions):
    # Path to the Featureclass, within the geodatabase or sde connection
    the_GeoDB_Location = geoDBOptions["GeoDBPath"]
    the_FeatureClassName = geoDBOptions["FeatureClassName"]
    the_List_Of_Fields = geoDBOptions["FieldList"]
    pathToFeatureClass = the_GeoDB_Location + "\\" + the_FeatureClassName

    # List to hold the objects
    retList = get_All_Objects_From_FeatureClass_Attribute_Table_In_GeoDB(pathToFeatureClass, the_List_Of_Fields)

    # Too much detail.. shorter version below
    addToLog("get_List_Of_All_ISERV_Objects_From_Filegeodatabase: retList (length): " + str(len(retList)), True)
    return retList

# Get List of Files to Remove From FileGeodatabase(All_ISERV_FTP_List, All_FileGeoDB_List) Returns 'Remove_FileGeoDB_List'
def get_List_Of_Files_To_Remove_From_FileGeodatabase(All_ISERV_FTP_List, All_FileGeoDB_List):

    # Iterate through each file in the FTP list.
    # Also Iterate through each item in the FileGeoDB list
    # Make a new list of items that are in the File Geodatabase but NOT in the FTP list.
    # Thats the list of files we want to remove from the geodatabase at a later step.. return that list.


    # List to hold the objects
    retList = []
    for curr_GeoDB_Item in All_FileGeoDB_List:
        curr_DLink_GeoDB = curr_GeoDB_Item['Download']
        hasFoundMatch = False
        for curr_FTP_Item in All_ISERV_FTP_List:
            curr_DLink_FTP = curr_FTP_Item['downloadURL']
            if curr_DLink_GeoDB == curr_DLink_FTP:
                hasFoundMatch = True
        if hasFoundMatch == True:
            # Item exists in both places, do nothing
            pass
        else:
            # Item only exists in geodatabase, add the download link to the list to be returned
            retList.append(curr_GeoDB_Item)

    addToLog("get_List_Of_Files_To_Remove_From_FileGeodatabase: retList: " + str(retList), True)
    return retList

# Get List of Files to Download From FTP(All_ISERV_FTP_List, All_FileGeoDB_List) Returns 'Download_FTP_List
def get_List_Of_Files_To_Download_From_FTP(All_ISERV_FTP_List, All_FileGeoDB_List):

    # Simillar (but not exactly the same) to the above function, Iterate through and find what exists on the server but is missing from the geodatabase, return that list.

    # List to hold the objects
    retList = []

    for curr_FTP_Item in All_ISERV_FTP_List:
        curr_DLink_FTP = curr_FTP_Item['downloadURL']
        hasFoundMatch = False
        for curr_GeoDB_Item in All_FileGeoDB_List:
            curr_DLink_GeoDB = curr_GeoDB_Item['Download']
            if curr_DLink_GeoDB == curr_DLink_FTP:
                hasFoundMatch = True
        if hasFoundMatch == True:
            # Item exists in both places, do nothing
            pass
        else:
            # Item only exists on the FTP, add the download link to the list to be returned
            retList.append(curr_FTP_Item)

    addToLog("get_List_Of_Files_To_Download_From_FTP: retList: " + str(retList), True)
    return retList

# ArcPy Support integrated function,
# Remove the items from the file geodatabase that match the objects passed in through the list.
def Do_RemoveFiles_From_GeoDB(Remove_FileGeoDB_List, geoDBOptions):

    # Bug Prevention refactor 4-2014    START       PART 1
    # So an interesting bug will show up if this function is left as is..
    #  The bug would be, if a connection is not made to the server, a very large list of items will get removed from the geodatabase.
    # This option is an attempt to restrict deleting until this issue is addressed and properly resolved.
    max_Number_To_Delete = 2    # Restricting the deleting to 2 minizes the impact on performance in the cast that the Remove list is the complete list.
    # Bug Prevention refactor 4-2014    END         PART 1

    ret_NumberOfRowsDeleted = 0

    # Path to the Featureclass, within the geodatabase or sde connection
    the_GeoDB_Location = geoDBOptions["GeoDBPath"]
    the_FeatureClassName = geoDBOptions["FeatureClassName"]
    the_List_Of_Fields = geoDBOptions["FieldList"]
    the_LinkingFieldName = geoDBOptions["LinkingFieldName"]
    pathToFeatureClass = the_GeoDB_Location + "\\" + the_FeatureClassName
    print(pathToFeatureClass+"**************************")
    # Attempting to delete an entire row by only selecting and looking at one field.
    # Delete the rows where a specific string matches the value of a single field
    theFields = [the_LinkingFieldName]
    the_Update_Cursor = arcpy.da.UpdateCursor(pathToFeatureClass, theFields)
    for current_Row in the_Update_Cursor:
        if ret_NumberOfRowsDeleted > (max_Number_To_Delete - 1):
            # Bug Prevention refactor 4-2014    PART 2
            # Do nothing, don't delete any more.
            pass
        else:
            # We are not at the max yet, go ahead and delete record if it is in the list.

            isDeleteCurrentRow = False
            # Compare current row's data with the entire list's
            # If a match is found, delete this row.
            curr_Row_Data = current_Row[0]
            for curr_GeoDB_Item_ToRemove in Remove_FileGeoDB_List:
                curr_ToRemove_Data = curr_GeoDB_Item_ToRemove[the_LinkingFieldName]
                if curr_Row_Data == curr_ToRemove_Data:
                    # Match found, remove this row.
                    # Deleting at this point in the code breaks the memory allocation of the current row data.
                    isDeleteCurrentRow = True
            if isDeleteCurrentRow == True:
                addToLog("Do_RemoveFiles_From_GeoDB: Removing row containing: " + str(curr_ToRemove_Data) + " from the GeoDabase.")
                the_Update_Cursor.deleteRow()
                ret_NumberOfRowsDeleted = ret_NumberOfRowsDeleted + 1

    del the_Update_Cursor

    return ret_NumberOfRowsDeleted

# Extract_Do_DownloadFiles_From_FTP(Download_FTP_List) Returns 'Files_Extracted_List'
def Extract_Do_DownloadFiles_From_FTP(Download_FTP_List, scriptOptions, ftpOptions):

    retList = []

    counter_FilesDownloaded = 0
    counter_FilesExtracted = 0

    # For debugging.
    debugFileDownloadLimiter = 10000 #10000


    path_To_Scratch_Folder = scriptOptions["ScratchFolder"] + "\\Extract"
    ftpHost = ftpOptions["location"]
    ftpUser = ftpOptions["user"]
    ftpPass = ftpOptions["pass"]

    try:
        # Open the FTP connection
        ftp_Connection = ftplib.FTP(ftpHost,ftpUser,ftpPass)

        # Itereate through the list
        for current_ftp_Obj_ToGet in Download_FTP_List:

            if counter_FilesDownloaded < debugFileDownloadLimiter:
                # Create a location for the file if it does not exist..
                if not os.path.exists(path_To_Scratch_Folder):
                    os.makedirs(path_To_Scratch_Folder)

                # Start reading the FTP List... Get the Directory and Filename
                current_DownloadURL = current_ftp_Obj_ToGet['downloadURL']
                current_FTP_Path = current_ftp_Obj_ToGet["ftpPath"]
                current_FileName = current_ftp_Obj_ToGet["filename"]
                current_FileDownload_Location = os.path.join(path_To_Scratch_Folder,current_FileName) # downloadedFile

                # Change to the directory
                ftp_Connection.cwd(current_FTP_Path)

                # Download the file from the FTP directory
                ftp_Connection.retrbinary("RETR " + current_FileName ,open(current_FileDownload_Location, 'wb').write)

                addToLog("Extract_Do_DownloadFiles_From_FTP: Downloaded file to: " + str(current_FileDownload_Location), True)
                counter_FilesDownloaded += 1

                # Try and Extract the file, catalog it, and add to the return list.
                try:
                    z = zipfile.ZipFile(current_FileDownload_Location)
                    zipfile.ZipFile.extractall(z,path_To_Scratch_Folder)
                    zipFileList = [os.path.join(path_To_Scratch_Folder,f) for f in z.namelist()]

                    addToLog("Extract_Do_DownloadFiles_From_FTP: Unzipped file, " + os.path.basename(current_FileDownload_Location), True)
                    addToLog("Zip Files: ", True)
                    for f in z.namelist():
                        addToLog("  "+f, True)
                        counter_FilesExtracted += 1

                    current_Ret_Obj = {
                        "FileName" : current_FileName,
                        "PathOnFileSystem" : current_FileDownload_Location,
                        "ExtractedFilesList" : zipFileList,
                        "downloadURL" : current_DownloadURL
                    }

                    retList.append(current_Ret_Obj)

                except:
                    e = sys.exc_info()[0]
                    addToLog("Extract_Do_DownloadFiles_From_FTP: Error extracting the zip files, Error Message: " + str(e))
            else:
                addToLog("Extract_Do_DownloadFiles_From_FTP: debugFileDownloadLimiter limit reached, " + str(debugFileDownloadLimiter) + " files have already been downloaded and extracted.", True)


    except:
        e = sys.exc_info()[0]
        addToLog("Extract_Do_DownloadFiles_From_FTP: Error with FTP, Error Message: " + str(e))

    addToLog("Extract_Do_DownloadFiles_From_FTP: " + str(counter_FilesDownloaded) + " Files downloaded and " + str(counter_FilesExtracted) + " Files have been extracted.")

    addToLog("Extract_Do_DownloadFiles_From_FTP: retList: " + str(retList), True)
    return retList

# Make a thumbnail
def Make_Thumb_ForFile(theMaxWidth, theMaxHeight, inputFileLocation, outputFileLocation):
    try:
        im = Image.open(inputFileLocation)
        size = (theMaxWidth, theMaxHeight)                # Max Dimensions (Width,Height)
        im.thumbnail(size, Image.ANTIALIAS)
        im.save(outputFileLocation, "JPEG")
    except:
        # Error making and/or saving the thumb image
        e = sys.exc_info()[0]
        addToLog("Make_Thumb_ForFile: Error Making Thumb image for file, " + str(inputFileLocation) + ", Error Message: " + str(e))


# S3 Tests
# Make S3 connection object.
def s3_Get_Connection(s3_AccessKey, s3_SecretKey, s3_Is_Use_Local_IAMRole = True):

    # Refactor for IAM Role
    s3_Connection = None


    if s3_Is_Use_Local_IAMRole == True:
        try:
            s3_Connection = boto.connect_s3()
        except:
            try:
                s3_Connection = boto.connect_s3(s3_AccessKey, s3_SecretKey)
            except:
                addToLog("s3_Get_Connection: ERROR Making S3 Connection using Credentials", True)
    else:
        try:
            s3_Connection = boto.connect_s3(s3_AccessKey, s3_SecretKey)
        except:
            addToLog("s3_Get_Connection: ERROR Making S3 Connection using Credentials", True)
    return s3_Connection



# Save a file to S3
def Push_To_S3(s3Options, file_To_Upload_Full_Location, file_To_Upload_Filename_Only):

    try:

        setting_s3_Is_Use_Local_IAM_Role = s3Options['s3_UseLocal_IAM_Role']
        setting_s3_BucketName = s3Options['s3_BucketName']
        setting_s3_BucketRootPath = s3Options['s3_BucketRootPath']
        setting_s3_UserName = s3Options['s3_UserName']
        setting_s3_AccessKeyID = s3Options['s3_AccessKeyID']
        setting_s3_SecretAccessKey = s3Options['s3_SecretAccessKey']
        setting_s3_PathTo_Output_Thumb_Files = s3Options['s3_PathTo_Output_Thumb_Files']

    	# Get Connection object from connection info
        s3_Connection = s3_Get_Connection(setting_s3_AccessKeyID, setting_s3_SecretAccessKey, setting_s3_Is_Use_Local_IAM_Role)

    	# since we have a root folder called, '/', this option must be set to false or we won't be able to use our keys.
        # https://groups.google.com/forum/#!topic/boto-dev/-ft0XPUy0y8
        s3_Connection.suppress_consec_slashes = False

    	# Connect to bucket
        s3_Bucket = s3_Connection.get_bucket(setting_s3_BucketName,True,None)

    	# Upload file (Rewritten)
        s3_upload_Path = r""
        fullPathToFile = file_To_Upload_Full_Location
        key_FileName = file_To_Upload_Filename_Only
        s3_upload_Path += setting_s3_PathTo_Output_Thumb_Files
        full_key_name = s3_upload_Path + key_FileName #full_key_name = os.path.join(s3_upload_Path, key_FileName)
        k = s3_Bucket.new_key(full_key_name)
        k.set_contents_from_filename(fullPathToFile)
        k.set_acl('public-read') # Make the file public
        #https://bucket.servirglobal.net.s3.amazonaws.com//iserv/ThumbTest_DeleteMe.jpg
        theURL = "https://bucket.servirglobal.net.s3.amazonaws.com/" + str(full_key_name)
        retObj = {
            "Key": str(full_key_name),
            "PublicURL" : theURL
        }

        return retObj
    except:
        e = sys.exc_info()[0]
        addToLog("Push_To_S3: Error pushing file to S3 for file, " + str(file_To_Upload_Filename_Only) + ", Error Message: " + str(e))
        retObj = {
            "Key": "ERROR",
            "PublicURL" : "ERROR"
        }
        return retObj

# Transform Downloaded Files.  In this case, find the matching jpg, convert it to a thumb.  Store it on the S3
def Transform_Downloaded_Files(Files_Extracted_List, scriptOptions, s3Options):

    retList = []


    path_To_Scratch_Folder = scriptOptions["ScratchFolder"] + "\\Transform"

    # Create a location for the file if it does not exist..
    if not os.path.exists(path_To_Scratch_Folder):
        os.makedirs(path_To_Scratch_Folder)

    # Iterate through each zip file that has been extracted,
    # Look for the 'expected' image file (jpg)
    # Example, inside,  "IP0201303271418280962S05837W"  we would have "IP0201303271418280962S05837W.jpg"

    for extracted_ZipFile_Info in Files_Extracted_List:
        try:
            current_DownloadURL = extracted_ZipFile_Info["downloadURL"]
            current_Zip_File = extracted_ZipFile_Info["FileName"]
            current_ExtractedFiles_List = extracted_ZipFile_Info["ExtractedFilesList"]

            current_ComponentFileName = current_Zip_File[:-4]  # Should be all the file name except the .zip part.
            current_Expected_jpg_Filename = str(current_ComponentFileName) + ".jpg"

            isFound = False
            current_JPG_File = ""

            # Check the list of all files for the matching jpg.
            for extractedFile in current_ExtractedFiles_List:
                tempCompare = extractedFile.split("\\")[-1]
                tempCompare = tempCompare.split("/")[-1]
                if tempCompare.lower() == current_Expected_jpg_Filename.lower():
                    isFound = True
                    current_JPG_File = extractedFile

            # If we found a jpg file, process next step (make a thumb and push it to Amazon S3 and retrieve the name/key)
            if isFound == True:
                maxWidth = scriptOptions["Thumb_Width"]
                maxHeight = scriptOptions["Thumb_Height"]

                # Convert strs to ints
                int_MaxWidth = 133 # Default if fail
                int_MaxHeight = 200 # Default if Fail
                try:
                    int_MaxWidth = int(maxWidth)
                    int_MaxHeight = int(maxHeight)
                except:
                    # Defaults
                    int_MaxWidth = 133
                    int_MaxHeight = 200

                thumbOut_Filename = "thumb_"+str(current_Expected_jpg_Filename)
                thumbOut_FileLocation = os.path.join(path_To_Scratch_Folder,thumbOut_Filename)
                Make_Thumb_ForFile(int_MaxWidth, int_MaxHeight, current_JPG_File, thumbOut_FileLocation)

                # Now push to S3,
                s3ResultObj = Push_To_S3(s3Options, thumbOut_FileLocation, thumbOut_Filename)
                the_S3_Key = s3ResultObj["Key"]
                thumb_Public_URL = s3ResultObj["PublicURL"]

                # Param, "downloadURL" refers to the original zip file download url.. not the processed thumb (which is Public_URL_To_Thumb)
                transItem = {
                    "ZipFileName": current_Zip_File,
                    "Thumb_Filename" : thumbOut_Filename,
                    "Thumb_FileLocation" : thumbOut_FileLocation,
                    "S3_Key" : the_S3_Key,
                    "Public_URL_To_Thumb" : thumb_Public_URL,
                    "downloadURL" : current_DownloadURL
                }

                retList.append(transItem)

        except:
            e = sys.exc_info()[0]
            addToLog("Transform_Downloaded_Files: Error getting the jpg file, Error Message: " + str(e))


    addToLog("Transform_Downloaded_Files: retList: " + str(retList), True)
    return retList


# Load Support function, adds a single row of data to the Geodatabase.
def add_Single_Attribute_Data_To_GeoDB(geoDB_Fields, geoDB_Data, pathTo_GeoDB, feature_ClassName):
    try:
        theFields = geoDB_Fields
        theData = geoDB_Data
        pathToFeatureClass = pathTo_GeoDB + "\\" + feature_ClassName
        the_Insert_Cursor = arcpy.da.InsertCursor(pathToFeatureClass, theFields)
        the_Insert_Cursor.insertRow(theData)
        del the_Insert_Cursor
        addToLog("add_Single_Attribute_Data_To_GeoDB: Data Added to GeoDB. geoDB_Data: " + str(geoDB_Data), True)

    except:
        e = sys.exc_info()[0]
        addToLog("add_Single_Attribute_Data_To_GeoDB: Error adding item to the Geodatabase, Error Message: " + str(e) + ", geoDB_Data: " + str(geoDB_Data))

# aLat = 51.03
# aLon = -114.27
# retObj = Get_Converted_ProjCoords_From_LatLong(aLat, aLon)
# debug_GetConverted_ProjCoords_From_LatLong(aLat, aLon)
# Convert Lat/Long to the coords used by the projection (so they appear in the proper place on the map)
# (Deprecating)
def Get_Converted_ProjCoords_From_LatLong(mercatorY_lat, mercatorX_lon):
    num = mercatorX_lon * 0.017453292519943295
    x = 6378137.0 * num
    a = mercatorY_lat * 0.017453292519943295

    retX = x
    retY = 3189068.5 * math.log((1.0 + math.sin(a)) / (1.0 - math.sin(a)))
    retObj = {
        "xLon":retX,
        "yLat":retY
    }
    return retObj


def Load_PostLoadPatch_Operation(geoDBOptions, scriptOptions):

    addToLog("Load_PostLoadPatch_Operation STARTED")

    geoDB_Location = geoDBOptions['GeoDBPath']
    featureClass_Name = geoDBOptions['FeatureClassName'] # "ISERV_MapData"
    pathTo_FeatureClass = geoDB_Location + "\\" + featureClass_Name

    path_To_Scratch_Folder = scriptOptions["ScratchFolder"] + "\\Load"
    temp_WorkSpace = path_To_Scratch_Folder

    pathTo_TempTableView = temp_WorkSpace + "\\" + "tempTable.dbf" # HC_path_ToTemp_TableView
    temp_TableView_Name = "temp_TableView"
    temp_XY_OutLayerName = "temp_XY_ISERV_Layer"
    temp_XY_OutLayerName_Location = temp_WorkSpace + "\\" + temp_XY_OutLayerName
    temp_saved_Layer_Location = temp_WorkSpace + "\\" + "iserv_Locations_XY.lyr"
    temp_FeatureClass_Name = "tempFeatureClass.shp"
    fullPath_To_TempFeatureClass = temp_WorkSpace + "\\" + temp_FeatureClass_Name

    # Items to move into settings
    uniqueFieldName = "Download"  # probably better to use OBJECTID field...
    the_LatField = "Lat"
    the_LonField = "Lon"
	
    # Create a location for the scratch folder if it does not exist..
    if not os.path.exists(temp_WorkSpace):
        os.makedirs(temp_WorkSpace)

    # pre-Clean up (Remove items if they already exist)
    if arcpy.Exists(temp_TableView_Name):
        arcpy.Delete_management(temp_TableView_Name)
    if arcpy.Exists(pathTo_TempTableView):
        arcpy.Delete_management(pathTo_TempTableView)
    if arcpy.Exists(temp_saved_Layer_Location):
        arcpy.Delete_management(temp_saved_Layer_Location)
    if arcpy.Exists(temp_XY_OutLayerName_Location):
        arcpy.Delete_management(temp_XY_OutLayerName_Location)
    if arcpy.Exists(fullPath_To_TempFeatureClass):
        arcpy.Delete_management(fullPath_To_TempFeatureClass)

    # Make a table view
    arcpy.MakeTableView_management(pathTo_FeatureClass, temp_TableView_Name)
    arcpy.CopyRows_management(temp_TableView_Name, pathTo_TempTableView)              # To persist the layer on disk make a copy of the view

    # Make an XY Event Layer
    arcpy.MakeXYEventLayer_management(temp_TableView_Name, the_LonField, the_LatField, temp_XY_OutLayerName)  #, the_spatial_projection)  # So the spatial reference is actually optional here..
    arcpy.SaveToLayerFile_management(temp_XY_OutLayerName, temp_saved_Layer_Location)       # Save to a layer file

    # Create a temp feature class
    copy_features_result = arcpy.CopyFeatures_management(temp_saved_Layer_Location, fullPath_To_TempFeatureClass)

    # Remove all data from the existing feature class
    remove_All_Rows_From_AttributeTable(geoDB_Location, featureClass_Name, uniqueFieldName)

    # Append the data into the Geodb feature class
    arcpy.Append_management(fullPath_To_TempFeatureClass, pathTo_FeatureClass)

    # Post Clean up (Move this to the clean up function)
    try:
        if os.path.exists(temp_WorkSpace):
            os.remove(temp_WorkSpace)
    except:
        addToLog("Load_PostLoadPatch_Operation: Could not remove temp scratch folder : " + str(temp_WorkSpace) + " Please remove this manually.")

    addToLog("Load_PostLoadPatch_Operation HAS REACHED THE END")

# Do the loading of data to arcGeoDB for ISERV ETL.
def Load_Data_To_GeoDB(Data_To_Load_List, geoDBOptions, scriptOptions):
    numOfItemsLoaded = 0


    try:
        the_Attribute_Table_Fields = geoDBOptions['FieldList']      # [ 'Download', 'Name', 'Date', 'Time', 'Lat', 'Lon', 'Preview'],       (And Disaster types have an extra one, called, "Disaster_E")
        the_PathTo_GeoDB = geoDBOptions['GeoDBPath']
        the_Feature_ClassName = geoDBOptions['FeatureClassName']

        theCurrentDateTime = datetime.datetime.now() # datetime.date.today() #   # DateAdded

        # Iterate through each item in the datalist,
        for Data_Item in Data_To_Load_List:

            # parse out info from filename, Process the info, store it in a data list
            the_ZipFilename = Data_Item['ZipFileName']
            the_FTP_DownloadURL = Data_Item['downloadURL']
            the_ThumbURL = Data_Item['Public_URL_To_Thumb']
            the_ParsedObj = get_FilenameParseObject_From_FileName(the_ZipFilename)

            # Datetime stuff
            tempDateTimeStr = str(the_ParsedObj['Month_MM']) + " " + str(the_ParsedObj['Day_DD']) + " " + str(the_ParsedObj['Year_YYYY'])
            tempDateTimeFormatStr = "%m %d %Y"
            the_Date = datetime.datetime.strptime(tempDateTimeStr, tempDateTimeFormatStr) # "MM DD YYYY"
            the_Time = the_ParsedObj['Hours_hh'] + ":" + the_ParsedObj['Minutes_mm'] + ":" + the_ParsedObj['Seconds_ss']

            float_Lat = None
            float_Lon = None
            try:
                float_Lat = float(the_ParsedObj['Lat'])
                float_Lon = float(the_ParsedObj['Lon'])
            except:
                e = sys.exc_info()[0]
                addToLog("Load_Data_To_GeoDB: Error parsing Lat/Lon to Floats, CurrentDataObject : " + str(Data_Item) + ", Error Message: " + str(e))

            # Quick fix for adding extra items.. Not good practice!!
            the_GeoDB_Data = None
            isDisaster = False
            for item in the_Attribute_Table_Fields:
                if item == "Disaster_E":
                    isDisaster = True

            if isDisaster == True:
                # This is a disaster type,
                currDisasterName = "Unknown"
                # Expecting something simillar to 'ftp://ghrc.nsstc.nasa.gov/pub/iserv/data/Disasters/2013/Calgary_flood/IP0201306222343485106N11415W.zip'
                currDisasterName = the_FTP_DownloadURL.split('/')[-2]

                the_GeoDB_Data = [None, None, None, None, None, None, None, None, None]     # Order must match the FieldList order or this does not work.
                #the_GeoDB_Data = [None, None, None, None, None, None, None, None, None, None]
                the_GeoDB_Data[0] = the_FTP_DownloadURL                         # 'Download',
                the_GeoDB_Data[1] = the_ParsedObj['NoExt_FileName']             # 'Name',
                the_GeoDB_Data[2] = the_Date                                    # 'Date',
                the_GeoDB_Data[3] = the_Time                                    # 'Time',
                the_GeoDB_Data[4] = float_Lat                                   # 'Lat',
                the_GeoDB_Data[5] = float_Lon                                   # 'Lon',
                the_GeoDB_Data[6] = the_ThumbURL                                # 'Preview'
                the_GeoDB_Data[7] = theCurrentDateTime                          # 'DateAdded'
                the_GeoDB_Data[8] = currDisasterName                            # 'Disaster_E'
                # Deprecating Use of Projection

            else:
                # Not a disaster type
                the_GeoDB_Data = [None, None, None, None, None, None, None, None]     # Order must match the FieldList order or this does not work.
                the_GeoDB_Data[0] = the_FTP_DownloadURL                         # 'Download',
                the_GeoDB_Data[1] = the_ParsedObj['NoExt_FileName']             # 'Name',
                the_GeoDB_Data[2] = the_Date                                    # 'Date',
                the_GeoDB_Data[3] = the_Time                                    # 'Time',
                the_GeoDB_Data[4] = float_Lat                                   # 'Lat',
                the_GeoDB_Data[5] = float_Lon                                   # 'Lon',
                the_GeoDB_Data[6] = the_ThumbURL                                # 'Preview'
                the_GeoDB_Data[7] = theCurrentDateTime                          # 'DateAdded'

            # Call the function to add the data to the GeoDB
            add_Single_Attribute_Data_To_GeoDB(the_Attribute_Table_Fields, the_GeoDB_Data, the_PathTo_GeoDB, the_Feature_ClassName)

            numOfItemsLoaded += 1

    except:
        e = sys.exc_info()[0]
        addToLog("Load_Data_To_GeoDB: Error while Loading Data to the GeoDB, Error Message: " + str(e))

    try:
        if numOfItemsLoaded > 0:
            Load_PostLoadPatch_Operation(geoDBOptions, scriptOptions)
            addToLog("Load_Data_To_GeoDB: Load_PostLoadPatch_Operation completed.")
    except:
        e = sys.exc_info()[0]
        addToLog("Load_Data_To_GeoDB: Error while Running 'Load_PostLoadPatch_Operation', Error Message: " + str(e))

    return numOfItemsLoaded



# Clean up
def postETL_Clean_Up_TempFiles(extractedList, transformList, scriptOptions):


    filesNotDeleted_List = []       # If any errors on cleanup, the files are stored here for the log.
    filesRemovedCounter = 0         # Count each time a file is removed.

    path_To_Scratch_Folder = scriptOptions["ScratchFolder"]

    # Remove downloaded and extracted files
    for extract_Zip_File in extractedList:

        # First remove the zip file
        current_ZipFile = extract_Zip_File["PathOnFileSystem"]
        try:
            os.remove(current_ZipFile)
            filesRemovedCounter += 1
        except:
            filesNotDeleted_List.append(current_ZipFile)

        # Now remove all the extracted files
        current_ExtractedFiles_List = extract_Zip_File["ExtractedFilesList"]
        for extFile in current_ExtractedFiles_List:
            try:
                os.remove(extFile)
                filesRemovedCounter += 1
            except:
                filesNotDeleted_List.append(extFile)

    # Remove transformed files
    for transform_ThumbFile in transformList:
        # Remove the thumbnail
        current_ThumbFile = transform_ThumbFile["Thumb_FileLocation"]
        try:
            os.remove(current_ThumbFile)
            filesRemovedCounter += 1
        except:
            filesNotDeleted_List.append(current_ThumbFile)

    # Output to log
    addToLog("postETL_Clean_Up_TempFiles: " + str(filesRemovedCounter) + " temp files have been removed.")
    if len(filesNotDeleted_List) > 0:
        addToLog("postETL_Clean_Up_TempFiles: Clean up Warnings.  " + str(len(filesNotDeleted_List)) + " files were not deleted.  Please manually remove these from the Scratch Folder: " + path_To_Scratch_Folder)
        addToLog("postETL_Clean_Up_TempFiles: List of files not deleted: " + str(filesNotDeleted_List), True)



    return filesRemovedCounter


# Do_Check_For_Updates
# Gets data from FTP and Geodatabase connection,
# Compares the lists to see if new items should be downloaded from FTP or old items should be removed from the geodatabase
# Controlls function calls for both of the above operations.
def Do_Check_For_Updates(scriptOptions, ftpOptions, geoDBOptions, s3Options):

    # Get a list of all ISERV objects from FTP
    time_PreETL_GetListOfAllFTPObjects_Process = get_NewStart_Time()
    addToLog("========= PRE ETL: Get List of all objects Found in the FTP Server =========")
    All_ISERV_FTP_List = get_List_Of_All_ISERV_Objects_From_FTP(ftpOptions)
    addToLog("TIME PERFORMANCE: time_PreETL_GetListOfAllFTPObjects_Process : " + get_Elapsed_Time_As_String(time_PreETL_GetListOfAllFTPObjects_Process))

    # Get List of All ISERV Objects from Filegeodatabase
    time_PreETL_GetListOfAllGeoDatabaseObjects_Process = get_NewStart_Time()
    addToLog("========= PRE ETL: Get List of all objects From GeoDatabase =========")
    All_FileGeoDB_List = get_List_Of_All_ISERV_Objects_From_Filegeodatabase(geoDBOptions)
    addToLog("TIME PERFORMANCE: time_PreETL_GetListOfAllGeoDatabaseObjects_Process : " + get_Elapsed_Time_As_String(time_PreETL_GetListOfAllGeoDatabaseObjects_Process))

    # Get List of Files to Remove From FileGeodatabase(All_ISERV_FTP_List, All_FileGeoDB_List) Returns 'Remove_GeoDB_List'
    time_PreETL_GetGeoDBRemoveList_Process = get_NewStart_Time()
    addToLog("========= PRE ETL: Get List of Files to Remove From GeoDatabase =========")
    Remove_GeoDB_List = get_List_Of_Files_To_Remove_From_FileGeodatabase(All_ISERV_FTP_List, All_FileGeoDB_List)
    addToLog("Do_Check_For_Updates: Found " + str(len(Remove_GeoDB_List)) + " items to remove from the Geodatabase")
    addToLog("TIME PERFORMANCE: time_PreETL_GetGeoDBRemoveList_Process : " + get_Elapsed_Time_As_String(time_PreETL_GetGeoDBRemoveList_Process))

    # Get List of Files to Download From FTP(All_ISERV_FTP_List, All_FileGeoDB_List) Returns 'Download_FTP_List
    time_PreETL_GetDownloadList_Process = get_NewStart_Time()
    addToLog("========= PRE ETL: Get List of Files to Download From FTP =========")
    Download_FTP_List = get_List_Of_Files_To_Download_From_FTP(All_ISERV_FTP_List, All_FileGeoDB_List)
    addToLog("Do_Check_For_Updates: Found " + str(len(Download_FTP_List)) + " items to download from the FTP")
    addToLog("TIME PERFORMANCE: time_PreETL_GetDownloadList_Process : " + get_Elapsed_Time_As_String(time_PreETL_GetDownloadList_Process))

    # Do_RemoveFiles_From_GeoDB(Remove_GeoDB_List) Returns 'NumberOfFilesRemoved'
    time_PreETL_CleanGeoDB_Process = get_NewStart_Time()
    addToLog("========= PRE ETL: Clean Geodatabase =========")
    NumberOfFilesRemoved = Do_RemoveFiles_From_GeoDB(Remove_GeoDB_List, geoDBOptions) # 0
    addToLog("Do_Check_For_Updates: Removed, " + str(NumberOfFilesRemoved) + " items from the GeoDatabase")
    addToLog("TIME PERFORMANCE: time_PreETL_CleanGeoDB_Process : " + get_Elapsed_Time_As_String(time_PreETL_CleanGeoDB_Process))

    # Extract_Do_DownloadFiles_From_FTP(Download_FTP_List) Returns 'Files_Extracted_List'
    time_Extract_Process = get_NewStart_Time()
    addToLog("========= EXTRACTING =========")
    Files_Extracted_List = Extract_Do_DownloadFiles_From_FTP(Download_FTP_List, scriptOptions, ftpOptions)
    addToLog("Do_Check_For_Updates: Downloaded and Extracted, " + str(len(Files_Extracted_List)) + " items from the FTP Server")
    addToLog("TIME PERFORMANCE: time_Extract_Process : " + get_Elapsed_Time_As_String(time_Extract_Process))

    # Transform_Downloaded_Files()  returns list of 'data objects' for loading
    time_Transform_Process = get_NewStart_Time()
    addToLog("========= TRANSFORMING =========")
    Transform_Data_To_Load_List = Transform_Downloaded_Files(Files_Extracted_List, scriptOptions, s3Options)
    addToLog("Do_Check_For_Updates: Transformed, " + str(len(Transform_Data_To_Load_List)) + " items from extracted zip files")
    addToLog("TIME PERFORMANCE: time_Transform_Process : " + get_Elapsed_Time_As_String(time_Transform_Process))


    # Load_Data_To_GeoDB(Transform_Data_To_Load_List) returns count of items loaded into Geodatabase.
    time_Load_Process = get_NewStart_Time()
    addToLog("========= LOADING =========")
    Number_Of_Items_Loaded = Load_Data_To_GeoDB(Transform_Data_To_Load_List, geoDBOptions, scriptOptions)
    addToLog("TIME PERFORMANCE: time_Load_Process : " + get_Elapsed_Time_As_String(time_Load_Process))


    # Cleanup
    time_PostETL_CleanUp_Process = get_NewStart_Time()
    addToLog("========= POST ETL: Cleaning Temp files =========")
    Number_Of_Temp_Files_Removed = postETL_Clean_Up_TempFiles(Files_Extracted_List, Transform_Data_To_Load_List, scriptOptions)
    addToLog("TIME PERFORMANCE: time_PostETL_CleanUp_Process : " + get_Elapsed_Time_As_String(time_PostETL_CleanUp_Process))


    # Return anything?  Report?  Success or Fail?
    retObj = {
        "New_Items_Loaded":Number_Of_Items_Loaded,
        "Temp_Files_Removed":Number_Of_Temp_Files_Removed
    }
    return retObj

# Main Controller function for this script.
def main(config_Settings):

    time_Total_Process = get_NewStart_Time()

    addToLog("======================= SESSION START =======================")


    addToLog("main: Loading Settings")

    settingsObj = config_Settings.xmldict['ConfigObjectCollection']['ConfigObject']

    setting_Name = settingsObj['Name']
    setting_ScratchFolder = settingsObj['ScratchFolder']
    setting_DetailedLogging = settingsObj['DetailedLogging']
    setting_Logger_Output_Location = settingsObj['Logger_Output_Location']

    setting_GeoDBPath = settingsObj["Path_To_GeoDatabase_Or_SDE"]

    setting_FTP_Host = settingsObj['FTP_Host']
    setting_FTP_User = settingsObj['FTP_User']
    setting_FTP_Pass = settingsObj['FTP_Pass']

    setting_Thumb_Output_Folder = settingsObj['Thumb_Output_Folder']
    setting_Thumb_Width = settingsObj['Thumb_Width']
    setting_Thumb_Height = settingsObj['Thumb_Height']

    setting_s3_IsUseLocalRole = settingsObj['s3_UseLocal_IAM_Role']
    setting_s3_BucketName = settingsObj['s3_BucketName']
    setting_s3_BucketRootPath = settingsObj['s3_BucketRootPath']
    setting_s3_UserName = settingsObj['s3_UserName']
    setting_s3_AccessKeyID = settingsObj['s3_AccessKeyID']
    setting_s3_SecretAccessKey = settingsObj['s3_SecretAccessKey']


    setting_FeatureClass_Name = settingsObj["FeatureClassName"]
    setting_FeatureClass_FieldList = settingsObj['FeatureClassFields']['Field']
    setting_FeatureClass_LinkingFieldName = settingsObj["LinkingFieldName"]
    setting_FTP_SubFolderPath = settingsObj['FTP_SubFolderPath']
    setting_s3_PathTo_Output_Thumb_Files = settingsObj['s3_PathTo_Output_Thumb_Files']

    # Deprecating
    #setting_Thumb_WebLocation_Image_Root_Folder = settingsObj['Thumb_WebLocation_Image_Root_Folder']


    addToLog("main: Settings Loaded")

    addToLog("Main: " + setting_Name + ", has started")


    # Set up Detailed Logging
    global g_DetailedLogging_Setting
    if setting_DetailedLogging == '1':
        g_DetailedLogging_Setting = True
    else:
        g_DetailedLogging_Setting = False
    addToLog("Main: Detailed logging has been enabled", True)

    # Create the Script Options Object
    scriptOptions = {
        "Name" : setting_Name,
        "ScratchFolder" : setting_ScratchFolder,
        "DetailedLogging" : setting_DetailedLogging,
        "Logger_Output_Location" : setting_Logger_Output_Location,
        "Thumb_Output_Folder" : setting_Thumb_Output_Folder,
        "Thumb_Width" : setting_Thumb_Width,
        "Thumb_Height" : setting_Thumb_Height

    }
    addToLog("Main: scriptOptions created: " + str(scriptOptions))

    # Create the FTP Options Object
    ftpOptions = {
        "location":setting_FTP_Host,
        "user":setting_FTP_User,
        "pass":setting_FTP_Pass,
        "subfolder":setting_FTP_SubFolderPath
    }
    addToLog("Main: ftpOptions created: " + str(ftpOptions))

    # Create the GeoDB Options Object
    geoDBOptions = {
        "GeoDBPath":setting_GeoDBPath,
        "FeatureClassName":setting_FeatureClass_Name,
        "FieldList" : setting_FeatureClass_FieldList,
        "LinkingFieldName" : setting_FeatureClass_LinkingFieldName
    }
    addToLog("Main: geoDBOptions created: " + str(geoDBOptions))


    bool_s3_IsUseLocalRole = True
    if setting_s3_IsUseLocalRole == '1':
        bool_s3_IsUseLocalRole = True
    else:
        bool_s3_IsUseLocalRole = False

    s3Options = {
        "s3_UseLocal_IAM_Role" : bool_s3_IsUseLocalRole,
        "s3_BucketName" : setting_s3_BucketName,
        "s3_BucketRootPath" : setting_s3_BucketRootPath,
        "s3_UserName" : setting_s3_UserName,
        "s3_AccessKeyID" : setting_s3_AccessKeyID,
        "s3_SecretAccessKey" : setting_s3_SecretAccessKey,
        "s3_PathTo_Output_Thumb_Files" : setting_s3_PathTo_Output_Thumb_Files
    }
    addToLog("Main: s3Options created: " + str(s3Options))


    # Controlls the execution for this whole process.
    UpdatesResultObject = Do_Check_For_Updates(scriptOptions, ftpOptions, geoDBOptions, s3Options)
    addToLog("Main: UpdatesResultObject: " + str(UpdatesResultObject))

    addToLog("TIME PERFORMANCE: time_Total_Process : " + get_Elapsed_Time_As_String(time_Total_Process))

    addToLog("======================= SESSION END =======================")


    pass



# Run the main script

# Type: L0 Execution
main(g_ConfigSettings)

# Type: Disasters Execution
main(g_ConfigSettings2)

# Type: Raw Execution
main(g_ConfigSettings3)





## END
## END
## END