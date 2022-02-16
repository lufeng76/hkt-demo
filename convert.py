"""
This script will convert Fixed width File into Delimiter File, tried on Python 3.5 only
Sample run: (Order of argument doesnt matter)
python ConvertFixedToDelimiter.py -i SrcFile.txt -o TrgFile.txt -c Config.txt -d "|"
Inputs are as follows
1. Input FIle - Mandatory(Argument -i) - File which has fixed Width data in it
2. Config File - Optional (Argument -c, if not provided will look for Config.txt file on same path, if not present script will not run)
    Should have format as
    FieldName,fieldLength
    eg:
    FirstName,10
    SecondName,8
    Address,30
    etc:
3. Output File - Optional (Argument -o, if not provided will be used as InputFIleName plus Delimited.txt)
4. Delimiter - Optional (Argument -d, if not provided default value is "|" (pipe))
"""
from collections import OrderedDict
import argparse
from argparse import ArgumentParser
import os.path
import sys
from datetime import datetime
import re


def slices(s, args):
    position = 0
    for length in args:
        length = int(length)
        yield s[position:position + length]
        position += length

def extant_file(x):
    """
    'Type' for argparse - checks that file exists but does not open.
    """
    if not os.path.exists(x):
        # Argparse uses the ArgumentTypeError to give a rejection message like:
        # error: argument input: x does not exist
        raise argparse.ArgumentTypeError("{0} does not exist".format(x))
    return x


parser = ArgumentParser(description="Please provide your Inputs as -i InputFile -o OutPutFile -c ConfigFile")
parser.add_argument("-i", dest="InputFile", required=True,    help="Provide your Input file name here, if file is on different path than where this script resides then provide full path of the file", metavar="FILE", type=extant_file)
parser.add_argument("-o", dest="OutputFile", required=False,    help="Provide your Output file name here, if file is on different path than where this script resides then provide full path of the file", metavar="FILE")
parser.add_argument("-c", dest="ConfigFile", required=False,   help="Provide your Config file name here,File should have value as fieldName,fieldLength. if file is on different path than where this script resides then provide full path of the file", metavar="FILE",type=extant_file)
parser.add_argument("-d", dest="Delimiter", required=False,   help="Provide the delimiter string you want",metavar="STRING", default="|")

args = parser.parse_args()

#Input file madatory
InputFile = args.InputFile
#Delimiter by default "|"
DELIMITER = args.Delimiter

#Output file checks
if args.OutputFile is None:
    OutputFile = str(InputFile) + "Delimited.txt"
    print ("Setting Ouput file as "+ OutputFile)
else:
    OutputFile = args.OutputFile

#Config file check
if args.ConfigFile is None:
    if not os.path.exists("Config.txt"):
        print ("There is no Config File provided exiting the script")
        sys.exit()
    else:
        ConfigFile = "config.txt"
        print ("Taking Config.txt file on this path as Default Config File")
else:
    ConfigFile = args.ConfigFile

fieldNames = []
fieldLength = []
myvars = OrderedDict()


with open(ConfigFile) as myfile:
    for line in myfile:
        name, var = line.partition(",")[::2]
        myvars[name.strip()] = int(var)
for key,value in myvars.items():
    fieldNames.append(key)
    fieldLength.append(value)

with open(OutputFile, 'w') as f1:
    fieldNames = DELIMITER.join(map(str, fieldNames))
    f1.write(fieldNames[3:])
    with open(InputFile, 'r') as f:
        for line in f:
            if len(line) < 100:
                continue
            ll = re.sub('\|',',', re.sub('"',',', line))
            rec = (list(slices(ll, fieldLength)))
            # print(rec[19].strip()[:9] + ' ' + rec[19].strip()[10:] + ' ' + rec[32])
            if len(rec[19].strip()) > 8:
                dd1 = rec[19].strip()[:9]
                ss1 = rec[19].strip()[10:]
                x1 = datetime.strptime(dd1, '%d%b%Y')
                rec[19] = '"' + x1.strftime('%Y-%m-%d') + ' ' + ss1 +'"'
            if len(rec[32].strip()) > 8:
                dd1 = rec[32].strip()[:9]
                ss1 = rec[32].strip()[10:]
                x1 = datetime.strptime(dd1, '%d%b%Y')
                rec[32] = '"' + x1.strftime('%Y-%m-%d') + ' ' + ss1 +'"'            
            #print(rec[19] + ' ' + rec[32])
            myLine = DELIMITER.join(map(str, rec))
            #print(line + "\n")
            #print(myLine[4:] + "\n")
            f1.write("\n" + myLine[4:])

            