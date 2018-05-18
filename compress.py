#!/usr/bin/env python3
import argparse
from ctcompression import *   # My constrained timecrunch compression module

### Handle command args with argparse to support usage: compress.py [-h] (-c | -d) inputFile outputFile
parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("-c", "--compress", action="store_true",
                   help="Apply constrained time-crunch compression algorithm to the inputFile and write compressed data to the outputFile.")
group.add_argument("-d", "--decompress", action="store_true",
                   help="Apply constrained time-crunch decompression algorithm to the inputFile and write decompressed data to the outputFile.")
parser.add_argument("inputFile", help="The file to read as input.")
parser.add_argument("outputFile", help="The file to write as output.")
args = parser.parse_args()

if args.compress:
    ### received a '-c' or '--compress' arg
    ctc = ctCompressor()
    ctc.compress(args.inputFile, args.outputFile)
elif args.decompress:
    ### received a '-d' or '--decompress' arg
    ctd = ctDecompressor()
    ctd.decompress(args.inputFile, args.outputFile)
