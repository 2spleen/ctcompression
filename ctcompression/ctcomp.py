#!/usr/bin/env python3
import fileinput
import pickle
import operator

class ctCompressor:
    def __init__(self):
        self.firstRecvTime = ''
        self.firstSendTime = ''
        self.patternStats = {}   # format: { pattern: freq, ... }
        self.ctcDict = {}   # format: { pattern: codeword, ... }

        
    def compress(self, inputFile, outputFile):   # primary method that gets called
        self.preScanInput(inputFile)
        self.buildCTCDictionary()
        self.writeOutFile(inputFile, outputFile)

        
    def preScanInput(self,inputFile):
        # first loop through input file, storing the first timestamps in series, translating subsequent timestamps to deltas
        with fileinput.input(files=(inputFile)) as inFile:
            prevRecvTime = None
            prevSendTime = None
            recvTimeDelta = None
            sendTimeDelta = None
            
            for line in inFile:
                line = line.strip()   #valid to remove leading and trailing whitespace from this data (especially newlines)
                vals = line.split(',')

                ticker = vals[0]
                exchange = vals[1]
                side = vals[2]
                condition = vals[3]
                sendTime = int(vals[4])
                recvTime = int(vals[5])
                price = vals[6]
                size = vals[7]

                if not inFile.isfirstline():
                    recvTimeDelta = recvTime - prevRecvTime   # this is relative to previous tick since we assume strongest ordering of ticks is by recvTimes
                    sendTimeDelta = recvTime - sendTime   # this is the # of ms in the past the tick was sent relative to the tick's recvTime
                else:
                    self.firstRecvTime = str(recvTime)
                    self.firstSendTime = str(sendTime)

                ### prepare for next loop iteration
                prevRecvTime = recvTime
                prevSendTime = sendTime
                    
                ### now need to call updatePatternStats to record pattern frequencies for this line
                self.updatePatternStats(ticker, exchange, side, condition, sendTimeDelta, recvTimeDelta, price, size)


    def updatePatternStats(self, ticker, exchange, side, condition, sendTimeDelta, recvTimeDelta, price, size):
        ### update pattern stats with this tick's values starting with ts delta values
        if recvTimeDelta != None:  # There are no deltas for the first line in the file.
            self.upsertPatternStat(recvTimeDelta, 1)
            self.upsertPatternStat(sendTimeDelta, 1)
        else:
            self.upsertPatternStat(self.firstRecvTime, 1)
            self.upsertPatternStat(self.firstSendTime, 1)

        ### now account for 7 comma chars per tick
        self.upsertPatternStat(',', 7)

        ### now the ticker field
        self.upsertPatternStat(ticker, 1)

        ### now the size field
        self.upsertPatternStat(size, 1)

        ### now the price field components separately (whole units, then optionally a '.' char and decimal digits)
        priceComponents = price.split('.')
        wholePrice = priceComponents[0]
        self.upsertPatternStat(wholePrice, 1)
        
        decimalPrice = None
        if len(priceComponents) > 1:
            decimalPrice = priceComponents[1]
            self.upsertPatternStat('.', 1)
            self.upsertPatternStat(decimalPrice, 1)

        ### now exchange, side, and condition chars
        self.upsertPatternStat(exchange, 1)
        self.upsertPatternStat(side, 1)
        self.upsertPatternStat(condition, 1)

        ### account for 1 \r\n newline per tick based on input file format
        self.upsertPatternStat('\r', 1)
        self.upsertPatternStat('\n', 1)

        
    def upsertPatternStat(self, pattern, freq):
        if pattern in self.patternStats:   # increment the current recorded frequency by +freq
            self.patternStats[pattern] += freq
        else:   # enter this pattern in the patternStats dict
            self.patternStats[str(pattern)] = freq

            
    def buildCTCDictionary(self):
        ### sort patternStats by bytes consumed by the pattern in the input, i.e. (len(pattern) * freq(pattern)) to prioritize replacements
        priorityDict = {}
        for pattern in self.patternStats.keys():
            bytesConsumed = len(str(pattern)) * self.patternStats[pattern]
            priorityDict[pattern] = bytesConsumed

        sortedPatterns = sorted(priorityDict.items(), key=operator.itemgetter(1), reverse=True)  #list of tuples reverse sorted on compPriority
        self.generateCTCDictCodewords(sortedPatterns)
        

    def generateCTCDictCodewords(self, sortedPatterns):
        ### generate codewords that are not allowed to contain the terminatorBitPattern
        codeword = 1
        for ppTuple in sortedPatterns:
            pattern = ppTuple[0]
            bitStr = bin(codeword)[2:]
            ### Since I'll have a lot of codeword terminators per line of input and I think input data should be highly repetitive (i.e. not *too* many codewords needed)
            ### I'm going to favor the terminator being small, '00'
            ### If codes are written next to each other where one ends with 0 and the next starts with 0 we would have effectively written an unintentional terminator...
            while '00' in bitStr or bitStr[0] == '0' or bitStr[-1] == '0':  
                codeword += 1
                bitStr = bin(codeword)[2:]

            self.ctcDict[pattern] = codeword
            codeword += 1
                    

    def writeOutFile(self, inputFile, outputFile):
        ### second loop through input file to actually compress lines...  Need to use the CTCDict to replace patterns found for each field
        ### this has gotten to big, should refactor into a couple of called methods
        with open(outputFile, 'wb') as outFile:
            with fileinput.input(files=(inputFile)) as inFile:
                prevRecvTime = None
                prevSendTime = None
                recvTimeDelta = None
                sendTimeDelta = None
                leftOverBitString = ''

                limit = 0
                for line in inFile:
                    line = line.strip()   #valid to remove leading and trailing whitespace from this data (especially newlines)
                    vals = line.split(',')

                    ticker = vals[0]
                    exchange = vals[1]
                    side = vals[2]
                    condition = vals[3]
                    sendTime = int(vals[4])
                    recvTime = int(vals[5])
                    price = vals[6]
                    size = vals[7]

                    if not inFile.isfirstline():
                        recvTimeDelta = str(recvTime - prevRecvTime)   # this is relative to previous tick since we assume strongest ordering of ticks is by recvTimes
                        sendTimeDelta = str(recvTime - sendTime)   # this is the # of ms in the past the tick was sent relative to the tick's recvTime

                    ### need to lookup codeword bits for all the fields, concatenate them together (with terminators separating)
                    ### split that into however many whole bytes they make and write those to the file,
                    ### carry any leftover bits forward to be placed at the beginning of the string built for the following input line
                    ### Seems kind of silly to convert to binary stings and concatenate, but without using a 3rd party bitfile module this seems easiest/quickest
                    cwComma = bin(self.ctcDict[','])[2:]
                    cwDecimal = bin(self.ctcDict['.'])[2:]
                    cwTicker = bin(self.ctcDict[ticker])[2:]
                    cwExchange = bin(self.ctcDict[exchange])[2:]
                    cwSide = bin(self.ctcDict[side])[2:]
                    cwCondition = bin(self.ctcDict[condition])[2:]
                    if not inFile.isfirstline():
                        cwSendTime = bin(self.ctcDict[sendTimeDelta])[2:]
                        cwRecvTime = bin(self.ctcDict[recvTimeDelta])[2:]
                    else:
                        cwSendTime = bin(self.ctcDict[self.firstSendTime])[2:]
                        cwRecvTime = bin(self.ctcDict[self.firstRecvTime])[2:]

                    priceComponents = price.split('.')
                    cwWholePrice = bin(self.ctcDict[priceComponents[0]])[2:]
                    cwDecimalPrice = ''
                    if len(priceComponents) > 1:
                        cwDecimalPrice = bin(self.ctcDict[priceComponents[1]])[2:]

                    cwSize = bin(self.ctcDict[size])[2:]
                    cwNewLine = bin(self.ctcDict['\r'])[2:] + '00' + bin(self.ctcDict['\n'])[2:]

                    bitString = ''
                    bitString = bitString + leftOverBitString   # carry over previously left over bits
                    for cw in [cwTicker, cwExchange, cwSide, cwCondition, cwSendTime, cwRecvTime]:
                        bitString = bitString + cw + '00'+ cwComma + '00'

                    if cwDecimalPrice == '':
                        bitString = bitString + cwWholePrice + '00'+ cwComma + '00'
                    else:
                        bitString = bitString + cwWholePrice + '00'+ cwDecimal + '00' + cwDecimalPrice + '00'+ cwComma + '00'
                    bitString = bitString + cwSize + '00' + cwNewLine + '00'  # newline instead of comma for last field

                    byteCount = len(bitString) // 8
                    leftOverBitString = bitString[(byteCount * 8):]
                    bitsToWrite = bitString[:(byteCount * 8)]
                    bytesToWrite = self.bitStringToBytes(bitsToWrite, byteCount)
                    outFile.write(bytesToWrite)

                    ### prepare for next loop iteration
                    prevRecvTime = recvTime
                    prevSendTime = sendTime

                #final byte will start with whatever is in leftOverBitString at the end, right pad with zeros to make a full byte, then append another byte of all zeros for good measure.
                ### when decompressing hitting multiple terminator ('00') codes back to back signals that no more codewords follow to decompress.
                rightPadZerosNeeded = 8 - len(leftOverBitString)
                finalBitString = leftOverBitString
                for i in range(0, rightPadZerosNeeded):
                    finalBitString += '0'
                finalBitString += '00000000'
                finalBytesToWrite = self.bitStringToBytes(finalBitString, 2)
                outFile.write(finalBytesToWrite)
                outFile.close()

                ### save the ctcompression dictionary used for compression to a hidden.ctcd file
                ### would be nicer to write it as a header on a single output file, but this will work for now...
                pickle.dump(self.ctcDict, open('.' + outputFile + '.ctcd', 'wb'))

    def bitStringToBytes(self, bitString, byteCount):
        bsBytes = int(bitString, 2).to_bytes(byteCount, byteorder='big')   # bitstrings we get here are big endian even if the system stores them differently...
        return bsBytes
