#!/usr/bin/env python3
import pickle

class ctDecompressor:
    def __init__(self):
        self.ctdDict = {}
        self.chunkSize = 1024

        
    def decompress(self, inputFile, outputFile):
        self.loadCTDDict(inputFile)

        with open(outputFile, 'w') as outFile:   # truncate an existing outputFile if it exists
            outFile.truncate()
        
        with open(inputFile, 'rb') as inFile:   # read in chunks of binary data to process
            chunk = inFile.read(self.chunkSize)
            ### need to reconstruct by lines so we can track values in previous line to "un-delta" the timestamps...
            workingData = ['', '', '']   # list of the workingBits, previousLine, and currentLine
            while chunk:
                workingData = self.processChunk(outputFile, chunk, workingData)
                chunk = inFile.read(self.chunkSize)

                
    def processChunk(self, outputFile, chunk, workingData):
        bitString = self.buildBitString(chunk)   # get string representation of chunk in binary
        
        with open(outputFile, 'a') as outFile:
            ### restore working state as of end of the previous chunk
            cw = workingData[0]
            prevBit = ''
            if len(cw) > 0:
                prevBit = cw[-1]

            previousLine = workingData[1]
            currentLine = workingData[2]
            ### now iterate through the bitString identifying codewords... this is horribly slow, I'd think through something less silly if I had time
            for i in range(0, len(bitString)):
                bit = bitString[i]
                if (prevBit + bit) == '00':   #we've found a cw terminator, strip the last zero off of cw and look up it's pattern string
                    cw = cw[:-1]
                    cwStr = self.ctdDict[int(cw, 2)]
                    ### if it is a newline, we've assembled a complete line, need to process it 
                    if cwStr == '\n':
                        currentLine = self.unDeltaTimestamps(previousLine, currentLine)   # replace ts deltas with actual timestamps
                        ### set previousLine to current, write to outFile, and reset current
                        previousLine = currentLine
                        outFile.write(currentLine + cwStr)
                        currentLine = ''
                    else:
                        currentLine += cwStr   # keep appending till we get a complete line
                    cw = ''   # reset to find the next codeword
                else:   # haven't yet assembled the bits of a complete codeword
                    cw += bit
                    prevBit = bit
            return([cw, previousLine, currentLine])   # need to know where we left off here for processing the next chunk

        
    def unDeltaTimestamps(self, previousLine, currentLine):
        if previousLine != '':   # not on the first line of the file
            prevRecvTime = int(previousLine.split(',')[5])   # grab recvTime from previous line
            
            ### calculate the actual recvTime and sendTime for the current line based on the deltas stored
            currentLineVals = currentLine.split(',')
            currentRecvTimeDelta = int(currentLineVals[5])
            currentSendTimeDelta = int(currentLineVals[4])
            currentActualRecvTime = prevRecvTime + currentRecvTimeDelta
            currentActualSendTime = currentActualRecvTime - currentSendTimeDelta
            
            ### replace the deltas with the actual recvTime and sendTime in the newCurrentLine
            currentLineVals[5] = str(currentActualRecvTime)
            currentLineVals[4] = str(currentActualSendTime)
            newCurrentLine = ''
            for val in currentLineVals:
                newCurrentLine += val + ','
            newCurrentLine = newCurrentLine.rstrip(',')   # remove trailing comma from the line
            return(newCurrentLine)
        else:
            return(currentLine)   # previousLine will be empty for first line in file

        
    def buildBitString(self, chunk):
        ### need to iterate through bytes in the chunk, building a bitString
        bitString = ''
        for byte in chunk:
            bitString += bin(byte)[2:].zfill(8)
        
        if bitString[-4:] == '0000':   # This is the last chunk
            bitString = bitString.rstrip('0')   # strip the padding
            bitString = bitString + '00'   # add back a terminator so the last codeword gets processed

        return bitString
    
        
    def loadCTDDict(self, inputFile):
        ctcDict = pickle.load(open('.' + inputFile + '.ctcd', 'rb'))
        self.ctdDict = {y:x for x,y in ctcDict.items()}   #for decompression need format: { codeword: pattern, ... }
