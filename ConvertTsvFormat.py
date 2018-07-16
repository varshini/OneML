"""
This code is to convert generic ranklist file to Microsoft-specific one
"""
#!/usr/bin/python3

import sys
import pandas as pd
from pathlib import Path

def main():
    if len(sys.argv) < 3:
        sys.exit("Usage: python ConvertTsvFormat.py <input-file> <output-file>")
    
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]
    
    inFile = Path(inputFile)
    
    if not inFile.is_file:
        sys.exit("input-file does not exist or not accessible")
    
    df1 = pd.read_csv(inputFile, sep='\t', names=['qid', 'q0', 'docid', 'rank', 'error', 'method'])
    df1 = df1.filter(items=['qid', 'docid', 'rank'])
    df2 = df1.loc[df1['rank'] < 10].copy()
    df2['rank'] = df2['rank'] + 1 # This is needed only if ranking output is 0-based indexing
    df2.to_csv(outputFile, sep='\t', index=False, header=False)

if __name__ == "__main__":
    main()
    
