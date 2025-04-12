#!/usr/bin/env python3
import sys
import re
import requests

# tried to use .txt, unsuccesfully
stopwords_list = requests.get("https://gist.githubusercontent.com/rg089/35e00abf8941d72d419224cfd5b5925d/raw/12d899b70156fd0041fa9778d657330b024b959c/stopwords.txt").content
stopwords = set(stopwords_list.decode().splitlines())

# input comes from STDIN (standard input)
# meaning no custom prints are allowed
# each line is separate document
# test:
# lines=' '.join(sys.stdin)
# print(len(lines.split('\n')))
# for line in lines.split('\n')[:-2]:
# inference:
for line in sys.stdin:
    try:
        words=line.split()
        doc_id=words[0]
        words=words[1:]
        doc_len=len(words)
        
        # remove leading and trailing whitespace
        line = line.strip()
        # keep only alpha and spaces
        line=re.sub('[^a-zA-Z\s]+', '', line)
        # remove stopwords, split into list of words
        words = [word for word in words if word not in stopwords]
        
        for word in words:
            # write the results to STDOUT (standard output);
            # what we output here will be the input for the
            # Reduce step, i.e. the input for reducer.py
            
            # every word within one document is mapped to unique number
            print (f'{word}\t{doc_id}\t{doc_len}')
    except Exception as e:
        sys.stderr.write(f"ERROR processing line '{line}': {str(e)}\n")
        continue