#!/usr/bin/env python3
import sys

# In cassandra we will have tables:
# document_frequency: 
last_doc=None
search_word_count_across_docs =0
# word_within_one_document_count:
search_word_count_within_doc = 0

search_doc=None
search_word = None
# input comes from STDIN
# each line = word, sentence_num
# input is sorted by word:
# 3214 word1 100 
# 3214 word1 100
# 2345 word1 200
# 2342 word2 100
# 9079 word2 300
# 3685 word3 400
for line in sys.stdin:
    try:
        line = line.strip()
        # current line word
        # word, doc_id
        word, doc_id, doc_len = line.split('\t')
        doc_id = int(doc_id)
        doc_len=int(doc_len)
        # new word
        if search_word!=word:
            # save previous word data into document_frequency
            if search_word!=None: #skip first
                # 1 example is too few, skip and save compute. Also those words are probably non-ascii
                if search_word_count_across_docs!=1:
                    print (f'word_within_one_document_count {search_word} {search_doc} {search_word_count_within_doc}')
                    print (f'document_frequency {search_word} {search_word_count_across_docs}' )
                    print (f'doc_id_len {doc_id} {doc_len}')
                
            search_word=word
            search_doc=doc_id
            search_word_count_within_doc=1
            # found first doc with this word
            last_doc=doc_id
            search_word_count_across_docs=1
            
        # same word
        else:
            if search_doc==doc_id:
                search_word_count_within_doc+=1
            else:
                # New doc
                print (f'word_within_one_document_count {search_word} {search_doc} {search_word_count_within_doc}')
                print (f'doc_id_len {doc_id} {doc_len}')
                search_word_count_within_doc=1
                search_doc=doc_id

            if last_doc!=doc_id:
                # found one more document with this word
                search_word_count_across_docs+=1
                last_doc=doc_id
    except Exception as e:
        sys.stderr.write(f"ERROR processing line '{line}': {str(e)}\n")
        continue

# do not forget to output the last word
print (f'document_frequency {search_word} {search_word_count_across_docs}') 
print (f'word_within_one_document_count {search_word} {search_doc} {search_word_count_within_doc}')
print (f'doc_id_len {doc_id} {doc_len}')