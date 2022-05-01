from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import pandas as pd 


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract


class MostPopular(MRJob):
#     re.compile(r"[\w']+")
#     def isEnglish(text):
#         try:
#             if detect(text) != 'en':
#                 return "No"
#             return "Yes"
#         except:
#             return "No"
#         return "No"
# # Convert function to U
     
    def mapper(self,_,line ):
        #line=line.strip()
        #line=list(line.split(','))
        if len(list(line.split(",")))==22:
            (created_utc, ups, subreddit_id, link_id, name, score_hidden, author_flair_css_class, author_flair_text, subreddit, id, removal_reason, gilded, downs, archived, author, score, retrieved_on, body, distinguished, edited, controversiality, parent_id) = line.split(",")
            #if isEnglish(body)=="Yes":
            #if created_utc.isnumeric()==True:
            yield subreddit,1
                
    def reducer(self,key,value):
        k=sum(value)
        if k>40000:
            yield key,k

if __name__ == '__main__':
    MostPopular.run()

