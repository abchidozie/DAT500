# -*- coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract




class MostPopularsort(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer1),
            MRStep(mapper=self.mapper2,
                   reducer = self.reducer)
        ]
     
    def mapper(self,_,line ):
        #line=line.strip()
        #line=list(line.split(','))
        if len(list(line.split(",")))==22:
            (created_utc, ups, subreddit_id, link_id, name, score_hidden, author_flair_css_class, author_flair_text, subreddit, id, removal_reason, gilded, downs, archived, author, score, retrieved_on, body, distinguished, edited, controversiality, parent_id) = line.split(",")
            #if isEnglish(body)=="Yes":
            #if created_utc.isnumeric()==True:
            yield subreddit,1
                
    def reducer1(self,key,value):
        k=sum(value)
        if k>40000:
            yield key,k
    
    def mapper2(self,key,k):
        yield '%09d'%int(k),key
        
    def reducer(self,k,keys):
        for key in keys:
            yield k,key 

if __name__ == '__main__':
    MostPopularsort.run()

