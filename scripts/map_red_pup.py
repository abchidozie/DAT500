"""
Script to find the most popular subreddit

"""


from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MostPopular(MRJob):
    re.compile(r"[\w']+")
    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
                   reducer=self.reducer_count),
            MRStep(mapper=self.mapper_passthrough,
                   reducer = self.reducer_max)
        ]

    def mapper1(self,key,line):
        if len(list(line.split(",")))==22:
            (created_utc, ups, subreddit_id, link_id, name, score_hidden, author_flair_css_class, author_flair_text, subreddit, id, removal_reason, gilded, downs, archived, author, score, retrieved_on, body, distinguished, edited, controversiality, parent_id) = line.split(',') 
        #(sn, subreddit, count) =line.split( )
        #if sn is float:
            yield subreddit,1

    def mapper_passthrough(self, key, value):
        yield key, value

    def reducer_count(self, key, values):
        yield None, (sum(values), key)

    def reducer_max(self, key, values):
        yield key, max(values)

if __name__ == '__main__':
    MostPopular.run()