from mrjob.job import MRJob
from mrjob.step import MRStep
import re
class MostPopular(MRJob):
    re.compile(r"[\w']+")
    def steps(self):
        return [
            MRStep(mapper=self.mapper_parent_id,
                   reducer=self.reducer_count_parent_id),
            MRStep(mapper=self.mapper_passthrough,
                   reducer = self.reducer_find_popular)
        ]

    def mapper_parent_id(self,key,line):
        if len(list(line.split(",")))==22:
            (created_utc, ups, subreddit_id, link_id, name, score_hidden, author_flair_css_class, author_flair_text, subreddit, id, removal_reason, gilded, downs, archived, author, score, retrieved_on, body, distinguished, edited, controversiality, parent_id) = line.split(',') 
        #(sn, subreddit, count) =line.split( )
        #if sn is float:
            yield (parent_id,subreddit),(1)

    def mapper_passthrough(self, key, value):
        yield key, value

    def reducer_count_parent_id(self, key, values):
        yield None, (sum(values), key)

    def reducer_find_popular(self, key, values): 
        p,s,v,q=values
        yield (key),(p,s,v,q)

if __name__ == '__main__':
    MostPopular.run()