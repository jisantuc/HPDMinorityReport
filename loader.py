import pandas as pd 
 
class DataLoader(object): 
    def __init__(self, complaint_fname, weather_fname): 
        self.fname = fname 
        self.cols = map( 
            lambda x: x.proper(), 
            ['unique key', 'created date', 'closed date', 'agency', 'agency name', 
             'complaint type', 'descriptor', 'location type', 'incident zip', 
             'incident address', 'street name', 'address type', 'city', 
             'facility type', 'status', 'resolution description', 'borough'] 
        ) 
 
    def load_complaint_data(self): 
        chunks = pd.read_csv( 
            self.complaint_fname, 
            header=None, 
            names=self.cols, 
            usecols=self.cols, 
            iterator=True, 
            chunksize=10000 
        ) 
        return pd.concat([chunk for chunk in chunks], ignore_index=True).rename( 
            columns={'created date': 'date'} 
        ) 
 
    def load_weather_data(self): 
        chunks = pd.read_csv( 
            self.wather_fname, 
            iterator=True, 
            chunksize=10000
        )

        return pd.concat([chunk for chunk in chunks], ignore_index=True)

    def load_data(self):
        weather = self.load_weather_data()
        complaints = self.load_complaint_data()

        return complaints.join(
            weather, on='date'
        )
