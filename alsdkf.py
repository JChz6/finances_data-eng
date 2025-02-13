#import pandas as pd
from datetime import datetime
import pytz

utc_minus_5 = pytz.timezone('America/Bogota')
print(datetime.now(pytz.utc).astimezone(utc_minus_5))