import pandas as pd

def nan_serializer(obj):
        if pd.isna(obj):
                return None
        return obj

