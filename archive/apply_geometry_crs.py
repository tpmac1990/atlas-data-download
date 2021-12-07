import os
import pandas as pd

# from .clean_geometry import clean_multipolygon_by_df

# def add_crs_to_wkt(self):
#     ''' This inserts the crs code to the start of the wkt for each geometry feature in the table '''
#     print('Adding crs to wkt fields.')
#     new_dir = os.path.join(self.output_dir,'new')
#     for file in ['Tenement','Occurrence']:
#         path = os.path.join(new_dir,"%s.csv"%(file))
#         df = pd.read_csv(path)
#         # reduce the redundant points along the polygons straight sides
#         if file == 'Tenement':
#             df = clean_multipolygon_by_df(df,'geom')
#         # add the SRID to the geometry
#         df['geom'] = ["SRID=4202;%s"%(feature) for feature in df['geom']]
#         df.to_csv(path,index=False)