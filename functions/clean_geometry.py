import math
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from shapely import wkt



def calculate_initial_compass_bearing(pointA, pointB):
    """
    Calculates the bearing between two points.
    The formulae used is the following:
        θ = atan2(sin(Δlong).cos(lat2),
                  cos(lat1).sin(lat2) − sin(lat1).cos(lat2).cos(Δlong))
    :Parameters:
      - `pointA: The tuple representing the latitude/longitude for the
        first point. Latitude and longitude must be in decimal degrees
      - `pointB: The tuple representing the latitude/longitude for the
        second point. Latitude and longitude must be in decimal degrees
    :Returns:
      The bearing in degrees
    :Returns Type:
      float
    """
    if (type(pointA) != tuple) or (type(pointB) != tuple):
        raise TypeError("Only tuples are supported as arguments")

    lat1 = math.radians(pointA[0])
    lat2 = math.radians(pointB[0])

    diffLong = math.radians(pointB[1] - pointA[1])

    x = math.sin(diffLong) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1)
            * math.cos(lat2) * math.cos(diffLong))

    initial_bearing = math.atan2(x, y)

    # Now we have the initial bearing but math.atan2 return values
    # from -180° to + 180° which is not what we want for a compass bearing
    # The solution is to normalize the initial bearing as shown below
    initial_bearing = math.degrees(initial_bearing)
    compass_bearing = (initial_bearing + 360) % 360

    return compass_bearing


def clean_by_bearing(coords):
    ''' for a given set of polygon coordinates, this will remove almost all of the redundant points that lie on a straight line.
        It compares the compass bearing angle of adjacent points and if the difference in degrees is less than 0.05 degrees 
        then it removes the middle point from the final collection.
        Returns a list of latlng tuples
    '''
    pnt_lst = coords[:2]
    for latlng in coords[2:]:
        pnt_a = pnt_lst[-2]
        pnt_b = pnt_lst[-1]
        pnt_c = latlng
        bear_a = calculate_initial_compass_bearing(pnt_a, pnt_b)
        bear_b = calculate_initial_compass_bearing(pnt_b, pnt_c)

        if (abs(bear_a - bear_b)) < 0.2:
            pnt_lst.pop()
        pnt_lst.append(latlng)
    return pnt_lst


def clean_multipolygon_by_bearing(geom):
    ''' loops through each polygon in the multipolygon and then cleans the exterior and all interior polygons and rebuilds the complete polygon.
        geom is passed in as wkt and then returned as wkt. This means there is no need to convert the df being worked on into a geopandas df.
        The percentage of the reduction in the file size is about the percentage of time that will be saved when retrieving the data
    '''
    geom = wkt.loads(geom)
    multi_lst = []    
    for poly in geom:
        ext_pnt_lst = clean_by_bearing(poly.exterior.coords)
        poly_lst = [] # this can remain empty
        for inner_poly in poly.interiors:
            int_pnt_lst = clean_by_bearing(inner_poly.coords)
            poly_lst.append(int_pnt_lst)
        multi_lst.append(Polygon(ext_pnt_lst,poly_lst))

    return wkt.dumps(MultiPolygon(multi_lst))


def clean_multipolygon_by_df(df,geom_field):
    ''' clean the multipolygon field in a df '''
    df[geom_field] = df[geom_field].apply(lambda x: clean_multipolygon_by_bearing(x))
    return df
