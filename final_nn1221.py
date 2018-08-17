import sys
from pyspark import SparkContext, SparkFiles

def createIndex(cities):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(cities).to_crs(fiona.crs.from_epsg(5070))
    index = rtree.Rtree()
    for idx, geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return zones.plctract10[idx], zones.plctrpop10[idx]
    return None

def processTweets(_, lines):
    import re
    import pyproj
    import rtree
    import shapely.geometry as geom
    separator = re.compile('\W+')
    proj = pyproj.Proj(init="epsg:5070", preserve_units=True)    
    index, zones = createIndex(SparkFiles.get('500cities_tracts.geojson'))
    counts = {}
    drugs1 = SparkFiles.get('drug_illegal.txt')
    drugs2 = SparkFiles.get('drug_sched2.txt')
    terms = set(map(lambda x: x.strip(), 
                   (open(drugs1, 'r').readlines()+
                   open(drugs2, 'r').readlines())))
    for line in lines:
        fields = line.split('|')
        lat,lon,body = fields[1], fields[2], fields[-1]
        words = separator.split(body.lower())
        for words in terms:
            if len(terms.intersection(words)) >= len(words):
                p = geom.Point(proj(lon,lat))                 
                match = None
                try:
                    zone = findZone(p, index, zones)
                except:
                    continue
                if zone: 
                    if zone[1] > 0:
                        counts[zone[0]] = counts.get(zone[0], 0) + (1.0 / zone[1])  

    return counts.items()

if __name__=='__main__':
    sc = SparkContext()
    tweetsource = sys.argv[-2]                               
    tweets = sc.textFile(tweetsource, use_unicode=False).cache()\
                        .mapPartitionsWithIndex(processTweets)\
                        .reduceByKey(lambda x,y: x+y)\
                        .sortBy(lambda x: x[0]) \
                        .saveAsTextFile(sys.argv[-1])
                