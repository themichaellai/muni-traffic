from collections import defaultdict, deque
from datetime import datetime
import geopy
import geopy.distance
import itertools
import numpy as np
import pytz
from scipy import stats
import sqlite3
import sys
import time

TZ = 'America/Los_Angeles'

COL_NAMES = [
    'id',
    'time',
    'vehicleId',
    'lon',
    'routeTag',
    'predictable',
    'speedKmHr',
    'heading',
    'lat',
    'secsSinceReport',
]


def datetime_to_epoch(dt):
    localized = pytz.timezone(TZ).localize(dt)
    as_utc = localized.astimezone(pytz.utc)
    return int(time.mktime(as_utc.timetuple()))


def epoch_to_datetime(epoch):
    return datetime.fromtimestamp(epoch)


def row_to_dict(col_names, data):
    return {k: v for k, v in zip(col_names, data)}


def get_locations(conn, start_date, end_date):
    c = conn.cursor()
    print datetime_to_epoch(start_date)
    print datetime_to_epoch(end_date)
    c.execute(
        'SELECT {} FROM vehicle_locations WHERE time > ? AND time < ? AND routeTag = ? ORDER BY time'.format(
            ','.join(COL_NAMES)
        ),
        [
            datetime_to_epoch(start_date),
            datetime_to_epoch(end_date),
            'M',
        ]
    )
    results = [row_to_dict(COL_NAMES, row) for row in c]
    return results


def group_by(data, f):
    res = defaultdict(list)
    for row in data:
        res[f(row)].append(row)
    return res


def distance_to_coord(lat1, lng1, lat2, lng2):
    return geopy.distance.vincenty((lat1, lng1), (lat2, lng2)).kilometers


def within_box(bounding_box, lat, lng):
    return ((bounding_box['south_lat'] < lat < bounding_box['north_lat']) and
            bounding_box['west_lng'] < lng < bounding_box['east_lng'])


def get_bounding_box(miles, lat, lng):
    start = geopy.Point(lat, lng)
    d = geopy.distance.VincentyDistance(miles=miles)
    return {
        'north_lat': d.destination(point=start, bearing=0).latitude,
        'south_lat': d.destination(point=start, bearing=180).latitude,
        'east_lng':  d.destination(point=start, bearing=90).longitude,
        'west_lng': d.destination(point=start, bearing=270).longitude,
    }


# fill in speeds based on last known position time elapsed
def fill_mean_speed(locations):
    DELTAS = [
        3 * 60,
        5 * 60,
        10 * 60,
    ]
    # dict of vehicleId to array of last positions
    memo = defaultdict(lambda: deque(maxlen=10))
    for location in itertools.islice(locations, 1, len(locations)):
        vehicleId = location['vehicleId']
        time = location['time']
        last_positions = memo[vehicleId]
        for time_delta in DELTAS:
            last_positions_within = [p for p in last_positions
                                     if time - p['time'] < time_delta]
            speed_key = 'speedKmHr_{}'.format(time_delta / 60)
            if len(last_positions_within) > 0:
                earliest = last_positions_within[0]
                distance = distance_to_coord(earliest['lat'],
                                             earliest['lon'],
                                             location['lat'],
                                             location['lon'])
                time_delta_hours = (time - earliest['time']) / float(60 * 60)
                location[speed_key] = distance / time_delta_hours
            else:
                location[speed_key] = None
        memo[vehicleId].append(location)

if __name__ == '__main__':
    conn = sqlite3.connect(sys.argv[1])
    church_coords = [37.767343, -122.429065]

    start_date = datetime(2017, 3, 29, 0, 0)
    end_date = datetime(2017, 3, 30, 0, 0)
    results = get_locations(conn, start_date, end_date)
    conn.close()

    church_box = get_bounding_box(1.5, *church_coords)
    close_to_church = [r for r in results
                       if within_box(church_box, r['lat'], r['lon'])]

    secsSinceReports = [el['secsSinceReport'] for el in close_to_church]
    print 'secsSinceReports'
    print stats.describe(secsSinceReports)
    print 'median', np.median(secsSinceReports)
    print stats.normaltest(secsSinceReports)

    print 'filling in'
    fill_mean_speed(close_to_church)


    grouped_by_hour = group_by(close_to_church,
                               lambda r: epoch_to_datetime(r['time']).hour)

    DELTAS = [
        3 * 60,
        5 * 60,
        10 * 60,
    ]
    for delta in DELTAS:
        speed_key = 'speedKmHr_{}'.format(delta / 60)
        num_filled = sum(1 for el in close_to_church if el.get(speed_key, None) is not None)
        print '{}: {}/{} ({})'.format(speed_key,
                                      num_filled,
                                      len(close_to_church),
                                      num_filled / float(len(close_to_church)))
    #for k, v in grouped_by_hour.iteritems():
    #    speeds = [el['speedKmHr'] for el in v]
    #    description = stats.describe(speeds)
    #    print '{} ({})'.format(k, len(v))
    #    pretty_pairs = ['{}: {}'.format(col, getattr(description, col))
    #                    for col in ['mean', 'variance', 'minmax']]
    #    print ' '.join(pretty_pairs)
    #    #if 6 <= k <= 10:
    #    #    import pdb; pdb.set_trace()
