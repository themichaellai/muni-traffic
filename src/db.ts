import * as db from 'sqlite';
import * as P from 'bluebird';
import * as R from 'ramda';

import { VehicleLocation } from './muni';

const insertVehicleLocation = async (
  db: db.Database,
  loc: VehicleLocation,
): Promise<void> => {
  const cols = [
    'vehicleId',
    'lon',
    'routeTag',
    'predictable',
    'speedKmHr',
    'heading',
    'lat',
    'secsSinceReport',
  ];
  const placeholders = R.repeat('?', cols.length).join(',');
  await db.run(
    `INSERT INTO vehicle_locations (${cols.join(',')}) VALUES (${placeholders})`,
    [
      loc.id,
      loc.lon.toString(),
      loc.routeTag,
      loc.predictable,
      loc.speedKmHr,
      loc.heading,
      loc.lat.toString(),
      loc.secsSinceReport,
    ],
  );
};

export const insertVehicleLocations = async (
  db: db.Database,
  locs: Array<VehicleLocation>,
): Promise<void> => {
  await db.run('begin transaction');
  for (let loc of locs) {
    await insertVehicleLocation(db, loc);
  }
  await db.run('commit');
};

export const openDB =
  (filename: string): Promise<db.Database> => db.open(filename);