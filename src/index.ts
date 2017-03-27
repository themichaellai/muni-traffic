import * as P from 'bluebird';
import * as moment from 'moment';
import * as R from 'ramda';
import * as sqlite from 'sqlite';

import * as db from './db';
import * as muni from './muni';

const ROUTE_TAG_WHITELIST = [
  'J',
  'N',
  'M',
  //'K',
];

const BASE_WAIT_SECS = 60;
const SPREAD_SECS = 10;

const getWaitTime = (
  baseWait: number,
  spread: number,
): number => (baseWait - (spread / 2)) + (Math.random() * spread);

const debug = (
  name: string,
  f: Function,
) => (...args: Array<any>) => {
  console.log(`${name}(${args.join(', ')})`);
  return f(...args);
};

const sleep = (seconds: number): Promise<{}> =>
  new Promise((resolve) => setTimeout(resolve, seconds * 1000));

const runRoutePoller = R.curry(async (
  conn: sqlite.Database,
  pollBaseWait: number,
  pollWaitSpread: number,
  routeTag: string,
): Promise<void> => {
  console.log(`${routeTag}: initializing poller`);
  let $lastTime: number = moment().subtract(5, 'minutes').valueOf();
  await sleep(getWaitTime(7, pollWaitSpread));
  while (true) {
    console.log(`${routeTag}: getting data`);
    try {
      const { locations, lastTime } = await muni.getVehicleLocations(routeTag,
                                                                     $lastTime);
      $lastTime = lastTime;
      const ids = locations.map((l) => l.id);
      console.log(
        `${routeTag}: got ${locations.length} vehicles: ${ids.join(', ')}`);
      if (locations.length > 0) {
        try {
          await db.insertVehicleLocations(conn, locations);
        } catch (e) {
          console.warn(
            `${routeTag}: could not save ${locations.length} locations`);
        }
      }
    } catch (e) {
      console.warn(`${routeTag}: ${e.message}`);
    }
    const sleepTime = getWaitTime(pollBaseWait, pollWaitSpread);
    console.log(`${routeTag}: sleeping ${sleepTime}`);
    await sleep(sleepTime);
  }
});

(async () => {
  const conn = await db.openDB('data.db');
  await P.map(ROUTE_TAG_WHITELIST, runRoutePoller(conn,
                                                  BASE_WAIT_SECS,
                                                  SPREAD_SECS));
})();
