import * as request from 'request-promise';

const BASE_URL = 'http://webservices.nextbus.com/service/publicJSONFeed';

interface Route {
  tag: string;
  title: string;
};

interface RouteListResponse {
  route: Array<Route>;
};

export interface VehicleLocation {
  id: string;
  lon: number;
  routeTag: string;
  predictable: boolean;
  speedKmHr: number;
  heading: number;
  lat: number;
  secsSinceReport: number;
};

interface RawVehicleLocation {
  id: string;
  lon: string;
  routeTag: string;
  predictable: string;
  speedKmHr: string;
  heading: string;
  lat: string;
  secsSinceReport: string;
};

interface VehicleLocationsResponse {
  vehicle?: Array<RawVehicleLocation>;
  lastTime: {
    // epoch in ms
    time: string;
  };
  Error?: {
    content: string;
    // "true" or "false"
    shouldRetry: string;
  };
};

const parseRawVehicleLocation = (
  raw: RawVehicleLocation,
): VehicleLocation => ({
  id: raw.id,
  lon: parseFloat(raw.lon),
  routeTag: raw.routeTag,
  predictable: raw.lon === 'true',
  speedKmHr: parseInt(raw.speedKmHr, 10),
  heading: parseInt(raw.heading, 10),
  lat: parseFloat(raw.lat),
  secsSinceReport: parseInt(raw.secsSinceReport, 10),
});

export const getRouteList = async (): Promise<Array<Route>> => {
  const res = await request({
    method: 'GET',
    uri: BASE_URL,
    json: true,
    qs: {
      command: 'routeList',
      a: 'sf-muni',
    },
    resolveWithFullResponse: true,
  });

  if (res.statusCode === 200) {
    return (res.body as RouteListResponse).route;
  } else {
    throw new Error('could not get route list');
  }
};

export const getVehicleLocations = async (
  routeTag: string,
  lastTime: number,
): Promise<{
  locations: Array<VehicleLocation>;
  lastTime: number;
}> => {
  const res = await request({
    method: 'GET',
    uri: BASE_URL,
    json: true,
    qs: {
      a: 'sf-muni',
      command: 'vehicleLocations',
      r: routeTag,
      t: lastTime,
    },
    resolveWithFullResponse: true,
  });

  if (res.statusCode === 200) {
    const body = res.body as VehicleLocationsResponse;
    if (body.Error != null) {
      throw new Error(`could not get vehicle locations: ${body.Error.content}`);
    } else if (body.vehicle == null) {
      return {
        lastTime: parseInt(body.lastTime.time, 10),
        locations: [],
      };
    } else {
      return {
        lastTime: parseInt(body.lastTime.time, 10),
        locations: body.vehicle.map(parseRawVehicleLocation),
      };
    }
  } else {
    throw new Error('could not get vehicle locations');
  }
};
